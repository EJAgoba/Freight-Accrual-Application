# pipeline.py
import pandas as pd
from extract_codes import Extractor
from address_merge import CombinedAddress
from address_crossref import Merger
from map_types import TypeMapper, TypeCleaner
from matrix_map import MatrixMapper

class PipelineRunner:
   def run(self, accrual_df, cintas_location_table, complete_location_table, codes_df):
       # ==============================================================
       # 1. NORMALIZE + CREATE BLANK COLUMNS
       # ==============================================================
       extractor = Extractor()
       extractor.create_columns(accrual_df)
       extractor.lower_columns(accrual_df, "Consignor", "Consignee")
       # ==============================================================
       # 2. PRIORITY 1 — ORIGIN/DEST TYPE CODE OVERRIDE
       # ==============================================================
       accrual_df = extractor.apply_type_code_priority(accrual_df)
       # ==============================================================
       # 3. PRIORITY 2 — EXTRACT FROM CONSIGNOR/CONSIGNEE STRING
       # Only fills blanks or Third Party
       # ==============================================================
       accrual_df = extractor.extract_from_consignor_consignee(accrual_df, codes_df)
       # ==============================================================
       # 4. BUILD COMBINED ADDRESSES (NORMALIZED)
       # ==============================================================
       comb = CombinedAddress()
       # Cintas reference table normalized
       cintas_location_table = comb.create(
           cintas_location_table,
           "Combined Address",
           "Loc_Address", "Loc_City", "Loc_ST"
       )
       # Consignor address
       accrual_df = comb.create(
           accrual_df,
           "Consignor Combined Address",
           "Origin Addresss", "Origin City", "Origin State Code"
       )
       # Consignee address
       accrual_df = comb.create(
           accrual_df,
           "Consignee Combined Address",
           "Dest Address1", "Dest City", "Dest State Code"
       )
       # ==============================================================
       # 5. PRIORITY 3 — USE COMBINED ADDRESS FALLBACK
       # ==============================================================
       merger = Merger()
       accrual_df = merger.merge_address_codes(accrual_df, cintas_location_table)
       # ==============================================================
       # 6. MAP CONSIGNOR/CONSIGNEE TYPE
       # ==============================================================
       mapper = TypeMapper()
       accrual_df = mapper.map_types(accrual_df, cintas_location_table, "Consignor Code", "Consignor Type")
       accrual_df = mapper.map_types(accrual_df, cintas_location_table, "Consignee Code", "Consignee Type")
       cleaner = TypeCleaner()
       accrual_df = cleaner.fill_non_cintas(accrual_df, "Consignor Type", "Consignee Type")
       # ==============================================================
       # 7. MATRIX MAPPING → ASSIGNED LOCATION CODE
       # ==============================================================
       matrix_mapper = MatrixMapper()
       accrual_df["Assigned Location Code"] = accrual_df.apply(
           matrix_mapper.determine_profit_center,
           axis=1
       )
       # ==============================================================
       # 8. MERGE PROFIT & COST CENTER
       # ==============================================================
       accrual_df = accrual_df.merge(
           complete_location_table[["Loc Code", "Prof_Cntr", "Cost_Cntr"]],
           left_on="Assigned Location Code",
           right_on="Loc Code",
           how="left"
       )
       accrual_df.rename(columns={
           "Prof_Cntr": "Profit Center EJ",
           "Cost_Cntr": "Cost Center EJ",
       }, inplace=True)
       # ==============================================================
       # 9. ACCOUNT EJ RULE
       # ==============================================================
       accrual_df["Account # EJ"] = accrual_df.apply(
           lambda row: 621000
           if row.get("Consignee Code") == row.get("Assigned Location Code")
           else 621020,
           axis=1,
       )
       # ==============================================================
       # 10. ORDER COLUMNS
       # ==============================================================
       first_cols = [
           "Profit Center", "Cost Center", "Account #",
           "Profit Center EJ", "Cost Center EJ", "Account # EJ",
           "Assigned Location Code",
           "Consignor Code", "Consignee Code",
           "Consignor Type", "Consignee Type"
       ]
       ordered = [c for c in first_cols if c in accrual_df.columns] + \
                 [c for c in accrual_df.columns if c not in first_cols]
       accrual_df = accrual_df[ordered]
       return accrual_df
