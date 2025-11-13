# pipeline.py
import pandas as pd
from extract_codes import Extractor
from address_merge import CombinedAddress
from address_crossref import Merger
from map_types import TypeMapper, TypeCleaner
from matrix_map import MatrixMapper

class PipelineRunner:
   """
   FULL accrual re-coding pipeline:
     1) Origin/Dest Type Code priority
     2) Extract codes from Consignor/Consignee text
     3) Build Combined Addresses (first word + city + state)
     4) Initial Type mapping
     5) Address-based cross-ref for rows still blank / third-party
     6) Re-map Types after codes changed
     7) Matrix mapping (Assigned Location Code)
     8) Join profit/cost centers
     9) Account # EJ rule
   """
   def run(
       self,
       accrual_df: pd.DataFrame,
       cintas_location_table: pd.DataFrame,
       complete_location_table: pd.DataFrame,
       location_codes_df: pd.DataFrame,
   ) -> pd.DataFrame:
       extractor = Extractor()
       extractor.create_columns(accrual_df)
       extractor.lower_columns(accrual_df, "Consignor", "Consignee")
       # ---------- 1. Origin/Dest Type Code priority ----------
       accrual_df = extractor.apply_type_code_priority(accrual_df)
       # ---------- 2. Extract from Consignor / Consignee text ----------
       accrual_df = extractor.extract_from_consignor_consignee(
           accrual_df, location_codes_df
       )
       # ---------- 3. Build Combined Addresses ----------
       comb = CombinedAddress()
       cintas_location_table = comb.create_combined_address_accrual(
           cintas_location_table,
           "Combined Address",
           "Loc_Address",
           "Loc_City",
           "Loc_ST",
       )
       accrual_df = comb.create_combined_address_accrual(
           accrual_df,
           "Consignor Combined Address",
           "Origin Addresss",
           "Origin City",
           "Origin State Code",
       )
       accrual_df = comb.create_combined_address_accrual(
           accrual_df,
           "Consignee Combined Address",
           "Dest Address1",
           "Dest City",
           "Dest State Code",
       )
       # ---------- 4. Initial Type mapping ----------
       type_mapper = TypeMapper()
       accrual_df = type_mapper.map_types(
           accrual_df, cintas_location_table, "Consignor Code", "Consignor Type"
       )
       accrual_df = type_mapper.map_types(
           accrual_df, cintas_location_table, "Consignee Code", "Consignee Type"
       )
       cleaner = TypeCleaner()
       accrual_df = cleaner.fill_non_cintas(
           accrual_df, "Consignor Type", "Consignee Type"
       )
       # ---------- 5. Address-based cross-ref for blanks / third-party ----------
       merger = Merger()
       accrual_df = merger.merge_by_address(accrual_df, cintas_location_table)
       # ---------- 6. Re-map Types after address fills ----------
       accrual_df = type_mapper.map_types(
           accrual_df, cintas_location_table, "Consignor Code", "Consignor Type"
       )
       accrual_df = type_mapper.map_types(
           accrual_df, cintas_location_table, "Consignee Code", "Consignee Type"
       )
       accrual_df = cleaner.fill_non_cintas(
           accrual_df, "Consignor Type", "Consignee Type"
       )
       # ---------- 7. Matrix mapping ----------
       matrix_mapper = MatrixMapper()
       accrual_df["Assigned Location Code"] = accrual_df.apply(
           matrix_mapper.determine_profit_center,
           axis=1,
       )
       # ---------- 8. Join profit/cost centers ----------
       accrual_df = accrual_df.merge(
           complete_location_table[["Loc Code", "Prof_Cntr", "Cost_Cntr"]],
           left_on="Assigned Location Code",
           right_on="Loc Code",
           how="left",
       )
       accrual_df.rename(
           columns={
               "Prof_Cntr": "Profit Center EJ",
               "Cost_Cntr": "Cost Center EJ",
           },
           inplace=True,
       )
       # ---------- 9. Account # EJ rule ----------
       accrual_df["Account # EJ"] = accrual_df.apply(
           lambda row: 621000
           if row.get("Consignee Code") == row.get("Assigned Location Code")
           else 621020,
           axis=1,
       )
       return accrual_df
