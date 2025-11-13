# pipeline.py
import pandas as pd
from extract_codes import Extractor
from address_merge import CombinedAddress
from address_crossref import Merger
from map_types import TypeMapper, TypeCleaner
from matrix_map import MatrixMapper
class PipelineRunner:
   def run(self, accrual_df, cintas_location_table, complete_location_table, codes_df):
       # ---------------- 1. Extract Codes ----------------
       extractor = Extractor()
       extractor.create_columns(accrual_df)
       extractor.lower_columns(accrual_df, "Consignor", "Consignee")
       accrual_df = extractor.extract_codes(accrual_df, codes_df)
       # ---------------- 2. Build Combined Addresses ----------------
       comb = CombinedAddress()
       cintas_location_table = comb.create(
           cintas_location_table,
           "Combined Address",
           "Loc_Address", "Loc_City", "Loc_ST"
       )
       accrual_df = comb.create(
           accrual_df,
           "Consignor Combined Address",
           "Origin Addresss", "Origin City", "Origin State Code"
       )
       accrual_df = comb.create(
           accrual_df,
           "Consignee Combined Address",
           "Dest Address1", "Dest City", "Dest State Code"
       )
       # ---------------- 3. Use address to fill missing codes ----------------
       merger = Merger()
       accrual_df = merger.merge_address_codes(accrual_df, cintas_location_table)
       # ---------------- 4. Fill Type Columns ----------------
       mapper = TypeMapper()
       accrual_df = mapper.map_types(accrual_df, cintas_location_table, "Consignor Code", "Consignor Type")
       accrual_df = mapper.map_types(accrual_df, cintas_location_table, "Consignee Code", "Consignee Type")
       cleaner = TypeCleaner()
       accrual_df = cleaner.fill_non_cintas(accrual_df, "Consignor Type", "Consignee Type")
       # ---------------- 5. Matrix Mapping ----------------
       matrix_mapper = MatrixMapper()
       accrual_df["Assigned Location Code"] = accrual_df.apply(
           matrix_mapper.determine_profit_center, axis=1
       )
       # ---------------- 6. Merge Profit/Cost Center ----------------
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
       # ---------------- 7. Account EJ Rule ----------------
       accrual_df["Account # EJ"] = accrual_df.apply(
           lambda row: (
               621000 if row.get("Consignee Code") == row.get("Assigned Location Code")
               else 621020
           ),
           axis=1,
       )
       # ---------------- 8. Final ordering ----------------
       first_cols = [
           "Profit Center", "Cost Center", "Account #",
           "Profit Center EJ", "Cost Center EJ", "Account # EJ",
           "Assigned Location Code", "Consignor Code", "Consignee Code"
       ]
       accrual_df = accrual_df[
           [col for col in first_cols if col in accrual_df.columns] +
           [col for col in accrual_df.columns if col not in first_cols]
       ]
       return accrual_df
