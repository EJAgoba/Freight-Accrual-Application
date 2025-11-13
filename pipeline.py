# pipeline.py
import pandas as pd
from extract_codes import Extractor
from address_merge import CombinedAddress
from address_crossref import Merger
from map_types import TypeMapper, TypeCleaner
from matrix_map import MatrixMapper
class PipelineRunner:
   def run(self, accrual_df, cintas_location_table, complete_location_table, codes_df):
       # -------------- STEP 0: Normalize text --------------
       extractor = Extractor()
       extractor.create_columns(accrual_df)
       extractor.lower_columns(accrual_df, "Consignor", "Consignee")
       # -------------- STEP 1: Use Origin/Dest Type Code first --------------
       accrual_df = extractor.apply_type_code_priority(accrual_df)
       # -------------- STEP 2: Extract codes from text if still blank OR third party --------------
       accrual_df = extractor.extract_from_consignor_consignee(accrual_df, codes_df)
       # -------------- STEP 3: Build Combined Addresses --------------
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
       # -------------- STEP 4: Fill missing codes with Combined Address --------------
       merger = Merger()
       accrual_df = merger.merge_address_codes(accrual_df, cintas_location_table)
       # -------------- STEP 5: Map Types --------------
       mapper = TypeMapper()
       accrual_df = mapper.map_types(accrual_df, cintas_location_table, "Consignor Code", "Consignor Type")
       accrual_df = mapper.map_types(accrual_df, cintas_location_table, "Consignee Code", "Consignee Type")
       cleaner = TypeCleaner()
       accrual_df = cleaner.fill_non_cintas(accrual_df, "Consignor Type", "Consignee Type")
       # -------------- STEP 6: Profit Center Logic --------------
       matrix_mapper = MatrixMapper()
       accrual_df["Assigned Location Code"] = accrual_df.apply(
           matrix_mapper.determine_profit_center, axis=1
       )
       # -------------- STEP 7: Merge Profit/Cost Center --------------
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
       # -------------- STEP 8: Account EJ Rule --------------
       accrual_df["Account # EJ"] = accrual_df.apply(
           lambda row: 621000 if row["Consignee Code"] == row["Assigned Location Code"] else 621020,
           axis=1
       )
       return accrual_df
