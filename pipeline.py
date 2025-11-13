# pipeline.py
import pandas as pd
from extract_codes import Extractor
from address_merge import CombinedAddress
from address_crossref import Merger
from clean_codes import CodeFormatter
from map_types import TypeMapper, TypeCleaner
from matrix_map import MatrixMapper

class PipelineRunner:
   def run(self, accrual_df, cintas_location_table, complete_location_table, location_codes):
       extractor = Extractor()
       extractor.create_columns(accrual_df)
       extractor.lower_columns(accrual_df, "Consignor", "Consignee")
       # --------------- CRITICAL FIX ----------------
       # Always extract from location_codes FIRST
       extractor.extract1(accrual_df, "Consignor", "Consignor Code", location_codes, only_null=False)
       extractor.extract1(accrual_df, "Consignee", "Consignee Code", location_codes, only_null=False)
       # ------------------------------------------------
       # Combined Address
       comb = CombinedAddress()
       comb.create_combined_address_accrual(
           cintas_location_table, "Combined Address", "Loc_Address", "Loc_City", "Loc_ST"
       )
       comb.create_combined_address_accrual(
           accrual_df, "Consignor Combined Address", "Origin Address", "Origin City", "Origin State Code"
       )
       comb.create_combined_address_accrual(
           accrual_df, "Consignee Combined Address", "Dest Address1", "Dest City", "Dest State Code"
       )
       # Cross-reference (but DO NOT overwrite extracted codes)
       merger = Merger()
       accrual_df = merger.merge(accrual_df, cintas_location_table, "Consignor Code", overwrite=False)
       accrual_df = merger.merge(accrual_df, cintas_location_table, "Consignee Code", overwrite=False)
       # Code cleanup
       formatter = CodeFormatter()
       accrual_df = formatter.pad_codes(accrual_df, "Consignor Code", "Consignee Code")
       # Type mapping
       tmap = TypeMapper()
       accrual_df = tmap.map_types(accrual_df, cintas_location_table, "Consignor Code", "Consignor Type")
       accrual_df = tmap.map_types(accrual_df, cintas_location_table, "Consignee Code", "Consignee Type")
       cleaner = TypeCleaner()
       accrual_df = cleaner.fill_non_cintas(accrual_df, "Consignor Type", "Consignee Type")
       # Matrix mapping
       mapper = MatrixMapper()
       accrual_df["Assigned Location Code"] = accrual_df.apply(mapper.determine_profit_center, axis=1)
       # Join Profit/Cost centers
       accrual_df = accrual_df.merge(
           complete_location_table[["Loc Code", "Prof_Cntr", "Cost_Cntr"]],
           left_on="Assigned Location Code",
           right_on="Loc Code",
           how="left",
       )
       accrual_df.rename(
           columns={"Prof_Cntr": "Profit Center EJ", "Cost_Cntr": "Cost Center EJ"}, inplace=True
       )
       return accrual_df
