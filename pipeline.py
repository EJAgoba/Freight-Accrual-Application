# pipeline.py
import pandas as pd
from extract_codes import Extractor
from address_merge import CombinedAddress
from address_crossref import Merger
from clean_codes import CodeFormatter
from map_types import TypeMapper, TypeCleaner
from matrix_map import MatrixMapper

class PipelineRunner:
   """Encapsulates the accrual recoding pipeline as a single callable."""
   def run(
       self,
       accrual_df: pd.DataFrame,
       cintas_location_table: pd.DataFrame,
       complete_location_table: pd.DataFrame,
       location_codes: list[str],
   ) -> pd.DataFrame:
       # ---------- 1) Extract Location Codes from text / Org & Dest columns ----------
       extractor = Extractor()
       # create empty code/type columns (string, upper-case)
       extractor.create_columns(accrual_df)
       # lower-case consignor/consignee text for search
       extractor.lower_columns(accrual_df, "Consignor", "Consignee")
       # prefill codes from Org Type Code / Dest Type Code when valid
       extractor.prefill_from_loc_columns(accrual_df, location_codes)
       # now extract codes directly from the Consignor / Consignee text,
       # but ONLY where the code column is still blank
       extractor.extract1(
           accrual_df,
           "Consignor",
           "Consignor Code",
           location_codes,
           only_null=True,
       )
       extractor.extract1(
           accrual_df,
           "Consignee",
           "Consignee Code",
           location_codes,
           only_null=True,
       )
       # ---------- 2) Build combined addresses ----------
       combined_address = CombinedAddress()
       # master table combined address
       combined_address.create_combined_address_accrual(
           cintas_location_table,
           "Combined Address",
           "Loc_Address",
           "Loc_City",
           "Loc_ST",
       )
       # shipment destination combined address
       combined_address.create_combined_address_accrual(
           accrual_df,
           "Consignee Combined Address",
           "Dest Address1",
           "Dest City",
           "Dest State Code",
       )
       # shipment origin combined address
       combined_address.create_combined_address_accrual(
           accrual_df,
           "Consignor Combined Address",
           "Origin Addresss",
           "Origin City",
           "Origin State Code",
       )
       # normalize to upper for safe joins
       cintas_location_table["Combined Address"] = (
           cintas_location_table["Combined Address"].astype(str).str.upper()
       )
       accrual_df["Consignee Combined Address"] = (
           accrual_df["Consignee Combined Address"].astype(str).str.upper()
       )
       accrual_df["Consignor Combined Address"] = (
           accrual_df["Consignor Combined Address"].astype(str).str.upper()
       )
       # ---------- 3) Cross-reference by combined address (FILL BLANK CODES ONLY) ----------
       merger = Merger()
       accrual_df = merger.merge(accrual_df, cintas_location_table, "Consignor Code")
       accrual_df = merger.merge(accrual_df, cintas_location_table, "Consignee Code")
       # ---------- 4) Clean up codes (pad) ----------
       formatter = CodeFormatter()
       accrual_df = formatter.pad_codes(accrual_df, "Consignor Code", "Consignee Code")
       # ---------- 5) Map Type Codes ----------
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
       # ---------- 6) Matrix mapping → Assigned Location Code ----------
       matrix_mapper = MatrixMapper()
       accrual_df["Assigned Location Code"] = accrual_df.apply(
           matrix_mapper.determine_profit_center, axis=1
       )
       # ---------- 7) Join profit/cost centers from complete location table ----------
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
       # ---------- 8) Account # EJ rule ----------
       accrual_df["Account # EJ"] = accrual_df.apply(
           lambda row: 621000
           if "G59" in str(row.get("Profit Center EJ", ""))
           else (
               621000
               if row.get("Consignee Code") == row.get("Assigned Location Code")
               else 621020
           ),
           axis=1,
       )
       # ---------- 9) De-dupe on Invoice Number + Paid Amount (if present) ----------
       if {"Invoice Number", "Paid Amount"}.issubset(accrual_df.columns):
           accrual_df = accrual_df.drop_duplicates(
               subset=["Invoice Number", "Paid Amount"]
           )
       # ---------- 10) Automation accuracy ----------
       if "Profit Center" in accrual_df.columns:
           accrual_df["Profit Center"] = accrual_df["Profit Center"].astype("string")
       accrual_df["Profit Center EJ"] = accrual_df["Profit Center EJ"].astype("string")
       accrual_df["Automation Accuracy"] = accrual_df.apply(
           lambda row: 1
           if (
               pd.notna(row.get("Profit Center"))
               and pd.notna(row.get("Profit Center EJ"))
               and row.get("Profit Center") == row.get("Profit Center EJ")
           )
           else 0,
           axis=1,
       )
       # ---------- 11) Column ordering ----------
       first_cols = [
           "Profit Center",
           "Cost Center",
           "Account #",
           "Automation Accuracy",
           "Profit Center EJ",
           "Cost Center EJ",
           "Account # EJ",
       ]
       ordered = [c for c in first_cols if c in accrual_df.columns] + [
           c for c in accrual_df.columns if c not in first_cols
       ]
       accrual_df = accrual_df[ordered]
       return accrual_df
