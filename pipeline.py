# pipeline.py
import pandas as pd
from extract_codes import Extractor
from address_merge import CombinedAddress
from address_crossref import Merger
from clean_codes import CodeFormatter
from map_types import TypeMapper, TypeCleaner
from matrix_map import MatrixMapper

class PipelineRunner:
   """Accrual re-coding pipeline."""
   def run(
       self,
       accrual_df: pd.DataFrame,
       cintas_location_table: pd.DataFrame,
       complete_location_table: pd.DataFrame,
       location_codes: list[str],
   ) -> pd.DataFrame:
       # ---------- 1. Extract location codes from text ----------
       extractor = Extractor()
       extractor.create_columns(accrual_df)  # creates Consignor/Consignee Code + Type cols
       extractor.lower_columns(accrual_df, "Consignor", "Consignee")
       # Use all_location_codes against Consignor/Consignee text.
       # We set only_null=False so text ALWAYS wins.
       extractor.extract1(
           accrual_df,
           df_column="Consignor",
           new_column="Consignor Code",
           location_codes=location_codes,
           only_null=False,
       )
       extractor.extract1(
           accrual_df,
           df_column="Consignee",
           new_column="Consignee Code",
           location_codes=location_codes,
           only_null=False,
       )
       # Optional prefill from Org/Dest columns if you still want that behaviour.
       extractor.prefill_from_loc_columns(accrual_df, location_codes)
       # ---------- 2. Build combined addresses ----------
       comb = CombinedAddress()
       # MY LOCATION TABLE side
       comb.create_combined_address_accrual(
           cintas_location_table,
           "Combined Address",
           "Loc_Address",
           "Loc_City",
           "Loc_ST",
       )
       # Accrual file side
       comb.create_combined_address_accrual(
           accrual_df,
           "Consignee Combined Address",
           "Dest Address1",
           "Dest City",
           "Dest State Code",
       )
       comb.create_combined_address_accrual(
           accrual_df,
           "Consignor Combined Address",
           "Origin Addresss",  # keep your original column name
           "Origin City",
           "Origin State Code",
       )
       cintas_location_table["Combined Address"] = (
           cintas_location_table["Combined Address"].astype(str).str.upper()
       )
       accrual_df["Consignor Combined Address"] = (
           accrual_df["Consignor Combined Address"].astype(str).str.upper()
       )
       accrual_df["Consignee Combined Address"] = (
           accrual_df["Consignee Combined Address"].astype(str).str.upper()
       )
       # ---------- 3. Address cross-ref (DO NOT overwrite extracted codes) ----------
       merger = Merger()
       # assumes Merger.merge(..., overwrite=False) respects existing codes
       accrual_df = merger.merge(
           accrual_df,
           cintas_location_table,
           target_code_column="Consignor Code",
           overwrite=False,
       )
       accrual_df = merger.merge(
           accrual_df,
           cintas_location_table,
           target_code_column="Consignee Code",
           overwrite=False,
       )
       # ---------- 4. Clean code formatting ----------
       formatter = CodeFormatter()
       accrual_df = formatter.pad_codes(
           accrual_df,
           "Consignor Code",
           "Consignee Code",
       )
       # ---------- 5. Type mapping + Non-Cintas fill ----------
       type_mapper = TypeMapper()
       accrual_df = type_mapper.map_types(
           accrual_df,
           cintas_location_table,
           "Consignor Code",
           "Consignor Type",
       )
       accrual_df = type_mapper.map_types(
           accrual_df,
           cintas_location_table,
           "Consignee Code",
           "Consignee Type",
       )
       cleaner = TypeCleaner()
       accrual_df = cleaner.fill_non_cintas(
           accrual_df,
           "Consignor Type",
           "Consignee Type",
       )
       # ---------- 6. Matrix mapping → Assigned Location Code ----------
       matrix_mapper = MatrixMapper()
       accrual_df["Assigned Location Code"] = accrual_df.apply(
           matrix_mapper.determine_profit_center,
           axis=1,
       )
       # ---------- 7. Join Profit/Cost centers from complete table ----------
       if {"Loc Code", "Prof_Cntr", "Cost_Cntr"}.issubset(complete_location_table.columns):
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
       else:
           # keep columns but as NaN if complete table is missing or wrong
           accrual_df["Profit Center EJ"] = pd.NA
           accrual_df["Cost Center EJ"] = pd.NA
       # ---------- 8. Account # EJ rule (G59 + destination rule) ----------
       def _account_rule(row):
           pc_ej = str(row.get("Profit Center EJ", "") or "")
           if "G59" in pc_ej:
               return 621000
           # destination code match
           if row.get("Consignee Code") == row.get("Assigned Location Code"):
               return 621000
           return 621020
       accrual_df["Account # EJ"] = accrual_df.apply(_account_rule, axis=1)
       # ---------- 9. De-dupe on Invoice Number + Paid Amount ----------
       if {"Invoice Number", "Paid Amount"}.issubset(accrual_df.columns):
           accrual_df = accrual_df.drop_duplicates(
               subset=["Invoice Number", "Paid Amount"]
           )
       # ---------- 10. Automation Accuracy ----------
       if "Profit Center" in accrual_df.columns and "Profit Center EJ" in accrual_df.columns:
           accrual_df["Profit Center"] = accrual_df["Profit Center"].astype("string")
           accrual_df["Profit Center EJ"] = accrual_df["Profit Center EJ"].astype("string")
           accrual_df["Automation Accuracy"] = accrual_df.apply(
               lambda row: (
                   1
                   if (
                       pd.notna(row["Profit Center"])
                       and pd.notna(row["Profit Center EJ"])
                       and row["Profit Center"] == row["Profit Center EJ"]
                   )
                   else 0
               ),
               axis=1,
           )
       else:
           accrual_df["Automation Accuracy"] = 0
       # ---------- 11. Column ordering ----------
       first_cols = [
           "Profit Center",
           "Cost Center",
           "Account #",
           "Automation Accuracy",
           "Profit Center EJ",
           "Cost Center EJ",
           "Account # EJ",
       ]
       ordered = [
           c for c in first_cols if c in accrual_df.columns
       ] + [
           c for c in accrual_df.columns if c not in first_cols
       ]
       accrual_df = accrual_df[ordered]
       return accrual_df
