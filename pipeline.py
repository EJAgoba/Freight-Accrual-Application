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
       # ---------- 8) Account # EJ rule (avoid boolean NA) ----------
       def _account_rule(row) -> int:
           pc_ej = row.get("Profit Center EJ")
           # case 1: any G59 profit center → 621000
           if isinstance(pc_ej, str) and "G59" in pc_ej:
               return 621000
           cons_code = row.get("Consignee Code")
           assigned = row.get("Assigned Location Code")
           # normalize to strings only if not NA
           if cons_code is None or cons_code is pd.NA:
               cons_norm = None
           else:
               cons_norm = str(cons_code)
           if assigned is None or assigned is pd.NA:
               asg_norm = None
           else:
               asg_norm = str(assigned)
           # case 2: consignee code == assigned loc code → 621000
           if cons_norm is not None and asg_norm is not None and cons_norm == asg_norm:
               return 621000
           # otherwise 621020
           return 621020
       accrual_df["Account # EJ"] = accrual_df.apply(_account_rule, axis=1)
       # ---------- 9) De-dupe on Invoice Number + Paid Amount (if present) ----------
       if {"Invoice Number", "Paid Amount"}.issubset(accrual_df.columns):
           accrual_df = accrual_df.drop_duplicates(
               subset=["Invoice Number", "Paid Amount"]
           )
       # ---------- 10) Automation accuracy (avoid boolean NA) ----------
       if "Profit Center" in accrual_df.columns:
           accrual_df["Profit Center"] = accrual_df["Profit Center"].astype("string")
       accrual_df["Profit Center EJ"] = accrual_df["Profit Center EJ"].astype("string")
       def _accuracy_rule(row) -> int:
           pc = row.get("Profit Center")
           pc_ej = row.get("Profit Center EJ")
           # treat None / NA as "no match"
           if pc is None or pc is pd.NA:
               return 0
           if pc_ej is None or pc_ej is pd.NA:
               return 0
           return 1 if str(pc) == str(pc_ej) else 0
       accrual_df["Automation Accuracy"] = accrual_df.apply(
           _accuracy_rule, axis=1
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
