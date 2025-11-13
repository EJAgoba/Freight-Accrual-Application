import pandas as pd
import streamlit as st
from address_merge import CombinedAddress
from address_crossref import Merger
from clean_codes import CodeFormatter
from map_types import TypeMapper, TypeCleaner
from matrix_map import MatrixMapper
DEBUG = False  # set True if you want the debug tables in the UI

class PipelineRunner:
   """Accrual re-coding pipeline:
      1) Try to detect codes from Consignor/Consignee text using Location Codes DF
      2) Fallback to Combined Address + Merger
      3) Type mapping, matrix mapping, joins, etc.
   """
   def run(
       self,
       accrual_df: pd.DataFrame,
       cintas_location_table: pd.DataFrame,
       complete_location_table: pd.DataFrame,
       location_codes_df: pd.DataFrame,   # DataFrame with 'Code' column
   ) -> pd.DataFrame:
       # ========= 0. Ensure required code/type columns exist =========
       for col in ["Consignor Code", "Consignee Code", "Consignor Type", "Consignee Type"]:
           if col not in accrual_df.columns:
               accrual_df[col] = pd.NA
       # ========= 1. Prep Location Codes =========
       if "Code" not in location_codes_df.columns:
           raise ValueError("location_codes_df must contain a 'Code' column.")
       codes_series = (
           location_codes_df["Code"]
               .dropna()
               .astype(str)
               .str.strip()
               .str.upper()
               .unique()
       )
       if DEBUG:
           st.write("🔎 DEBUG – Location codes loaded:")
           st.write("Count:", len(codes_series))
           st.write("First 30 codes:", list(codes_series[:30]))
       def find_code_in_text(text: object) -> str | None:
           """Return the first code found inside the given text (case-insensitive)."""
           t = str(text or "").upper()
           if not t:
               return None
           for code in codes_series:
               if code and code in t:
                   return code
           return None
       # ========= 2. DEBUG: sample match check =========
       if DEBUG:
           st.write("🔎 DEBUG – Sample Consignor text → code match")
           sample = accrual_df[["Consignor", "Consignee"]].head(15)
           debug_rows = []
           for idx, row in sample.iterrows():
               cons = row.get("Consignor")
               cons_code = find_code_in_text(cons)
               dest = row.get("Consignee")
               dest_code = find_code_in_text(dest)
               debug_rows.append({
                   "row_index": idx,
                   "Consignor": cons,
                   "Consignor_match": cons_code,
                   "Consignee": dest,
                   "Consignee_match": dest_code,
               })
           st.dataframe(pd.DataFrame(debug_rows))
       # ========= 3. First pass: extract codes from Consignor / Consignee text =========
       cons_mask = (
           accrual_df["Consignor Code"].isna()
           | (accrual_df["Consignor Code"].astype(str).str.strip() == "")
       )
       dest_mask = (
           accrual_df["Consignee Code"].isna()
           | (accrual_df["Consignee Code"].astype(str).str.strip() == "")
       )
       accrual_df.loc[cons_mask, "Consignor Code"] = (
           accrual_df.loc[cons_mask, "Consignor"].apply(find_code_in_text)
       )
       accrual_df.loc[dest_mask, "Consignee Code"] = (
           accrual_df.loc[dest_mask, "Consignee"].apply(find_code_in_text)
       )
       if DEBUG:
           st.write("🔎 DEBUG – After first pass of text extraction:")
           st.write(
               accrual_df[["Consignor", "Consignor Code", "Consignee", "Consignee Code"]]
               .head(20)
           )
       # Save what we got so address merge won't overwrite
       extracted_consignor = accrual_df["Consignor Code"].copy()
       extracted_consignee = accrual_df["Consignee Code"].copy()
       # ========= 4. Combined Address fields =========
       combined_address = CombinedAddress()
       combined_address.create_combined_address_accrual(
           cintas_location_table, "Combined Address", "Loc_Address", "Loc_City", "Loc_ST"
       )
       combined_address.create_combined_address_accrual(
           accrual_df, "Consignee Combined Address",
           "Dest Address1", "Dest City", "Dest State Code"
       )
       combined_address.create_combined_address_accrual(
           accrual_df, "Consignor Combined Address",
           "Origin Addresss", "Origin City", "Origin State Code"
       )
       cintas_location_table["Combined Address"] = (
           cintas_location_table["Combined Address"].astype(str).str.upper()
       )
       accrual_df["Consignee Combined Address"] = (
           accrual_df["Consignee Combined Address"].astype(str).str.upper()
       )
       accrual_df["Consignor Combined Address"] = (
           accrual_df["Consignor Combined Address"].astype(str).str.upper()
       )
       # ========= 5. Address-based merge fallback =========
       merger = Merger()
       accrual_df = merger.merge(accrual_df, cintas_location_table, "Consignor Code")
       accrual_df = merger.merge(accrual_df, cintas_location_table, "Consignee Code")
       # ========= 6. Re-protect codes we got from text =========
       cons_keep_mask = (
           extracted_consignor.notna()
& (extracted_consignor.astype(str).str.strip() != "")
       )
       dest_keep_mask = (
           extracted_consignee.notna()
& (extracted_consignee.astype(str).str.strip() != "")
       )
       accrual_df.loc[cons_keep_mask, "Consignor Code"] = extracted_consignor[cons_keep_mask]
       accrual_df.loc[dest_keep_mask, "Consignee Code"] = extracted_consignee[dest_keep_mask]
       # ========= 7. Clean codes (pad, etc.) =========
       formatter = CodeFormatter()
       accrual_df = formatter.pad_codes(accrual_df, "Consignor Code", "Consignee Code")
       # ========= 8. Map Types based on codes =========
       type_mapper = TypeMapper()
       accrual_df = type_mapper.map_types(
           accrual_df, cintas_location_table,
           "Consignor Code", "Consignor Type"
       )
       accrual_df = type_mapper.map_types(
           accrual_df, cintas_location_table,
           "Consignee Code", "Consignee Type"
       )
       # Ensure type columns exist (defensive, in case map_types no-ops)
       for col in ["Consignor Type", "Consignee Type"]:
           if col not in accrual_df.columns:
               accrual_df[col] = pd.NA
       cleaner = TypeCleaner()
       accrual_df = cleaner.fill_non_cintas(
           accrual_df, "Consignor Type", "Consignee Type"
       )
       # ========= 9. Matrix mapping: Assigned Location Code =========
       matrix_mapper = MatrixMapper()
       accrual_df["Assigned Location Code"] = accrual_df.apply(
           matrix_mapper.determine_profit_center,
           axis=1,
       )
       # ========= 10. Join Profit/Cost centers from complete location table =========
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
       # ========= 11. Account # EJ rule =========
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
       # ========= 12. De-dupe & Automation Accuracy =========
       if {"Invoice Number", "Paid Amount"}.issubset(accrual_df.columns):
           accrual_df = accrual_df.drop_duplicates(
               subset=["Invoice Number", "Paid Amount"]
           )
       if {"Profit Center", "Profit Center EJ"}.issubset(accrual_df.columns):
           pc = accrual_df["Profit Center"]
           pc_ej = accrual_df["Profit Center EJ"]
           match = (pc == pc_ej) & pc.notna() & pc_ej.notna()
           accrual_df["Automation Accuracy"] = match.fillna(False).astype(int)
       else:
           accrual_df["Automation Accuracy"] = 0
       # ========= 13. Column ordering =========
       first_cols = [
           "Profit Center",
           "Cost Center",
           "Account #",
           "Automation Accuracy",
           "Profit Center EJ",
           "Cost Center EJ",
           "Account # EJ",
       ]
       ordered = (
           [c for c in first_cols if c in accrual_df.columns]
           + [c for c in accrual_df.columns if c not in first_cols]
       )
       accrual_df = accrual_df[ordered]
       return accrual_df
