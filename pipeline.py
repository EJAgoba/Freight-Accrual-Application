# pipeline.py
import pandas as pd
import streamlit as st
import re
from address_merge import CombinedAddress
from address_crossref import Merger
from clean_codes import CodeFormatter
from map_types import TypeMapper, TypeCleaner
from matrix_map import MatrixMapper

DEBUG = False  # flip to True to see debug tables in Streamlit

class PipelineRunner:
   """Complete accrual re-coding pipeline:
      1) Detect codes from text (Consignor/Consignee)
      2) Fallback to Combined Address matching
      3) Type mapping + Non-Cintas filling
      4) Matrix mapping (Assigned Location Code)
      5) Profit/Cost center joining
      6) Automation accuracy calculation
      7) Output cleaning and ordering
   """
   def run(
       self,
       accrual_df: pd.DataFrame,
       cintas_location_table: pd.DataFrame,
       complete_location_table: pd.DataFrame,
       location_codes_df: pd.DataFrame,    # must have “Code” column
   ) -> pd.DataFrame:
       # ===============================================================
       # 0. Ensure required columns exist
       # ===============================================================
       for col in ["Consignor Code", "Consignee Code", "Consignor Type", "Consignee Type"]:
           if col not in accrual_df.columns:
               accrual_df[col] = pd.NA
       # ===============================================================
       # 1. Prepare location codes
       # ===============================================================
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
       # Sort codes longest → shortest: ensures “067N” beats “67”
       codes_sorted = sorted(codes_series, key=len, reverse=True)
       if DEBUG:
           st.write("🔍 DEBUG: Loaded codes:", len(codes_sorted))
           st.write(list(codes_sorted[:20]))
       # ===============================================================
       # 2. Code extraction helper (token-based matching)
       # ===============================================================
       def find_code_in_text(text: object) -> str | None:
           """
           Find a code inside text by matching WHOLE tokens.
           Example:
               text = "CINTAS 067N 14601 SOW FORT WORTH TX"
               tokens → ["CINTAS","067N","14601","SOW","FORT","WORTH","TX"]
               Returns "067N", not "67".
           """
           t = str(text or "").upper()
           if not t:
               return None
           # break Consignor/Consignee into words/tokens
           tokens = re.findall(r"[A-Z0-9]+", t)
           # check codes longest → shortest
           for code in codes_sorted:
               if code in tokens:
                   return code
           return None
       # ===============================================================
       # 3. First pass – extract from Consignor & Consignee text
       # ===============================================================
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
           st.write("🔍 DEBUG Post-text extraction:")
           st.dataframe(
               accrual_df[["Consignor", "Consignor Code", "Consignee", "Consignee Code"]].head(20)
           )
       # Preserve what we extracted so address merge cannot overwrite
       extracted_consignor = accrual_df["Consignor Code"].copy()
       extracted_consignee = accrual_df["Consignee Code"].copy()
       # ===============================================================
       # 4. Combined Address generation
       # ===============================================================
       combined = CombinedAddress()
       combined.create_combined_address_accrual(
           cintas_location_table,
           "Combined Address",
           "Loc_Address",
           "Loc_City",
           "Loc_ST",
       )
       combined.create_combined_address_accrual(
           accrual_df,
           "Consignee Combined Address",
           "Dest Address1",
           "Dest City",
           "Dest State Code",
       )
       combined.create_combined_address_accrual(
           accrual_df,
           "Consignor Combined Address",
           "Origin Addresss",
           "Origin City",
           "Origin State Code",
       )
       cintas_location_table["Combined Address"] = cintas_location_table["Combined Address"].astype(str).str.upper()
       accrual_df["Consignee Combined Address"] = accrual_df["Consignee Combined Address"].astype(str).str.upper()
       accrual_df["Consignor Combined Address"] = accrual_df["Consignor Combined Address"].astype(str).str.upper()
       # ===============================================================
       # 5. Fallback merging (address-based)
       # ===============================================================
       merger = Merger()
       accrual_df = merger.merge(accrual_df, cintas_location_table, "Consignor Code")
       accrual_df = merger.merge(accrual_df, cintas_location_table, "Consignee Code")
       # ===============================================================
       # 6. Restore the codes found from text
       # ===============================================================
       cons_keep = extracted_consignor.notna() & (extracted_consignor.astype(str).str.strip() != "")
       dest_keep = extracted_consignee.notna() & (extracted_consignee.astype(str).str.strip() != "")
       accrual_df.loc[cons_keep, "Consignor Code"] = extracted_consignor[cons_keep]
       accrual_df.loc[dest_keep, "Consignee Code"] = extracted_consignee[dest_keep]
       # ===============================================================
       # 7. Format/pad codes
       # ===============================================================
       formatter = CodeFormatter()
       accrual_df = formatter.pad_codes(accrual_df, "Consignor Code", "Consignee Code")
       # ===============================================================
       # 8. Type mapping
       # ===============================================================
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
       # Ensure type columns exist
       for col in ["Consignor Type", "Consignee Type"]:
           if col not in accrual_df.columns:
               accrual_df[col] = pd.NA
       cleaner = TypeCleaner()
       accrual_df = cleaner.fill_non_cintas(
           accrual_df, "Consignor Type", "Consignee Type"
       )
       # ===============================================================
       # 9. Matrix mapping (Assigned Location Code)
       # ===============================================================
       matrix_mapper = MatrixMapper()
       accrual_df["Assigned Location Code"] = accrual_df.apply(
           matrix_mapper.determine_profit_center,
           axis=1,
       )
       # ===============================================================
       # 10. Join profit/cost center table
       # ===============================================================
       accrual_df = accrual_df.merge(
           complete_location_table[["Loc Code", "Prof_Cntr", "Cost_Cntr"]],
           left_on="Assigned Location Code",
           right_on="Loc Code",
           how="left",
       )
       accrual_df.rename(
           columns={"Prof_Cntr": "Profit Center EJ", "Cost_Cntr": "Cost Center EJ"},
           inplace=True,
       )
       # ===============================================================
       # 11. Account # EJ rule
       # ===============================================================
       accrual_df["Account # EJ"] = accrual_df.apply(
           lambda row:
               621000 if "G59" in str(row.get("Profit Center EJ", "")) else
               (621000 if row.get("Consignee Code") == row.get("Assigned Location Code")
                else 621020),
           axis=1,
       )
       # ===============================================================
       # 12. De-dupe
       # ===============================================================
       if {"Invoice Number", "Paid Amount"}.issubset(accrual_df.columns):
           accrual_df = accrual_df.drop_duplicates(subset=["Invoice Number", "Paid Amount"])
       # ===============================================================
       # 13. Automation Accuracy
       # ===============================================================
       if {"Profit Center", "Profit Center EJ"}.issubset(accrual_df.columns):
           pc = accrual_df["Profit Center"]
           pc_ej = accrual_df["Profit Center EJ"]
           match = (pc == pc_ej) & pc.notna() & pc_ej.notna()
           accrual_df["Automation Accuracy"] = match.astype(int)
       else:
           accrual_df["Automation Accuracy"] = 0
       # ===============================================================
       # 14. Column ordering
       # ===============================================================
       first_cols = [
           "Profit Center",
           "Cost Center",
           "Account #",
           "Automation Accuracy",
           "Profit Center EJ",
           "Cost Center EJ",
           "Account # EJ",
       ]
       final_cols = (
           [c for c in first_cols if c in accrual_df.columns] +
           [c for c in accrual_df.columns if c not in first_cols]
       )
       return accrual_df[final_cols]
