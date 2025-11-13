# pipeline.py
import pandas as pd
import streamlit as st
import re
from address_merge import CombinedAddress
from address_crossref import Merger
from clean_codes import CodeFormatter
from map_types import TypeMapper, TypeCleaner
from matrix_map import MatrixMapper
DEBUG = False

class PipelineRunner:
   """Full accrual re-coding pipeline."""
   def run(
       self,
       accrual_df: pd.DataFrame,
       cintas_location_table: pd.DataFrame,
       complete_location_table: pd.DataFrame,
       location_codes_df: pd.DataFrame,
   ) -> pd.DataFrame:
       # ===============================================================
       # 0. Required columns
       # ===============================================================
       for col in ["Consignor Code", "Consignee Code", "Consignor Type", "Consignee Type"]:
           if col not in accrual_df.columns:
               accrual_df[col] = pd.NA
       # ===============================================================
       # 1. Prepare codes
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
       codes_sorted = sorted(codes_series, key=len, reverse=True)
       # helper
       def find_code_in_text(text):
           t = str(text or "").upper()
           tokens = re.findall(r"[A-Z0-9]+", t)
           for code in codes_sorted:
               if code in tokens:
                   return code
           return None
       # ===============================================================
       # 2. PRIORITY RULE – If Origin/Dest Type Code is filled, use that FIRST
       # ===============================================================
       if "Origin Type Code" in accrual_df.columns:
           mask = accrual_df["Origin Type Code"].notna() & (accrual_df["Origin Type Code"].astype(str).str.strip() != "")
           accrual_df.loc[mask, "Consignor Code"] = accrual_df.loc[mask, "Origin Type Code"].astype(str).str.upper()
       if "Dest Type Code" in accrual_df.columns:
           mask = accrual_df["Dest Type Code"].notna() & (accrual_df["Dest Type Code"].astype(str).str.strip() != "")
           accrual_df.loc[mask, "Consignee Code"] = accrual_df.loc[mask, "Dest Type Code"].astype(str).str.upper()
       # ===============================================================
       # 3. Extract from text ONLY IF type is blank or THIRD-PARTY
       # ===============================================================
       def needs_text_extract(type_col):
           return (type_col.isna()) | (type_col.astype(str).str.upper().str.contains("THIRD"))
       cons_extract_mask = needs_text_extract(accrual_df["Consignor Type"]) & (
           accrual_df["Consignor Code"].isna() | (accrual_df["Consignor Code"].astype(str).str.strip() == "")
       )
       dest_extract_mask = needs_text_extract(accrual_df["Consignee Type"]) & (
           accrual_df["Consignee Code"].isna() | (accrual_df["Consignee Code"].astype(str).str.strip() == "")
       )
       accrual_df.loc[cons_extract_mask, "Consignor Code"] = \
           accrual_df.loc[cons_extract_mask, "Consignor"].apply(find_code_in_text)
       accrual_df.loc[dest_extract_mask, "Consignee Code"] = \
           accrual_df.loc[dest_extract_mask, "Consignee"].apply(find_code_in_text)
       extracted_consignor = accrual_df["Consignor Code"].copy()
       extracted_consignee = accrual_df["Consignee Code"].copy()
       # ===============================================================
       # 4. Combined Address (unchanged)
       # ===============================================================
       comb = CombinedAddress()
       comb.create(
           cintas_location_table,
           "Combined Address",
           "Loc_Address",
           "Loc_City",
           "Loc_ST",
       )
       comb.create(
           accrual_df,
           "Consignee Combined Address",
           "Dest Address1",
           "Dest City",
           "Dest State Code",
       )
       comb.create(
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
       # 5. Address merging ONLY when type is blank or THIRD-PARTY
       # ===============================================================
       merger = Merger()
       addr_consignor_mask = needs_text_extract(accrual_df["Consignor Type"])
       addr_consignee_mask = needs_text_extract(accrual_df["Consignee Type"])
       if addr_consignor_mask.any():
           accrual_df = merger.merge(accrual_df, cintas_location_table, "Consignor Code")
       if addr_consignee_mask.any():
           accrual_df = merger.merge(accrual_df, cintas_location_table, "Consignee Code")
       # restore extracted
       accrual_df.loc[extracted_consignor.notna(), "Consignor Code"] = extracted_consignor
       accrual_df.loc[extracted_consignee.notna(), "Consignee Code"] = extracted_consignee
       # ===============================================================
       # 6. Format codes
       # ===============================================================
       formatter = CodeFormatter()
       accrual_df = formatter.pad_codes(accrual_df, "Consignor Code", "Consignee Code")
       # ===============================================================
       # 7. Type Mapping
       # ===============================================================
       mapper = TypeMapper()
       accrual_df = mapper.map_types(accrual_df, cintas_location_table, "Consignor Code", "Consignor Type")
       accrual_df = mapper.map_types(accrual_df, cintas_location_table, "Consignee Code", "Consignee Type")
       cleaner = TypeCleaner()
       accrual_df = cleaner.fill_non_cintas(accrual_df, "Consignor Type", "Consignee Type")
       # ===============================================================
       # 8. Matrix Mapping
       # ===============================================================
       matrix = MatrixMapper()
       accrual_df["Assigned Location Code"] = accrual_df.apply(matrix.determine_profit_center, axis=1)
       # ===============================================================
       # 9. Join Profit/Cost Center
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
       # 10. Account # EJ
       # ===============================================================
       accrual_df["Account # EJ"] = accrual_df.apply(
           lambda row:
               621000 if "G59" in str(row.get("Profit Center EJ", "")) else
               (621000 if row.get("Consignee Code") == row.get("Assigned Location Code") else 621020),
           axis=1,
       )
       # ===============================================================
       # 11. De-dupe
       # ===============================================================
       if {"Invoice Number", "Paid Amount"}.issubset(accrual_df.columns):
           accrual_df = accrual_df.drop_duplicates(subset=["Invoice Number", "Paid Amount"])
       # ===============================================================
       # 12. Automation Accuracy
       # ===============================================================
       if {"Profit Center", "Profit Center EJ"}.issubset(accrual_df.columns):
           match = (
               (accrual_df["Profit Center"] == accrual_df["Profit Center EJ"]) &
               accrual_df["Profit Center"].notna() &
               accrual_df["Profit Center EJ"].notna()
           )
           accrual_df["Automation Accuracy"] = match.astype(int)
       else:
           accrual_df["Automation Accuracy"] = 0
       # ===============================================================
       # 13. Column Ordering
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
