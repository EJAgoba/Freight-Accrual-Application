# pipeline.py
import pandas as pd
from address_merge import CombinedAddress
from address_crossref import Merger
from clean_codes import CodeFormatter
from map_types import TypeMapper, TypeCleaner
from matrix_map import MatrixMapper

class PipelineRunner:
   """Accrual re-coding pipeline with:
      1) Code search from Location Codes
      2) Fallback using Combined Address
   """
   def run(
       self,
       accrual_df: pd.DataFrame,
       cintas_location_table: pd.DataFrame,
       complete_location_table: pd.DataFrame,
       location_codes_df: pd.DataFrame,   # <- now a DataFrame, not list[str]
   ) -> pd.DataFrame:
       # ========= 1. Prepare and clean Location Codes list =========
       codes_series = (
           location_codes_df["Code"]
           .dropna()
           .astype(str)
           .str.strip()
           .str.upper()
           .unique()
       )
       def find_code_in_text(text: object) -> str | None:
           """Return the first code found inside the given text (case-insensitive)."""
           t = str(text or "").upper()
           if not t:
               return None
           for code in codes_series:
               if code and code in t:
                   return code
           return None
       # Make sure our code columns exist
       if "Consignor Code" not in accrual_df.columns:
           accrual_df["Consignor Code"] = pd.NA
       if "Consignee Code" not in accrual_df.columns:
           accrual_df["Consignee Code"] = pd.NA
       # ========= 2. FIRST: Try to pull codes from Consignor / Consignee text =========
       # Only fill where currently blank so we don't overwrite anything prefilled
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
       # Save what we got from this step so we can protect it later
       extracted_consignor = accrual_df["Consignor Code"].copy()
       extracted_consignee = accrual_df["Consignee Code"].copy()
       # ========= 3. Build Combined Address fields =========
       combined_address = CombinedAddress()
       combined_address.create_combined_address_accrual(
           cintas_location_table, "Combined Address", "Loc_Address", "Loc_City", "Loc_ST"
       )
       combined_address.create_combined_address_accrual(
           accrual_df, "Consignee Combined Address", "Dest Address1", "Dest City", "Dest State Code"
       )
       combined_address.create_combined_address_accrual(
           accrual_df, "Consignor Combined Address", "Origin Addresss", "Origin City", "Origin State Code"
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
       # ========= 4. Fallback: use address cross-ref (Merger) =========
       merger = Merger()
       accrual_df = merger.merge(accrual_df, cintas_location_table, "Consignor Code")
       accrual_df = merger.merge(accrual_df, cintas_location_table, "Consignee Code")
       # ========= 5. PROTECT codes we already found from text =========
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
       # ========= 6. Clean codes (pad, etc.) =========
       formatter = CodeFormatter()
       accrual_df = formatter.pad_codes(accrual_df, "Consignor Code", "Consignee Code")
       # ========= 7. Map Types based on codes =========
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
       # ========= 8. Matrix mapping: Assigned Location Code =========
       matrix_mapper = MatrixMapper()
       accrual_df["Assigned Location Code"] = accrual_df.apply(
           matrix_mapper.determine_profit_center,
           axis=1,
       )
       # ========= 9. Join Profit/Cost centers from complete location table =========
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
       # ========= 10. Account # EJ rule =========
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
       # ========= 11. De-dupe & Automation Accuracy =========
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
       # ========= 12. Column ordering =========
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
