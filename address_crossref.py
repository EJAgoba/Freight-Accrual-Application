# address_crossref.py
import pandas as pd

class Merger:
   """
   Cross-reference combined address to fill missing Consignor/Consignee codes.
   IMPORTANT: we never overwrite an existing code – we only fill blanks.
   """
   def merge(
       self,
       accrual_df: pd.DataFrame,
       location_table: pd.DataFrame,
       code_col: str,
   ) -> pd.DataFrame:
       """
       code_col: 'Consignor Code' or 'Consignee Code'
       Uses:
         - accrual_df[addr_col]          (Consignor/Consignee Combined Address)
         - location_table['Combined Address'], location_table['Loc Code']
       """
       if code_col == "Consignor Code":
           addr_col = "Consignor Combined Address"
       elif code_col == "Consignee Code":
           addr_col = "Consignee Combined Address"
       else:
           # unknown; do nothing
           return accrual_df
       if addr_col not in accrual_df.columns:
           return accrual_df
       if "Combined Address" not in location_table.columns or "Loc Code" not in location_table.columns:
           return accrual_df
       # normalise location table
       loc_ref = location_table[["Combined Address", "Loc Code"]].copy()
       loc_ref["Combined Address"] = (
           loc_ref["Combined Address"].astype(str).str.upper().str.strip()
       )
       loc_ref["Loc Code"] = loc_ref["Loc Code"].astype(str).str.upper().str.strip()
       # one row per combined address (first wins – but only for rows that have NO code yet)
       loc_ref = loc_ref.drop_duplicates(subset=["Combined Address"])
       # mask rows where code is missing/blank
       existing = accrual_df[code_col].astype(str).str.strip()
       mask_missing = (existing == "") | existing.isna()
       if not mask_missing.any():
           # nothing to fill
           return accrual_df
       # work only on the missing subset
       df_missing = accrual_df.loc[mask_missing, [addr_col]].copy()
       df_missing[addr_col] = df_missing[addr_col].astype(str).str.upper().str.strip()
       merged = df_missing.merge(
           loc_ref,
           left_on=addr_col,
           right_on="Combined Address",
           how="left",
       )
       # write back loc codes ONLY for those missing rows
       new_codes = merged["Loc Code"].fillna("").astype(str).str.upper().str.strip()
       accrual_df.loc[mask_missing, code_col] = new_codes.values
       return accrual_df
