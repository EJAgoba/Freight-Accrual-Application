# address_crossref.py
from __future__ import annotations
import pandas as pd

class Merger:
   """
   Address-based lookup that ONLY fills missing codes.
   - Uses Consignor/Consignee Combined Address to look up Loc Code
     from the Cintas location table.
   - If Consignor Code / Consignee Code is already populated
     (from text extract or Org/Dest), it will NOT be overwritten.
   """
   @staticmethod
   def _addr_col_for_code(code_col: str) -> str:
       """Map target code column → combined address column."""
       col = code_col.strip().lower()
       if "consignor" in col:
           return "Consignor Combined Address"
       if "consignee" in col:
           return "Consignee Combined Address"
       # Fails loudly if someone passes an unexpected column name
       raise ValueError(f"Unknown code column for address merge: {code_col!r}")
   def merge(
       self,
       accrual_df: pd.DataFrame,
       cintas_location_table: pd.DataFrame,
       code_col: str,
   ) -> pd.DataFrame:
       """
       Fill `code_col` (e.g. 'Consignor Code' / 'Consignee Code')
       using address cross-reference, but **only where that code is blank**.
       """
       if code_col not in accrual_df.columns:
           # nothing to do
           return accrual_df
       addr_col = self._addr_col_for_code(code_col)
       if addr_col not in accrual_df.columns:
           # no combined address → nothing to cross-ref
           return accrual_df
       # --- build a clean lookup table: Combined Address → Loc Code ---
       lut = cintas_location_table[["Combined Address", "Loc Code"]].copy()
       lut = lut.dropna(subset=["Combined Address", "Loc Code"])
       # If multiple locations share an address, keep the first. We are
       # only using this when we *have no code at all*.
       lut = lut.drop_duplicates(subset=["Combined Address"])
       # --- merge in Loc Code by address ---
       merged = accrual_df.merge(
           lut,
           how="left",
           left_on=addr_col,
           right_on="Combined Address",
           suffixes=("", "_addr"),
       )
       # --- only fill blanks; NEVER overwrite existing codes ---
       code_is_blank = (
           merged[code_col].isna()
           | (merged[code_col].astype(str).str.strip() == "")
       )
       merged.loc[code_is_blank, code_col] = merged.loc[code_is_blank, "Loc Code"]
       # --- clean up helper columns ---
       # 'Combined Address' is your original; 'Combined Address_addr' may appear from lut.
       for col in ["Loc Code", "Combined Address_addr"]:
           if col in merged.columns:
               merged = merged.drop(columns=[col])
       return merged
