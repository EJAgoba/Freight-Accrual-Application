# references.py
from pathlib import Path
import hashlib
import pandas as pd
from constants import (
   CINTAS_LOCATION_TABLE_FILE,
   COMPLETE_LOCATION_TABLE_FILE,
   LOCATION_CODES_CANDIDATES,   # <-- uses your all_location_codes file name
)

def _canon_code(s: pd.Series) -> pd.Series:
   """Normalize codes (remove spaces, uppercase, strip)."""
   return (
       s.astype(str)
        .str.strip()
        .str.upper()
        .str.replace(r"\s+", "", regex=True)
   )

def _file_hash(p: Path) -> str:
   """Generate hash so cache refreshes when Excel file changes."""
   return hashlib.sha1(p.read_bytes()).hexdigest()

def _load_location_codes(here: Path) -> list[str]:
   """
   Load the Cintas location codes from LOCATION_CODES_CANDIDATES
   (e.g. all_location_codes.xlsx), using the 'Codes' column.
   """
   codes_path = here / LOCATION_CODES_CANDIDATES
   if not codes_path.exists():
       # If the file is missing, fail clearly so you know what to fix
       raise FileNotFoundError(
           f"Location codes file '{LOCATION_CODES_CANDIDATES}' "
           "not found next to app.py / references.py."
       )
   codes_df = pd.read_excel(codes_path, dtype=str)
   possible_cols = ["Codes", "Code", "Loc Code", "Loc_Code", "Location Code"]
   code_col = next((c for c in possible_cols if c in codes_df.columns), None)
   if code_col is None:
       raise ValueError(
           f"'{LOCATION_CODES_CANDIDATES}' must contain one of these "
           f"columns: {possible_cols}. Found: {list(codes_df.columns)}"
       )
   location_codes = (
       _canon_code(codes_df[code_col])
       .dropna()
       .tolist()
   )
   return location_codes

class ReferenceLoader:
   def load(self):
       here = Path(__file__).parent
       loc_path  = here / CINTAS_LOCATION_TABLE_FILE
       comp_path = here / COMPLETE_LOCATION_TABLE_FILE
       # Hash for Streamlit cache invalidation
       _ = (_file_hash(loc_path), _file_hash(comp_path))
       # --- Read location tables as text to keep leading zeros ---
       loc_tbl  = pd.read_excel(loc_path, dtype=str)
       comp_tbl = pd.read_excel(comp_path, dtype=str)
       # --- Clean up both tables ---
       if "Loc Code" in loc_tbl.columns:
           loc_tbl["Loc Code"] = _canon_code(loc_tbl["Loc Code"])
       for c in ["Prof_Cntr", "Cost_Cntr", "Profit Center", "Cost Center"]:
           if c in loc_tbl.columns:
               loc_tbl[c] = (
                   loc_tbl[c]
                   .astype(str)
                   .str.strip()
                   .str.upper()
               )
       if "Loc Code" in comp_tbl.columns:
           comp_tbl["Loc Code"] = _canon_code(comp_tbl["Loc Code"])
           comp_tbl = comp_tbl.drop_duplicates(subset=["Loc Code"])
       # --- Build location_codes from all_location_codes.xlsx (Codes column) ---
       location_codes = _load_location_codes(here)
       return location_codes, loc_tbl, comp_tbl
