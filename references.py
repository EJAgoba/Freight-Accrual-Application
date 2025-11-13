# references.py
from pathlib import Path
import hashlib
import pandas as pd
from constants import (
   CINTAS_LOCATION_TABLE_FILE,
   COMPLETE_LOCATION_TABLE_FILE,
   LOCATION_CODES_CANDIDATES,
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
   """Generate hash so Streamlit cache refreshes when Excel file changes."""
   return hashlib.sha1(p.read_bytes()).hexdigest()

class ReferenceLoader:
   def load(self):
       here = Path(__file__).parent
       # --- main reference tables (MY LOCATION TABLE + complete coding) ---
       loc_path = here / CINTAS_LOCATION_TABLE_FILE
       comp_path = here / COMPLETE_LOCATION_TABLE_FILE
       # touch for cache invalidation
       _ = (_file_hash(loc_path), _file_hash(comp_path))
       # read as text so we never lose leading zeros like '061R'
       loc_tbl = pd.read_excel(loc_path, dtype=str)
       comp_tbl = pd.read_excel(comp_path, dtype=str)
       # normalize Loc Code in both tables
       if "Loc Code" in loc_tbl.columns:
           loc_tbl["Loc Code"] = _canon_code(loc_tbl["Loc Code"])
       for c in ["Prof_Cntr", "Cost_Cntr", "Profit Center", "Cost Center"]:
           if c in loc_tbl.columns:
               loc_tbl[c] = loc_tbl[c].astype(str).str.strip().str.upper()
       if "Loc Code" in comp_tbl.columns:
           comp_tbl["Loc Code"] = _canon_code(comp_tbl["Loc Code"])
           comp_tbl = comp_tbl.drop_duplicates(subset=["Loc Code"])
       # --- NEW: build location_codes from all_location_codes.xlsx (or similar) ---
       # LOCATION_CODES_CANDIDATES can be a single string or a list in constants.py
       candidates = LOCATION_CODES_CANDIDATES
       if isinstance(candidates, str):
           candidates = [candidates]
       codes_path = None
       for fname in candidates:
           p = here / fname
           if p.exists():
               codes_path = p
               break
       if codes_path is not None:
           codes_df = pd.read_excel(codes_path, dtype=str)
           possible_cols = ["Codes", "Code", "Loc Code", "Loc_Code", "Location Code"]
           code_col = next((c for c in possible_cols if c in codes_df.columns), None)
           if code_col is None:
               raise ValueError(
                   f"'{codes_path.name}' must contain one of {possible_cols}. "
                   f"Found: {list(codes_df.columns)}"
               )
           location_codes = (
               codes_df[code_col]
               .dropna()
               .astype(str)
               .str.strip()
               .str.upper()
               .tolist()
           )
       else:
           # fallback: at least use the Loc Codes from MY LOCATION TABLE
           if "Loc Code" in loc_tbl.columns:
               location_codes = loc_tbl["Loc Code"].dropna().astype(str).tolist()
           else:
               location_codes = []
       return location_codes, loc_tbl, comp_tbl
