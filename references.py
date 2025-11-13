# references.py
from pathlib import Path
import hashlib
import pandas as pd
# Exact file names (same folder as app.py)
CINTAS_LOCATION_TABLE_FILE   = "MY LOCATION TABLE.xlsx"
COMPLETE_LOCATION_TABLE_FILE = "Coding_CintasLocation 11.05.25.xlsx"
LOCATION_CODES_FILE          = "all_location_codes.xlsx"

def _canon_code(s: pd.Series) -> pd.Series:
   """Normalize codes: strip, uppercase, remove spaces."""
   return (
       s.astype(str)
        .str.strip()
        .str.upper()
        .str.replace(r"\s+", "", regex=True)
   )

def _file_hash(p: Path) -> str:
   """Hash so Streamlit cache refreshes when Excel file changes."""
   return hashlib.sha1(p.read_bytes()).hexdigest()

class ReferenceLoader:
   def load(self):
       here = Path(__file__).parent
       # ---------- Location codes (for Extractor) ----------
       loc_codes_path = here / LOCATION_CODES_FILE
       if not loc_codes_path.exists():
           raise FileNotFoundError(
               f"Location codes file '{LOCATION_CODES_FILE}' not found next to app.py."
           )
       df_codes = pd.read_excel(loc_codes_path, dtype=str)
       if "Codes" not in df_codes.columns:
           raise ValueError(
               f"'{LOCATION_CODES_FILE}' must contain a 'Codes' column. "
               f"Found: {list(df_codes.columns)}"
           )
       location_codes = (
           df_codes["Codes"]
               .dropna()
               .astype(str)
               .str.strip()
               .str.upper()
               .tolist()
       )
       # ---------- MY LOCATION TABLE ----------
       loc_path = here / CINTAS_LOCATION_TABLE_FILE
       if not loc_path.exists():
           raise FileNotFoundError(
               f"'{CINTAS_LOCATION_TABLE_FILE}' not found next to app.py."
           )
       # hash for Streamlit cache invalidation (even if we don't use the value)
       _ = _file_hash(loc_path)
       cintas_tbl = pd.read_excel(loc_path, dtype=str)
       if "Loc Code" in cintas_tbl.columns:
           cintas_tbl["Loc Code"] = _canon_code(cintas_tbl["Loc Code"])
       # ---------- Complete coding table ----------
       comp_path = here / COMPLETE_LOCATION_TABLE_FILE
       if not comp_path.exists():
           raise FileNotFoundError(
               f"'{COMPLETE_LOCATION_TABLE_FILE}' not found next to app.py."
           )
       _ = _file_hash(comp_path)
       comp_tbl = pd.read_excel(comp_path, dtype=str)
       if "Loc Code" in comp_tbl.columns:
           comp_tbl["Loc Code"] = _canon_code(comp_tbl["Loc Code"])
           # one row per loc code
           comp_tbl = comp_tbl.drop_duplicates(subset=["Loc Code"])
       # shape: (location_codes list, MY LOCATION TABLE df, complete coding df)
       return location_codes, cintas_tbl, comp_tbl
