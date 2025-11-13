# references.py
from pathlib import Path
import hashlib
import pandas as pd
def _canon_code(s: pd.Series) -> pd.Series:
   return (s.astype(str)
             .str.strip()
             .str.upper()
             .str.replace(r"\s+", "", regex=True))
def _file_hash(p: Path) -> str:
   return hashlib.sha1(p.read_bytes()).hexdigest()
class ReferenceLoader:
   def load(self):
       here = Path(__file__).parent
       # --- FULL list of location codes ---
       codes_path = here / "all_location_codes.xlsx"
       df_codes = pd.read_excel(codes_path, dtype=str)
       code_col = "Codes"
       location_codes = (
           df_codes[code_col]
           .dropna()
           .astype(str)
           .str.strip()
           .str.upper()
           .tolist()
       )
       # --- load MY LOCATION TABLE ---
       loc_tbl_path = here / "MY LOCATION TABLE.xlsx"
       loc_tbl = pd.read_excel(loc_tbl_path, dtype=str)
       if "Loc Code" in loc_tbl.columns:
           loc_tbl["Loc Code"] = _canon_code(loc_tbl["Loc Code"])
       # --- load complete coding table ---
       comp_tbl_path = here / "Coding_CintasLocation 11.05.25.xlsx"
       comp_tbl = pd.read_excel(comp_tbl_path, dtype=str)
       if "Loc Code" in comp_tbl.columns:
           comp_tbl["Loc Code"] = _canon_code(comp_tbl["Loc Code"])
           comp_tbl = comp_tbl.drop_duplicates(subset=["Loc Code"])
       return location_codes, loc_tbl, comp_tbl
