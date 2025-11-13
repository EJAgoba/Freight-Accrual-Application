# references.py
from pathlib import Path
import pandas as pd
import hashlib
def _canon(s: pd.Series) -> pd.Series:
   return (
       s.astype(str)
        .str.strip()
        .str.upper()
        .str.replace(r"\s+", "", regex=True)
   )
def _file_hash(path: Path) -> str:
   return hashlib.sha1(path.read_bytes()).hexdigest()
class ReferenceLoader:
   def load(self):
       here = Path(__file__).parent
       # Load MY LOCATION TABLE.xlsx
       loc_path = here / "MY LOCATION TABLE.xlsx"
       comp_path = here / "Coding_CintasLocation 02.06.25.xlsx"
       allcodes_path = here / "all_location_codes.xlsx"
       # Cache identifiers
       _ = (_file_hash(loc_path), _file_hash(comp_path), _file_hash(allcodes_path))
       # Load tables
       loc_tbl = pd.read_excel(loc_path, dtype=str)
       comp_tbl = pd.read_excel(comp_path, dtype=str)
       allcodes = pd.read_excel(allcodes_path, dtype=str)
       # Clean tables
       if "Loc Code" in loc_tbl.columns:
           loc_tbl["Loc Code"] = _canon(loc_tbl["Loc Code"])
       if "Loc Code" in comp_tbl.columns:
           comp_tbl["Loc Code"] = _canon(comp_tbl["Loc Code"])
           comp_tbl = comp_tbl.drop_duplicates(subset=["Loc Code"])
       # Load all location codes
       code_col = next(
           (c for c in ["Codes", "Code", "Loc Code"] if c in allcodes.columns),
           None
       )
       if code_col is None:
           raise ValueError("all_location_codes.xlsx must contain a code column.")
       location_codes = (
           allcodes[code_col]
               .dropna()
               .astype(str)
               .str.strip()
               .str.upper()
               .tolist()
       )
       return location_codes, loc_tbl, comp_tbl
