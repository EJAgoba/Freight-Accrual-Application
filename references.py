# references.py
from pathlib import Path
import pandas as pd
import hashlib

def _canon_code(s: pd.Series) -> pd.Series:
   """Normalize location codes: strip, uppercase, remove all whitespace."""
   return (
       s.astype(str)
       .str.strip()
       .str.upper()
       .str.replace(r"\s+", "", regex=True)
   )

def _file_hash(p: Path) -> str:
   """Creates a hash so Streamlit cache resets when the Excel changes."""
   return hashlib.sha1(p.read_bytes()).hexdigest()

class ReferenceLoader:
   def load(self):
       """
       Loads 3 things:
       1. location_codes (list[str]) from all_location_codes.xlsx
       2. Cintas location table (MY LOCATION TABLE.xlsx)
       3. Complete location mapping table (Coding_CintasLocation 11.05.25.xlsx)
       """
       here = Path(__file__).parent
       # ================
       # 1. Full list of Cintas location codes
       # ================
       loc_codes_path = here / "all_location_codes.xlsx"
       df_codes = pd.read_excel(loc_codes_path, dtype=str)
       code_col = "Codes"
       if code_col not in df_codes.columns:
           raise ValueError(f"'all_location_codes.xlsx' must contain a '{code_col}' column.")
       location_codes = (
           df_codes[code_col]
           .dropna()
           .astype(str)
           .str.strip()
           .str.upper()
           .tolist()
       )
       # ================
       # 2. MY LOCATION TABLE.xlsx
       # ================
       cintas_loc_path = here / "MY LOCATION TABLE.xlsx"
       cintas_loc_tbl = pd.read_excel(cintas_loc_path, dtype=str)
       if "Loc Code" in cintas_loc_tbl.columns:
           cintas_loc_tbl["Loc Code"] = _canon_code(cintas_loc_tbl["Loc Code"])
       # Normalize the columns used for address matching
       for col in ["Loc_Address", "Loc_City", "Loc_ST", "Combined Address"]:
           if col in cintas_loc_tbl.columns:
               cintas_loc_tbl[col] = cintas_loc_tbl[col].astype(str).str.upper()
       # ================
       # 3. Coding_CintasLocation (complete table)
       # ================
       comp_path = here / "Coding_CintasLocation 11.05.25.xlsx"
       comp_tbl = pd.read_excel(comp_path, dtype=str)
       if "Loc Code" in comp_tbl.columns:
           comp_tbl["Loc Code"] = _canon_code(comp_tbl["Loc Code"])
           comp_tbl = comp_tbl.drop_duplicates(subset=["Loc Code"])
       return location_codes, cintas_loc_tbl, comp_tbl
