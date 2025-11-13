# references.py
import pandas as pd
from pathlib import Path
from constants import (
   CINTAS_LOCATION_TABLE_FILE,
   COMPLETE_LOCATION_TABLE_FILE,
   LOCATION_CODES_CANDIDATES
)
from io_utils import FileIO

class ReferenceLoader:
   def load(self):
       codes_path = self._find_codes_path()
       df_codes = pd.read_excel(codes_path)
       # force normalize all column names
       df_codes.columns = [c.strip().title() for c in df_codes.columns]
       # Find correct column
       possible_cols = ["Codes", "Code", "Loc Code", "Location Code"]
       code_col = next((c for c in possible_cols if c in df_codes.columns), None)
       if code_col is None:
           raise ValueError(
               f"Location Codes file must contain one of {possible_cols}. "
               f"Found: {list(df_codes.columns)}"
           )
       # Return DataFrame with guaranteed 'Codes' column
       df_codes = df_codes[[code_col]].rename(columns={code_col: "Codes"})
       df_codes["Codes"] = df_codes["Codes"].astype(str).str.upper().str.strip()
       cintas_location_table = FileIO.read_excel_here(CINTAS_LOCATION_TABLE_FILE)
       complete_location_table = FileIO.read_excel_here(COMPLETE_LOCATION_TABLE_FILE)
       return df_codes, cintas_location_table, complete_location_table
   @staticmethod
   def _find_codes_path():
       for fname in LOCATION_CODES_CANDIDATES:
           path = Path(__file__).with_name(fname)
           if path.exists():
               return path
       raise FileNotFoundError(
           "Location Codes Excel not found. "
           f"Expected one of: {', '.join(LOCATION_CODES_CANDIDATES)}"
       )
