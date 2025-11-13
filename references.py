from pathlib import Path
import pandas as pd
from typing import Tuple, List
from io_utils import FileIO
from constants import (
   CINTAS_LOCATION_TABLE_FILE, COMPLETE_LOCATION_TABLE_FILE, LOCATION_CODES_CANDIDATES
)
class ReferenceLoader:
   """Loads location codes, MY LOCATION TABLE, and the complete coding table."""
   def load(self) -> Tuple[List[str], pd.DataFrame, pd.DataFrame]:
       codes_path = self._find_codes_path()
       df_codes = pd.read_excel(codes_path)
       possible_cols = ["Codes", "Code", "Loc Code", "Loc_Code", "Location Code"]
       code_col = next((c for c in possible_cols if c in df_codes.columns), None)
       if code_col is None:
           raise ValueError(
               f"'{codes_path.name}' must contain one of these columns: {possible_cols}. "
               f"Found: {list(df_codes.columns)}"
           )
       location_codes = (
           df_codes[code_col]
           .dropna()
           .astype(str)
           .str.strip()
           .str.upper()
           .tolist()
       )
       cintas_location_table = FileIO.read_excel_here(CINTAS_LOCATION_TABLE_FILE)
       complete_location_table = FileIO.read_excel_here(COMPLETE_LOCATION_TABLE_FILE)
       return location_codes, cintas_location_table, complete_location_table
   @staticmethod
   def _find_codes_path() -> Path:
       codes_path = None
       for fname in LOCATION_CODES_CANDIDATES:
           p = Path(__file__).with_name(fname)
           if p.exists():
               codes_path = p
               break
       if codes_path is None:
           raise FileNotFoundError(
               "Location Codes Excel not found next to app.py. "
               f"Expected one of: {', '.join(LOCATION_CODES_CANDIDATES)}."
           )
       return codes_path
