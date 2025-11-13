# references.py
from pathlib import Path
import pandas as pd
from typing import Tuple
from io_utils import FileIO
from constants import (
   CINTAS_LOCATION_TABLE_FILE,
   COMPLETE_LOCATION_TABLE_FILE,
   LOCATION_CODES_CANDIDATES,
)
class ReferenceLoader:
   """Loads:
     - all_location_codes.xlsx (as DataFrame with a 'Code' column)
     - MY LOCATION TABLE.xlsx
     - Coding_CintasLocation 11.05.25.xlsx
   """
   def load(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
       # 1) Find and read the Location Codes workbook
       codes_path = self._find_codes_path()
       df_codes_raw = pd.read_excel(codes_path)
       # Find which column actually holds the codes
       possible_cols = ["Codes", "Code", "Loc Code", "Loc_Code", "Location Code"]
       code_col = next((c for c in possible_cols if c in df_codes_raw.columns), None)
       if code_col is None:
           raise ValueError(
               f"'{codes_path.name}' must contain one of these columns: {possible_cols}. "
               f"Found: {list(df_codes_raw.columns)}"
           )
       # Normalize to a single 'Code' column, cleaned & uppercased
       df_codes = df_codes_raw[[code_col]].copy()
       df_codes.rename(columns={code_col: "Code"}, inplace=True)
       df_codes["Code"] = (
           df_codes["Code"]
           .astype(str)
           .str.strip()
           .str.upper()
       )
       df_codes = df_codes[df_codes["Code"] != ""]  # drop blanks
       # 2) Cintas Location Table
       cintas_location_table = FileIO.read_excel_here(CINTAS_LOCATION_TABLE_FILE)
       # 3) Complete Coding Table
       complete_location_table = FileIO.read_excel_here(COMPLETE_LOCATION_TABLE_FILE)
       return df_codes, cintas_location_table, complete_location_table
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
