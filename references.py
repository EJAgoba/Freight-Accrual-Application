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
   """
   Loads:
     - all_location_codes.xlsx  (returns DataFrame with 'Code' column)
     - MY LOCATION TABLE.xlsx
     - Complete Coding table
   """
   def load(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
       """Return (location_codes_df, cintas_location_table, complete_location_table)."""
       # 1) Find and read the Location Codes workbook
       codes_path = self._find_codes_path()
       df_codes_raw = pd.read_excel(codes_path)
       # Find which column in the workbook contains codes
       possible_cols = ["Codes", "Code", "Loc Code", "Loc_Code", "Location Code"]
       code_col = next(
           (c for c in possible_cols if c in df_codes_raw.columns),
           None
       )
       if code_col is None:
           raise ValueError(
               f"'{codes_path.name}' must contain one of these columns: {possible_cols}. "
               f"Found columns: {list(df_codes_raw.columns)}"
           )
       # Normalize into a single 'Code' column
       df_codes = df_codes_raw[[code_col]].copy()
       df_codes.rename(columns={code_col: "Code"}, inplace=True)
       df_codes["Code"] = (
           df_codes["Code"]
           .astype(str)
           .str.strip()
           .str.upper()
       )
       df_codes = df_codes[df_codes["Code"] != ""]  # drop blanks
       # 2) Load Cintas Location Table
       cintas_location_table = FileIO.read_excel_here(CINTAS_LOCATION_TABLE_FILE)
       # 3) Load Complete Coding Table
       complete_location_table = FileIO.read_excel_here(COMPLETE_LOCATION_TABLE_FILE)
       return df_codes, cintas_location_table, complete_location_table
   @staticmethod
   def _find_codes_path() -> Path:
       """Find the first matching 'Location Codes' file from LOCATION_CODES_CANDIDATES."""
       for fname in LOCATION_CODES_CANDIDATES:
           p = Path(__file__).with_name(fname)
           if p.exists():
               return p
       raise FileNotFoundError(
           "Location Codes file not found next to app.py. "
           f"Expected one of: {', '.join(LOCATION_CODES_CANDIDATES)}."
       )
