# references.py
from pathlib import Path
from typing import Tuple
import pandas as pd
from io_utils import FileIO
from constants import (
   CINTAS_LOCATION_TABLE_FILE,
   COMPLETE_LOCATION_TABLE_FILE,
   LOCATION_CODES_CANDIDATES,
)

class ReferenceLoader:
   """
   Loads:
     - location_codes_df (with a 'Code' column, derived from 'Codes' in Excel)
     - Cintas Location Table
     - Complete Coding Table
   """
   def load(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
       codes_path = self._find_codes_path()
       df_codes_raw = pd.read_excel(codes_path)
       # Normalize column names (keep exact 'Codes' if that's what you have)
       df_codes_raw.columns = [c.strip() for c in df_codes_raw.columns]
       # Your file has 'Codes' – map that to 'Code' for the pipeline
       if "Codes" in df_codes_raw.columns:
           code_col = "Codes"
       else:
           # Fallback if you ever rename later
           possible_cols = ["Code", "Loc Code", "Loc_Code", "Location Code"]
           code_col = next((c for c in possible_cols if c in df_codes_raw.columns), None)
           if code_col is None:
               raise ValueError(
                   "Location Codes file must contain a 'Codes' column "
                   f"or one of {possible_cols}. Found: {list(df_codes_raw.columns)}"
               )
       # Build location_codes_df with the exact column name 'Code'
       location_codes_df = df_codes_raw[[code_col]].rename(columns={code_col: "Code"})
       location_codes_df["Code"] = (
           location_codes_df["Code"]
           .astype(str)
           .str.upper()
           .str.strip()
       )
       location_codes_df = location_codes_df[location_codes_df["Code"] != ""]
       # Load the other two reference tables
       cintas_location_table = FileIO.read_excel_here(CINTAS_LOCATION_TABLE_FILE)
       complete_location_table = FileIO.read_excel_here(COMPLETE_LOCATION_TABLE_FILE)
       return location_codes_df, cintas_location_table, complete_location_table
   @staticmethod
   def _find_codes_path() -> Path:
       """
       Search for the location-codes workbook using LOCATION_CODES_CANDIDATES.
       """
       base = Path(__file__).resolve().parent
       for fname in LOCATION_CODES_CANDIDATES:
           p = base / fname
           if p.exists():
               return p
       raise FileNotFoundError(
           "Location Codes Excel not found in the app folder. "
           f"Expected one of: {', '.join(LOCATION_CODES_CANDIDATES)}"
       )
