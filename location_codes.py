# location_codes.py  — cloud-safe, no absolute paths
import pandas as pd
from pathlib import Path
def load_location_codes():
   """
   Load a list of codes from an Excel file in the SAME FOLDER as this file.
   Tries a few common filenames and sheet/column names.
   Returns: List[str] (e.g., ["K35","003R","..."])
   """
   candidates = [
       Path(__file__).with_name("Location Codes.xlsx"),
       Path(__file__).with_name("LOCATION_CODES.xlsx"),
       Path(__file__).with_name("location_codes.xlsx"),
   ]
   path = next((p for p in candidates if p.exists()), None)
   if path is None:
       raise FileNotFoundError(
           "Could not find a Location Codes Excel next to location_codes.py "
           "(looked for: 'Location Codes.xlsx', 'LOCATION_CODES.xlsx', 'location_codes.xlsx')."
       )
   # Try a few likely sheets and column names
   possible_sheets = [None, "Sheet1", "Codes", "Locations"]
   possible_cols   = ["Codes", "Code", "Loc Code", "Loc_Code"]
   # read first sheet that works
   df = None
   for sh in possible_sheets:
       try:
           df = pd.read_excel(path, sheet_name=sh)
           break
       except Exception:
           continue
   if df is None:
       raise ValueError(f"Could not read any sheet from {path.name}")
   col = next((c for c in possible_cols if c in df.columns), None)
   if col is None:
       raise ValueError(
           f"'{path.name}' must contain a column named one of {possible_cols}. "
           f"Found columns: {list(df.columns)}"
       )
   codes = (
       df[col]
       .dropna()
       .astype(str)
       .str.strip()
       .str.upper()
       .tolist()
   )
   return codes
# exported symbol your app expects
location_codes = load_location_codes()
