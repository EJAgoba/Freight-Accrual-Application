import io
from pathlib import Path
import pandas as pd
class FileIO:
   """Local file helpers for reading references and exporting outputs."""
   @staticmethod
   def read_excel_here(filename: str) -> pd.DataFrame:
       """Read an Excel that sits next to app.py; raise clean error if missing."""
       path = Path(__file__).with_name(filename)
       if not path.exists():
           raise FileNotFoundError(f"Missing required file: '{filename}' in the same folder as app.py.")
       return pd.read_excel(path)
   @staticmethod
   def try_export_excel(df: pd.DataFrame, sheet_name: str = "Re-Coded") -> bytes:
       """Return XLSX bytes; prefer xlsxwriter; fallback to openpyxl."""
       bio = io.BytesIO()
       try:
           with pd.ExcelWriter(bio, engine="xlsxwriter") as w:
               df.to_excel(w, index=False, sheet_name=sheet_name)
       except Exception:
           bio = io.BytesIO()
           with pd.ExcelWriter(bio, engine="openpyxl") as w:
               df.to_excel(w, index=False, sheet_name=sheet_name)
       bio.seek(0)
       return bio.read()
