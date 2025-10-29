import pandas as pd
from io_utils import FileIO
class Exporter:
   """Wrapper for creating downloadable binaries."""
   def export_full_excel(self, df: pd.DataFrame, sheet_name: str = "Re-Coded") -> bytes:
       return FileIO.try_export_excel(df, sheet_name=sheet_name)
   def export_csv(self, df: pd.DataFrame) -> bytes:
       return df.to_csv(index=False).encode("utf-8")
