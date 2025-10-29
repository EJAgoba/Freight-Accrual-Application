import csv
import pandas as pd
class UploadReader:
   """Reads uploaded files for Accrual and Weekly Audit."""
   def read(self, file, kind: str) -> pd.DataFrame:
       name = (file.name or "").lower()
       if kind == "Accrual":
           return pd.read_excel(file)  # accrual is xlsx
       # Weekly Audit: allow .txt/.text/.csv/.xlsx
       if name.endswith((".txt", ".text", ".csv")):
           return self._read_weekly_text_to_df(file)
       return pd.read_excel(file)
   @staticmethod
   def _read_weekly_text_to_df(ufile) -> pd.DataFrame:
       """Convert uploaded .txt/.csv into a DataFrame. Auto-detect delimiter."""
       sample = ufile.read(4096)
       ufile.seek(0)
       try:
           sniff = csv.Sniffer().sniff(sample.decode("utf-8", errors="ignore"))
           sep = sniff.delimiter
       except Exception:
           sep = "\t" if b"\t" in sample else ","
       return pd.read_csv(
           ufile, sep=sep, dtype=str, engine="python",
           encoding="latin1", keep_default_na=False
       )
