# extract_codes.py
import pandas as pd
import re
class Extractor:
   def create_columns(self, df):
       for col in ["Consignor Code", "Consignee Code"]:
           if col not in df.columns:
               df[col] = None
   def lower_columns(self, df, *cols):
       for c in cols:
           if c in df.columns:
               df[c] = df[c].astype(str).str.upper()
   def apply_type_code_priority(self, df):
       """
       Priority 1: Use Origin Type Code and Dest Type Code directly.
       """
       if "Origin Type Code" in df.columns:
           df["Consignor Code"] = df["Origin Type Code"].astype(str).str.strip().replace({"": None})
       if "Dest Type Code" in df.columns:
           df["Consignee Code"] = df["Dest Type Code"].astype(str).str.strip().replace({"": None})
       return df
   # STRICT extractor — no partial matches
   def extract_code_from_string(self, text, codes_df):
       if text is None:
           return None
       text = str(text).upper()
       for code in codes_df["Codes"]:
           code = str(code).upper().strip()
           if re.search(rf"\b{re.escape(code)}\b", text):
               return code
       return None
   def extract_from_consignor_consignee(self, df, codes_df):
       """
       Only fill codes where still blank OR marked as third party.
       """
       def needs_fill(code, type_val):
           if code is None or code == "" or str(type_val).upper().startswith("THIRD"):
               return True
           return False
       # CONSIGNOR
       df["Consignor Code"] = df.apply(
           lambda row: self.extract_code_from_string(row["Consignor"], codes_df)
           if needs_fill(row["Consignor Code"], row.get("Consignor Type", ""))
           else row["Consignor Code"],
           axis=1,
       )
       # CONSIGNEE
       df["Consignee Code"] = df.apply(
           lambda row: self.extract_code_from_string(row["Consignee"], codes_df)
           if needs_fill(row["Consignee Code"], row.get("Consignee Type", ""))
           else row["Consignee Code"],
           axis=1,
       )
       return df
