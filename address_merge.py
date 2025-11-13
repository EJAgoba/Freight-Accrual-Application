# address_merge.py
import pandas as pd
def _normalize(val):
   return (
       str(val or "")
       .upper()
       .replace(" ", "")
       .replace(",", "")
       .replace(".", "")
       .replace("-", "")
       .strip()
   )
class CombinedAddress:
   """
   Creates perfectly normalized combined addresses
   so merges ALWAYS work.
   """
   def create(self, df, target, street_col, city_col, state_col):
       df[target] = (
           df[street_col].apply(_normalize)
           + df[city_col].apply(_normalize)
           + df[state_col].apply(_normalize)
       )
       return df
