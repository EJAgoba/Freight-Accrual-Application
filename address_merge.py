# address_merge.py
import pandas as pd

def _normalize_token(val: str) -> str:
   """
   Uppercase + remove spaces/punctuation.
   """
   return (
       str(val or "")
       .upper()
       .replace(" ", "")
       .replace(",", "")
       .replace(".", "")
       .replace("-", "")
       .strip()
   )

def _first_word(val: str) -> str:
   """
   Return first token from street.
   Examples:
       '14601 SOVEREIGN RD CENTERPORT BLDG 4' -> '14601'
       '5600 CHICAGO AVE SUITE 4'            -> '5600'
       '1 W SUPERIOR AVE'                    -> '1'
       '2500WCR101'                          -> '2500WCR101'
   """
   if val is None:
       return ""
   tokens = str(val).strip().split()
   if not tokens:
       return ""
   return tokens[0]

class CombinedAddress:
   """
   Combined Address = FIRST_WORD(street) + CITY(no spaces) + STATE(no spaces)
   Supports both:
     - create()
     - create_combined_address_accrual() (for backward compatibility)
   """
   def create(self, df, target: str, street_col: str, city_col: str, state_col: str):
       # Allow no-op if someone accidentally passes a list
       if not isinstance(df, pd.DataFrame):
           return df
       for col in (street_col, city_col, state_col):
           if col not in df.columns:
               raise KeyError(
                   f"Missing required address column '{col}'. "
                   f"Available columns: {list(df.columns)}"
               )
       first_street = df[street_col].apply(_first_word).apply(_normalize_token)
       norm_city = df[city_col].apply(_normalize_token)
       norm_state = df[state_col].apply(_normalize_token)
       df[target] = first_street + norm_city + norm_state
       return df
   def create_combined_address_accrual(
       self,
       df,
       target: str,
       street_col: str,
       city_col: str,
       state_col: str,
   ):
       """
       Old method name used in older code.
       Now simply calls create().
       """
       return self.create(df, target, street_col, city_col, state_col)
