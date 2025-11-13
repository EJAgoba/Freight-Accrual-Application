# address_merge.py
import pandas as pd
import re

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
   Returns the FIRST TOKEN from the street.
   Tokens are chunks separated by whitespace.
   Example:
       '14601 SOVEREIGN RD' -> '14601'
       '5500 Bakersfield Dr' -> '5500'
       '1 W SUPERIOR AVE' -> '1'
       '2500WCR101' -> '2500WCR101' (no spaces so whole thing is token)
   """
   if val is None:
       return ""
   tokens = str(val).strip().split()
   if len(tokens) == 0:
       return ""
   return tokens[0]

class CombinedAddress:
   """
   NEW: Combined address is:
       first_word(street) + city_no_spaces + state_no_spaces
   """
   def create(self, df, target, street_col, city_col, state_col):
       if not all(col in df.columns for col in [street_col, city_col, state_col]):
           raise KeyError(
               f"Missing required address columns: {street_col}, {city_col}, {state_col}"
           )
       # Extract first token of street
       first_street = df[street_col].apply(_first_word).apply(_normalize_token)
       # City normalized (full city but no spaces)
       norm_city = df[city_col].apply(_normalize_token)
       # State normalized
       norm_state = df[state_col].apply(_normalize_token)
       df[target] = first_street + norm_city + norm_state
       return df
   # Backward compatibility with old pipeline
   def create_combined_address_accrual(self, df, target, street_col, city_col, state_col):
       return self.create(df, target, street_col, city_col, state_col)
