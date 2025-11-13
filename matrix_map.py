# matrix_map.py
import pandas as pd
from coding_matrix import SPECIAL_TYPE_MAPPINGS, Coding_Matrix
# --- Static Code Sets ---
SPECIAL_CODES = {'0K35', '024P', '067N'}
# Location codes for 67N
LOCATION_CODES_67N = {
   '029N', '030N', '031N', '032N', '033N', '034N', '039N', '042N', '045N', '046N', '048N',
   '055N', '060N', '0827', '0839', '0847', '0850', '0851', '0857', '0881', '0882', '0884',
   '0885', '0886', '0888', '0889', '0903', '0951', '0W17'
}
# Add 897 logic
# Location codes for 97H
LOCATION_CODES_97H = {'029G', '030G', '031G'}

# --- Utility: normalize text ---
def _norm(x: object) -> str:
   """Normalize text: uppercase, strip spaces, remove hidden characters."""
   if not isinstance(x, str):
       return ""
   s = x.upper().strip()
   s = s.replace("\u00A0", "").replace("\u200B", "")
   s = " ".join(s.split())  # collapse extra spaces
   return s

class MatrixMapper:
   """Robust mapping for Assigned Location Code."""
   def _get_carrier(self, row: pd.Series) -> str:
       """Safely extract carrier name from any possible column name."""
       for k in ("Carrier Name", "Carrier", "CarrierName"):
           v = row.get(k)
           if isinstance(v, str) and v.strip():
               return v.strip().lower()
       return ""
   def determine_profit_center(self, row: pd.Series):
       # --- Pull & normalize fields ---
       consignor = _norm(row.get("Consignor"))
       consignee = _norm(row.get("Consignee"))
       consignee_code = _norm(row.get("Consignee Code"))
       consignor_code = _norm(row.get("Consignor Code"))
       origin_address = _norm(row.get("Origin Address"))
       consignor_type = _norm(row.get("Consignor Type"))
       consignee_type = _norm(row.get("Consignee Type"))
       carrier_lc = self._get_carrier(row)
       consignor_lower = consignor.lower()
       if "matheson" in consignor_lower and "fs" in consignor_lower:
           return "067N"
       # --- 1️⃣ Carrier override: Omnitrans = always charge to DESTINATION ---
       if carrier_lc == "omnitrans":
           return consignee_code or pd.NA
       # --- 2️⃣ Hardcoded business rules ---
       if "AVERITT" in consignor:
           return "0004"
       if "COOPETRAJES" in consignor or "COOPETRAJES" in consignee:
           return "0896"
       if consignee_code in SPECIAL_CODES:
           return consignee_code
       # --- 3️⃣ 67N rule ---
       if (
           consignee_code in LOCATION_CODES_67N
           and origin_address.startswith("570 MATH")
           and not any(code in consignor.lower() for code in ['cintas 0897', '0897', '897'])
       ):
           return "067N"
       # --- 4️⃣ 97H rule ---
       if consignee_code in LOCATION_CODES_97H:
           return "097H"
       # --- 5️⃣ Matrix-driven logic ---
       key = (consignor_type, consignee_type)
       key_norm = (_norm(consignor_type), _norm(consignee_type))
       # Normalize mappings for safer lookups
       special_map = {(_norm(k1), _norm(k2)): v for (k1, k2), v in SPECIAL_TYPE_MAPPINGS.items()}
       coding_map = {(_norm(k1), _norm(k2)): v for (k1, k2), v in Coding_Matrix.items()}
       if key_norm in special_map:
           return special_map[key_norm]
       if key_norm in coding_map:
           direction = coding_map[key_norm]
           if direction == "ORIGIN":
               return consignor_code or pd.NA
           elif direction == "DESTINATION":
               return consignee_code or pd.NA
       # --- 6️⃣ Fallbacks for common unmapped patterns ---
       non_cintas_aliases = {"NON-CINTAS", "NON CINTAS", "NONCINTAS", "NON-CIN"}
       if consignor_type.startswith("US") and consignee_type in non_cintas_aliases:
           return consignor_code or pd.NA
       if consignee_type.startswith("US") and consignor_type in non_cintas_aliases:
           return consignee_code or pd.NA
       # --- 7️⃣ Default ---
       return pd.NA

# --- Optional diagnostic to find unmapped pairs ---
def audit_missing_type_pairs(df: pd.DataFrame) -> pd.DataFrame:
   """Show type pairs that produced NA Assigned Location Code."""
   if "Assigned Location Code" not in df.columns:
       return pd.DataFrame()
   mask = df["Assigned Location Code"].isna()
   cols = ["Consignor Type", "Consignee Type"]
   if not set(cols).issubset(df.columns):
       return pd.DataFrame()
   return (
       df.loc[mask, cols]
       .applymap(_norm)
       .value_counts()
       .reset_index(name="count")
       .sort_values("count", ascending=False)
   )
