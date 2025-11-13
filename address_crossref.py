# address_crossref.py
import pandas as pd

class Merger:
   """
   Cross-reference combined addresses to fill missing/third-party codes.
   """
   @staticmethod
   def _needs_address_fill(code_val, type_val) -> bool:
       """
       Use address for rows where:
         - code is blank/None
         - OR type is blank
         - OR type contains 'THIRD'
       """
       code_str = "" if code_val is None else str(code_val).strip()
       type_str = "" if type_val is None else str(type_val).upper().strip()
       if code_str == "" or type_str == "" or "THIRD" in type_str:
           return True
       return False
   def merge_by_address(self, accrual_df: pd.DataFrame, cintas_loc_df: pd.DataFrame) -> pd.DataFrame:
       """
       Fills Consignor/Consignee Code using Combined Address → Loc Code mapping.
       """
       if not {"Combined Address", "Loc Code"}.issubset(cintas_loc_df.columns):
           raise KeyError(
               "cintas_location_table must contain 'Combined Address' and 'Loc Code'. "
               f"Found: {list(cintas_loc_df.columns)}"
           )
       addr_to_loc = (
           cintas_loc_df[["Combined Address", "Loc Code"]]
           .dropna()
           .drop_duplicates()
           .set_index("Combined Address")["Loc Code"]
           .to_dict()
       )
       def fill_code(row, addr_col, code_col, type_col):
           current_code = row.get(code_col)
           current_type = row.get(type_col)
           if not self._needs_address_fill(current_code, current_type):
               return current_code
           addr = row.get(addr_col)
           if pd.isna(addr):
               return current_code
           return addr_to_loc.get(addr, current_code)
       for addr_col, code_col, type_col in [
           ("Consignor Combined Address", "Consignor Code", "Consignor Type"),
           ("Consignee Combined Address", "Consignee Code", "Consignee Type"),
       ]:
           if addr_col in accrual_df.columns:
               accrual_df[code_col] = accrual_df.apply(
                   lambda r: fill_code(r, addr_col, code_col, type_col),
                   axis=1,
               )
       return accrual_df
