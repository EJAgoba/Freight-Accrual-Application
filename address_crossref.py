# address_crossref.py
import pandas as pd

class Merger:
   """
   Uses Combined Address to fill Consignor/Consignee Code from Cintas Location Table.
   Pipeline calls:
     merger.merge(accrual_df, cintas_location_table, "Consignor Code")
     merger.merge(accrual_df, cintas_location_table, "Consignee Code")
   """
   @staticmethod
   def _needs_address_fill(code_val, type_val) -> bool:
       """
       Use address-based fill when:
         - code is blank/None
         - AND type is blank OR contains 'THIRD'
       """
       code_str = "" if code_val is None else str(code_val).strip()
       type_str = "" if type_val is None else str(type_val).upper().strip()
       if code_str != "":
           return False
       if type_str == "" or "THIRD" in type_str:
           return True
       return False
   def merge(
       self,
       accrual_df: pd.DataFrame,
       cintas_location_table: pd.DataFrame,
       code_column: str,
   ) -> pd.DataFrame:
       """
       Fills `code_column` using Combined Address lookups.
       - If code_column == "Consignor Code"  -> use 'Consignor Combined Address'
         and 'Consignor Type'
       - If code_column == "Consignee Code"  -> use 'Consignee Combined Address'
         and 'Consignee Type'
       """
       # Validate reference table
       required_ref_cols = {"Combined Address", "Loc Code"}
       if not required_ref_cols.issubset(cintas_location_table.columns):
           raise KeyError(
               "cintas_location_table must contain columns "
               f"{required_ref_cols}. Found: {list(cintas_location_table.columns)}"
           )
       # Decide which combined-address + type column to use
       if code_column == "Consignor Code":
           addr_col = "Consignor Combined Address"
           type_col = "Consignor Type"
       elif code_column == "Consignee Code":
           addr_col = "Consignee Combined Address"
           type_col = "Consignee Type"
       else:
           raise ValueError(
               f"Merger.merge called with unsupported code_column '{code_column}'. "
               "Expected 'Consignor Code' or 'Consignee Code'."
           )
       if addr_col not in accrual_df.columns:
           raise KeyError(
               f"accrual_df must contain '{addr_col}' for address-based merge. "
               f"Found: {list(accrual_df.columns)}"
           )
       # Build mapping: Combined Address -> Loc Code
       addr_to_loc = (
           cintas_location_table[["Combined Address", "Loc Code"]]
           .dropna()
           .drop_duplicates()
           .set_index("Combined Address")["Loc Code"]
           .to_dict()
       )
       def fill_code(row):
           current_code = row.get(code_column)
           current_type = row.get(type_col)
           if not self._needs_address_fill(current_code, current_type):
               return current_code
           addr = row.get(addr_col)
           if pd.isna(addr):
               return current_code
           return addr_to_loc.get(addr, current_code)
       accrual_df[code_column] = accrual_df.apply(fill_code, axis=1)
       return accrual_df
