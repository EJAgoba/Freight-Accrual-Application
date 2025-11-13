# pipeline.py
import pandas as pd
from extract_codes import Extractor
from address_merge import CombinedAddress
from address_crossref import Merger
from clean_codes import CodeFormatter
from map_types import TypeMapper, TypeCleaner
from matrix_map import MatrixMapper

def run_pipeline(accrual_df: pd.DataFrame,
                cintas_location_table: pd.DataFrame,
                complete_location_table: pd.DataFrame,
                location_codes: list[str]) -> pd.DataFrame:
   # ----------------------------------------------------------
   # 1. INITIAL CODE COLUMNS
   # ----------------------------------------------------------
   extractor = Extractor()
   extractor.create_columns(accrual_df)
   # Prepare for scanning columns
   extractor.lower_columns(accrual_df, "Consignor", "Consignee")
   # ----------------------------------------------------------
   # 2. Extract directly from text FIRST (HIGHEST PRIORITY)
   # ----------------------------------------------------------
   extractor.extract1(
       accrual_df,
       df_column="Consignor",
       new_column="Consignor Code",
       location_codes=location_codes,
       only_null=True
   )
   extractor.extract1(
       accrual_df,
       df_column="Consignee",
       new_column="Consignee Code",
       location_codes=location_codes,
       only_null=True
   )
   # ----------------------------------------------------------
   # 3. Build Combined Address (for fallback only)
   # ----------------------------------------------------------
   combiner = CombinedAddress()
   combiner.create_combined_address_accrual(
       cintas_location_table,
       "Combined Address",
       "Loc_Address", "Loc_City", "Loc_ST"
   )
   combiner.create_combined_address_accrual(
       accrual_df,
       "Consignor Combined Address",
       "Origin Addresss", "Origin City", "Origin State Code"
   )
   combiner.create_combined_address_accrual(
       accrual_df,
       "Consignee Combined Address",
       "Dest Address1", "Dest City", "Dest State Code"
   )
   # Normalize for merging
   cintas_location_table["Combined Address"] = cintas_location_table["Combined Address"].astype(str).str.upper()
   accrual_df["Consignor Combined Address"] = accrual_df["Consignor Combined Address"].astype(str).str.upper()
   accrual_df["Consignee Combined Address"] = accrual_df["Consignee Combined Address"].astype(str).str.upper()
   # ----------------------------------------------------------
   # 4. ADDRESS CROSS-REF (ONLY fill blank codes!)
   # ----------------------------------------------------------
   merger = Merger()
   accrual_df = merger.merge(
       accrual_df, cintas_location_table,
       code_column="Consignor Code",
       only_fill_blank=True          # KEY OPTION: do NOT override extracted codes
   )
   accrual_df = merger.merge(
       accrual_df, cintas_location_table,
       code_column="Consignee Code",
       only_fill_blank=True
   )
   # ----------------------------------------------------------
   # 5. Clean and format codes
   # ----------------------------------------------------------
   formatter = CodeFormatter()
   accrual_df = formatter.pad_codes(
       accrual_df,
       "Consignor Code",
       "Consignee Code"
   )
   # ----------------------------------------------------------
   # 6. Populate TYPE CODES (CINTAS / NON-CINTAS)
   # ----------------------------------------------------------
   type_mapper = TypeMapper()
   accrual_df = type_mapper.map_types(
       accrual_df,
       cintas_location_table,
       "Consignor Code",
       "Consignor Type"
   )
   accrual_df = type_mapper.map_types(
       accrual_df,
       cintas_location_table,
       "Consignee Code",
       "Consignee Type"
   )
   cleaner = TypeCleaner()
   accrual_df = cleaner.fill_non_cintas(
       accrual_df,
       "Consignor Type",
       "Consignee Type"
   )
   # ----------------------------------------------------------
   # 7. MATRIX MAPPING → Assigned Location Code
   # ----------------------------------------------------------
   mapper = MatrixMapper()
   accrual_df["Assigned Location Code"] = accrual_df.apply(
       mapper.determine_profit_center,
       axis=1
   )
   # ----------------------------------------------------------
   # 8. Attach Profit/Cost Center EJ
   # ----------------------------------------------------------
   accrual_df = accrual_df.merge(
       complete_location_table[["Loc Code", "Prof_Cntr", "Cost_Cntr"]],
       left_on="Assigned Location Code",
       right_on="Loc Code",
       how="left"
   )
   accrual_df.rename(
       columns={
           "Prof_Cntr": "Profit Center EJ",
           "Cost_Cntr": "Cost Center EJ"
       },
       inplace=True
   )
   # ----------------------------------------------------------
   # 9. Account # EJ assignment
   # ----------------------------------------------------------
   accrual_df["Account # EJ"] = accrual_df.apply(
       lambda row:
           621000
           if "G59" in str(row.get("Profit Center EJ", ""))
           else (
               621000 if row.get("Consignee Code") == row.get("Assigned Location Code") else 621020
           ),
       axis=1
   )
   # ----------------------------------------------------------
   # 10. Cleanup + Automation Accuracy
   # ----------------------------------------------------------
   if {"Invoice Number", "Paid Amount"}.issubset(accrual_df.columns):
       accrual_df = accrual_df.drop_duplicates(
           subset=["Invoice Number", "Paid Amount"]
       )
   accrual_df["Profit Center"] = accrual_df["Profit Center"].astype("string")
   accrual_df["Profit Center EJ"] = accrual_df["Profit Center EJ"].astype("string")
   accrual_df["Automation Accuracy"] = accrual_df.apply(
       lambda row:
           1 if (
               pd.notna(row["Profit Center"])
               and pd.notna(row["Profit Center EJ"])
               and row["Profit Center"] == row["Profit Center EJ"]
           ) else 0,
       axis=1
   )
   # ----------------------------------------------------------
   # 11. Order columns
   # ----------------------------------------------------------
   first_cols = [
       "Profit Center", "Cost Center", "Account #",
       "Automation Accuracy",
       "Profit Center EJ", "Cost Center EJ", "Account # EJ"
   ]
   ordered_cols = first_cols + [c for c in accrual_df.columns if c not in first_cols]
   return accrual_df[ordered_cols]
