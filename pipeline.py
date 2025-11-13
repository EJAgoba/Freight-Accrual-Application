# pipeline.py
import pandas as pd
from extract_codes import Extractor
from address_merge import CombinedAddress
from address_crossref import Merger
from clean_codes import CodeFormatter
from map_types import TypeMapper, TypeCleaner
from matrix_map import MatrixMapper
def run_pipeline(df, cintas_loc_tbl, complete_loc_tbl, location_codes):
   # ---------- 1. Extract Codes ----------
   ext = Extractor()
   ext.create_columns(df)
   ext.lower_columns(df, "Consignor", "Consignee")
   # Prefill if Org/Dest Code exists
   ext.prefill_from_loc_columns(df, location_codes)
   # Extract from free-text Consignor/Consignee
   ext.extract1(df, "Consignor", "Consignor Code", location_codes, only_null=True)
   ext.extract1(df, "Consignee", "Consignee Code", location_codes, only_null=True)
   # ---------- 2. Build Combined Addresses ----------
   cmb = CombinedAddress()
   cmb.create_combined_address_accrual(
       cintas_loc_tbl, "Combined Address", "Loc_Address", "Loc_City", "Loc_ST"
   )
   cmb.create_combined_address_accrual(
       df, "Consignee Combined Address", "Dest Address1", "Dest City", "Dest State Code"
   )
   cmb.create_combined_address_accrual(
       df, "Consignor Combined Address", "Origin Addresss", "Origin City", "Origin State Code"
   )
   df["Consignor Combined Address"] = df["Consignor Combined Address"].str.upper()
   df["Consignee Combined Address"] = df["Consignee Combined Address"].str.upper()
   cintas_loc_tbl["Combined Address"] = cintas_loc_tbl["Combined Address"].str.upper()
   # ---------- 3. Address Crossref (FILL ONLY IF CODE IS BLANK) ----------
   merger = Merger()
   # Merge for Consignor
   temp = merger.merge(df.copy(), cintas_loc_tbl, "Consignor Code")
   df.loc[df["Consignor Code"].isna() | (df["Consignor Code"] == ""), "Consignor Code"] = \
       temp.loc[df["Consignor Code"].isna() | (df["Consignor Code"] == ""), "Consignor Code"]
   # Merge for Consignee
   temp = merger.merge(df.copy(), cintas_loc_tbl, "Consignee Code")
   df.loc[df["Consignee Code"].isna() | (df["Consignee Code"] == ""), "Consignee Code"] = \
       temp.loc[df["Consignee Code"].isna() | (df["Consignee Code"] == ""), "Consignee Code"]
   # ---------- 4. Clean codes ----------
   fmt = CodeFormatter()
   df = fmt.pad_codes(df, "Consignor Code", "Consignee Code")
   # ---------- 5. Type Mapping ----------
   tmap = TypeMapper()
   df = tmap.map_types(df, cintas_loc_tbl, "Consignor Code", "Consignor Type")
   df = tmap.map_types(df, cintas_loc_tbl, "Consignee Code", "Consignee Type")
   tclean = TypeCleaner()
   df = tclean.fill_non_cintas(df, "Consignor Type", "Consignee Type")
   # ---------- 6. Matrix Mapping ----------
   mm = MatrixMapper()
   df["Assigned Location Code"] = df.apply(mm.determine_profit_center, axis=1)
   # ---------- 7. Merge Profit/Cost Centers ----------
   df = df.merge(
       complete_loc_tbl[["Loc Code", "Prof_Cntr", "Cost_Cntr"]],
       left_on="Assigned Location Code",
       right_on="Loc Code",
       how="left"
   )
   df.rename(columns={
       "Prof_Cntr": "Profit Center EJ",
       "Cost_Cntr": "Cost Center EJ"
   }, inplace=True)
   # ---------- 8. Account EJ ----------
   df["Account # EJ"] = df.apply(
       lambda r: (
           621000 if "G59" in str(r.get("Profit Center EJ", ""))
           else 621000 if r["Consignee Code"] == r["Assigned Location Code"]
           else 621020
       ),
       axis=1
   )
   return df
