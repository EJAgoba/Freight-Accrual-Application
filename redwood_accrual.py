# redwood_accrual.py
from __future__ import annotations
import io, csv, re
from typing import Callable, Iterable
import pandas as pd
import streamlit as st
# ====================== CONFIG ======================
# Redwood -> pipeline header normalization
HEADER_MAP = {
   "destination facility": "Consignee",
   "origin address":       "Origin Addresss",   # triple 's' per your pipeline
   "origin state":         "Origin State Code",
   "destination address":  "Dest Address1",
   "destination city":     "Dest City",
   "destination state":    "Dest State Code",
   "origin facility":      "Consignor",
}
# Revert pipeline headers back to Redwood names for export
REVERT_MAP = {
   "Consignee":            "Destination Facility",
   "Origin Addresss":      "Origin Address",
   "Origin State Code":    "Origin State",
   "Dest Address1":        "Destination Address",
   "Dest City":            "Destination City",
   "Dest State Code":      "Destination State",
   "Consignor":            "Origin Facility",
}
# Candidate BOL column names (case-sensitive first, then case-insensitive)
BOL_CANDIDATES = ["BOL Number", "BOL", "BOLNumber", "Pro/BOL", "Pro / BOL", "Pro", "Reference", "B/L", "BL"]
# Columns the pipeline expects; we’ll create blanks if missing
REQUIRED_PIPELINE_COLS = [
   "Consignor","Consignee","Consignor Code","Consignee Code",
   "Dest Address1","Dest City","Dest State Code",
   "Origin Addresss","Origin City","Origin State Code",
   "Profit Center","Cost Center","Account #",
]
# Spend preference order for Pivot
SPEND_CANDIDATES = [
   "Total Paid Minus Duty and CAD Tax",
   "Paid Amount","Paid","Amount","Total","Spend","Charge","Charges"
]
# ====================== PUBLIC UI ======================
def render_redwood_accrual_ui(
   load_reference_tables: Callable[[], tuple[list[str], pd.DataFrame, pd.DataFrame]],
   run_pipeline: Callable[[pd.DataFrame, pd.DataFrame, pd.DataFrame, list[str]], pd.DataFrame],
) -> None:
   st.header("Redwood Accrual")
   c1, c2 = st.columns(2)
   with c1:
       a3_file = st.file_uploader("Upload **A3** file (TXT / CSV / Excel)",
                                  type=["txt","text","csv","xls","xlsx","xlsm"], key="rw_a3")
   with c2:
       redwood_file = st.file_uploader("Upload **Redwood** file (TXT / CSV / Excel)",
                                       type=["txt","text","csv","xls","xlsx","xlsm"], key="rw_rw")
   if not st.button("Run Redwood Accrual", type="primary"):
       return
   if a3_file is None or redwood_file is None:
       st.error("Please upload both the A3 file and the Redwood file.")
       return
   # ---------- Read both files safely (no leaking file handles) ----------
   try:
       a3_df = _read_any(a3_file)
       rw_df = _read_any(redwood_file)
   except Exception as e:
       st.error(f"Error reading file(s): {e}")
       return
   st.info(f"A3 rows: **{len(a3_df):,}**, Redwood rows: **{len(rw_df):,}**")
   # ---------- Locate BOL columns ----------
   a3_bol = _find_first_col(a3_df, BOL_CANDIDATES)
   rw_bol = _find_first_col(rw_df, BOL_CANDIDATES)
   if a3_bol is None or rw_bol is None:
       st.error("Could not find a BOL / Pro-BOL column in one or both files.")
       return
   # ---------- STRICT requirement: KEEP ONLY Redwood BOLs NOT IN A3 ----------
   a3_df["__BOL_NORM__"] = _normalize_bol_series(a3_df[a3_bol])
   rw_df["__BOL_NORM__"] = _normalize_bol_series(rw_df[rw_bol])
   a3_bol_set = set(a3_df["__BOL_NORM__"].dropna().tolist())
   filtered = rw_df[~rw_df["__BOL_NORM__"].isin(a3_bol_set)].copy()
   st.success(f"Filtered Redwood rows not in A3 by normalized BOL: **{len(filtered):,}**")
   if filtered.empty:
       st.warning("Nothing to process — all Redwood BOLs appear in A3 (after normalization).")
       return
   # ---------- Normalize headers for your pipeline ----------
   filtered = _normalize_headers(filtered, HEADER_MAP)
   for c in REQUIRED_PIPELINE_COLS:
       if c not in filtered.columns:
           filtered[c] = ""
   # Stabilize identity to guarantee one output per original Redwood row
   filtered = filtered.reset_index(drop=True).copy()
   filtered["__ROW_ID__"] = filtered.index.astype(int)
   # ---------- Load references ----------
   try:
       location_codes, my_location_table, complete_loc_tbl = load_reference_tables()
   except Exception as e:
       st.error(f"Reference load error: {e}")
       return
   # Make reference tables unique on keys commonly used in merges to prevent fan-out
   my_location_table  = _dedupe_by_keys(my_location_table,  ["Loc Code", "Combined Address"])
   complete_loc_tbl   = _dedupe_by_keys(complete_loc_tbl,   ["Loc Code"])
   # ---------- Run your pipeline ----------
   with st.spinner("Running Redwood accrual logic…"):
       try:
           result = run_pipeline(filtered.copy(), my_location_table, complete_loc_tbl, location_codes)
       except Exception as e:
           st.error(f"Pipeline error: {e}")
           return
   # ---------- Collapse any fan-out back to 1 row per Redwood shipment ----------
   before = len(result)
   result = (result.sort_values("__ROW_ID__")
                   .drop_duplicates(subset="__ROW_ID__", keep="first")
                   .drop(columns=["__ROW_ID__"], errors="ignore")
                   .reset_index(drop=True))
   if len(result) != before:
       st.caption(f"Collapsed merge fan-out: {before:,} → {len(result):,} rows (one per Redwood row)")
   # ---------- GL from EJ columns ----------
   for ej in ["Profit Center EJ","Cost Center EJ","Account # EJ"]:
       if ej not in result.columns:
           result[ej] = ""
   result["GL"] = (
       result["Profit Center EJ"].astype(str).str.strip() + "." +
       result["Cost Center EJ"].astype(str).str.strip() + "." +
       result["Account # EJ"].astype(str).str.strip()
   ).str.strip(".")
   # ---------- Pivot: GL + Sum of Spend ----------
   spend_col = next((c for c in SPEND_CANDIDATES if c in result.columns), None)
   if spend_col:
       pivot = _pivot_gl_sum(result, spend_col)
       st.caption(f"Pivot uses spend column: **{spend_col}**")
   else:
       pivot = pd.DataFrame(columns=["GL","Sum of Spend"])
       st.warning("No spend column found for Pivot Summary; sheet will be empty.")
   # ---------- Revert headers for export ----------
   export_df = _revert_headers(result, REVERT_MAP)
   st.success(f"Done! Redwood Accrual processed **{len(export_df):,}** rows.")
   # ---------- Export ----------
   xls_bytes = _to_multi_sheet_xlsx({"Redwood Accrual": export_df, "Pivot Summary": pivot})
   st.download_button("⬇️ Download Redwood Accrual (Excel w/ Pivot Summary)",
                      data=xls_bytes,
                      file_name="Redwood Accrual - Not In A3.xlsx",
                      mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
   st.download_button("⬇️ Download Redwood Accrual (CSV)",
                      data=export_df.to_csv(index=False).encode("utf-8"),
                      file_name="Redwood Accrual - Not In A3.csv",
                      mime="text/csv")
# ====================== HELPERS ======================
def _read_any(upload) -> pd.DataFrame:
   """Read TXT/CSV/Excel safely (copy to memory to avoid open-handle leaks)."""
   name = (upload.name or "").lower()
   file_bytes = upload.read()
   upload.seek(0)
   buf = io.BytesIO(file_bytes)
   if name.endswith((".xls",".xlsx",".xlsm")):
       return pd.read_excel(buf, dtype=str)
   sample = file_bytes[:4096]
   try:
       sniff = csv.Sniffer().sniff(sample.decode("utf-8", errors="ignore"))
       sep = sniff.delimiter
   except Exception:
       sep = "\t" if b"\t" in sample else ","
   return pd.read_csv(io.BytesIO(file_bytes), sep=sep, dtype=str,
                      engine="python", encoding="latin1", keep_default_na=False)
def _find_first_col(df: pd.DataFrame, candidates: Iterable[str]) -> str | None:
   for c in candidates:
       if c in df.columns: return c
   lower = {col.lower(): col for col in df.columns}
   for c in candidates:
       real = lower.get(c.lower())
       if real: return real
   return None
def _normalize_bol_series(s: pd.Series) -> pd.Series:
   """Upper, trim, remove non-alnum, strip leading zeros (keep one '0')."""
   t = s.astype(str).str.upper().str.strip()
   t = t.str.replace(r"[^A-Z0-9]", "", regex=True)
   def _lz(x: str) -> str:
       y = x.lstrip("0")
       return y if y else "0"
   return t.map(_lz)
def _normalize_headers(df: pd.DataFrame, mapping: dict[str,str]) -> pd.DataFrame:
   df = df.copy()
   lower_to_real = {c.lower(): c for c in df.columns}
   for left, right in mapping.items():
       src = lower_to_real.get(left)
       if not src: continue
       if right in df.columns:
           mask = df[right].isna() | (df[right].astype(str).str.strip() == "")
           df.loc[mask, right] = df.loc[mask, src]
       else:
           df[right] = df[src]
   return df
def _revert_headers(df: pd.DataFrame, rev: dict[str,str]) -> pd.DataFrame:
   rename_map = {src: tgt for src, tgt in rev.items() if src in df.columns}
   return df.rename(columns=rename_map)
def _dedupe_by_keys(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
   keys = [k for k in keys if k in df.columns]
   return df.drop_duplicates(subset=keys).copy() if keys else df.copy()
def _pivot_gl_sum(df: pd.DataFrame, spend_col: str) -> pd.DataFrame:
   tmp = df.copy()
   tmp["_Spend_"] = pd.to_numeric(
       tmp[spend_col].astype(str).str.replace("(", "-", regex=False).str.replace(")", "", regex=False),
       errors="coerce"
   ).fillna(0.0)
   out = tmp.groupby("GL", dropna=False)["_Spend_"].sum().reset_index(name="Sum of Spend")
   out["Sum of Spend"] = out["Sum of Spend"].round(2)
   return out.sort_values("GL").reset_index(drop=True)
def _to_multi_sheet_xlsx(sheets: dict[str, pd.DataFrame]) -> bytes:
   bio = io.BytesIO()
   try:
       with pd.ExcelWriter(bio, engine="xlsxwriter") as w:
           for name, frame in sheets.items():
               frame.to_excel(w, index=False, sheet_name=name)
   except Exception:
       bio = io.BytesIO()
       with pd.ExcelWriter(bio, engine="openpyxl") as w:
           for name, frame in sheets.items():
               frame.to_excel(w, index=False, sheet_name=name)
   bio.seek(0)
   return bio.read()
