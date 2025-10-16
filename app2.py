# app.py
import io
import calendar
import datetime as dt
from pathlib import Path
import pandas as pd
import streamlit as st
# ======= Your modules (must be in the same folder as app.py) =======
from extract_codes import Extractor
from address_merge import CombinedAddress
from address_crossref import Merger
from clean_codes import CodeFormatter
from map_types import TypeMapper, TypeCleaner
from coding_matrix import SPECIAL_TYPE_MAPPINGS, Coding_Matrix
from matrix_map import MatrixMapper
# ========= Page / Theme =========
st.set_page_config(
   page_title="Cintas Logistics — Accrual Re-Coding",
   page_icon="💼",
   layout="wide",
)
CINTAS_BLUE = "#003DA5"
CINTAS_RED = "#C8102E"
CINTAS_GRAY = "#F4F6F8"
st.markdown(
   f"""
<style>
       .block-container {{ padding-top: 1rem; padding-bottom: 1.5rem; }}
       .cintas-header {{
           background: linear-gradient(90deg, {CINTAS_BLUE} 0%, #1f5ed6 100%);
           color: white; padding: 16px 20px; border-radius: 12px; margin-bottom: 1rem;
       }}
       .stButton>button {{ background:{CINTAS_BLUE}; color:white; border:none; }}
       .stDownloadButton>button {{ background:{CINTAS_RED}; color:white; border:none; }}
       .cintas-card {{ background:{CINTAS_GRAY}; padding:14px; border-radius:10px; border:1px solid #e3e8ef; }}
</style>
   """,
   unsafe_allow_html=True,
)
st.markdown(
   """
<div class="cintas-header">
<h2 style="margin:0;">Cintas Logistics – Accrual Re-Coding Tool</h2>
<div style="opacity:0.85">Upload A3’s Accrual/Weekly Audit workbook. The app auto-loads Location Codes, MY LOCATION TABLE, and the Complete Coding table from this folder.</div>
</div>
   """,
   unsafe_allow_html=True,
)
# ========= Reference file names (same folder as app.py) =========
CINTAS_LOCATION_TABLE_FILE   = "MY LOCATION TABLE.xlsx"
COMPLETE_LOCATION_TABLE_FILE = "Coding_CintasLocation 02.06.25.xlsx"
# We’ll try these filenames for the Location Codes Excel:
LOCATION_CODES_CANDIDATES = [
   "Location Codes.xlsx",
   "LOCATION_CODES.xlsx",
   "location_codes.xlsx",
   "LocationCodes.xlsx",
   "all_location_codes.xlsx"
]
# ========= Helpers =========
def _read_excel_here(filename: str) -> pd.DataFrame:
   """Read an Excel that sits next to app.py; raise clean error if missing."""
   path = Path(__file__).with_name(filename)
   if not path.exists():
       raise FileNotFoundError(f"Missing required file: '{filename}' in the same folder as app.py.")
   return pd.read_excel(path)
def _try_export_excel(df: pd.DataFrame) -> bytes:
   """Return XLSX bytes; try xlsxwriter then openpyxl."""
   bio = io.BytesIO()
   try:
       with pd.ExcelWriter(bio, engine="xlsxwriter") as w:
           df.to_excel(w, index=False, sheet_name="Re-Coded")
   except Exception:
       bio = io.BytesIO()
       with pd.ExcelWriter(bio, engine="openpyxl") as w:
           df.to_excel(w, index=False, sheet_name="Re-Coded")
   bio.seek(0)
   return bio.read()
@st.cache_data(show_spinner=False)
def load_reference_tables():
   """
   Load:
     - location_codes (list[str]) from a Location Codes Excel in this folder
     - cintas_location_table (DataFrame)  — MY LOCATION TABLE.xlsx
     - complete_location_table (DataFrame) — Coding_CintasLocation 02.06.25.xlsx
   """
   # 1) Location Codes
   codes_path = None
   for fname in LOCATION_CODES_CANDIDATES:
       p = Path(__file__).with_name(fname)
       if p.exists():
           codes_path = p
           break
   if codes_path is None:
       # Let the UI handle missing file with uploader later
       raise FileNotFoundError(
           "Location Codes Excel not found next to app.py. "
           f"Expected one of: {', '.join(LOCATION_CODES_CANDIDATES)}."
       )
   # Try to read codes robustly
   df_codes = pd.read_excel(codes_path)
   possible_cols = ["Codes", "Code", "Loc Code", "Loc_Code", "Location Code"]
   code_col = next((c for c in possible_cols if c in df_codes.columns), None)
   if code_col is None:
       raise ValueError(
           f"'{codes_path.name}' must contain one of these columns: {possible_cols}. "
           f"Found: {list(df_codes.columns)}"
       )
   location_codes = (
       df_codes[code_col]
       .dropna()
       .astype(str)
       .str.strip()
       .str.upper()
       .tolist()
   )
   # 2) Cintas Location Table
   cintas_location_table = _read_excel_here(CINTAS_LOCATION_TABLE_FILE)
   # 3) Complete Coding Table
   complete_location_table = _read_excel_here(COMPLETE_LOCATION_TABLE_FILE)
   return location_codes, cintas_location_table, complete_location_table

def run_pipeline(accrual_df: pd.DataFrame,
                cintas_location_table: pd.DataFrame,
                complete_location_table: pd.DataFrame,
                location_codes: list[str]) -> pd.DataFrame:
   """Your pipeline with the location_codes passed in."""
   # --- Extract Location Codes ---
   extractor = Extractor()
   extractor.create_columns(accrual_df)
   extractor.lower_columns(accrual_df, 'Consignor', 'Consignee')
   extractor.prefill_from_loc_columns(accrual_df, location_codes)
   extractor.extract1(accrual_df, 'Consignor', 'Consignor Code', location_codes, only_null=True)
   extractor.extract1(accrual_df, 'Consignee', 'Consignee Code', location_codes, only_null=True)
   # --- Combined Address ---
   combined_address = CombinedAddress()
   combined_address.create_combined_address_accrual(
       cintas_location_table, 'Combined Address', 'Loc_Address', 'Loc_City', 'Loc_ST'
   )
   combined_address.create_combined_address_accrual(
       accrual_df, 'Consignee Combined Address', 'Dest Address1', 'Dest City', 'Dest State Code'
   )
   combined_address.create_combined_address_accrual(
       accrual_df, 'Consignor Combined Address', 'Origin Addresss', 'Origin City', 'Origin State Code'
   )
   cintas_location_table['Combined Address'] = cintas_location_table['Combined Address'].astype(str).str.upper()
   accrual_df['Consignee Combined Address']  = accrual_df['Consignee Combined Address'].astype(str).str.upper()
   accrual_df['Consignor Combined Address']  = accrual_df['Consignor Combined Address'].astype(str).str.upper()
   # --- Cross reference the combined address ---
   merger = Merger()
   accrual_df = merger.merge(accrual_df, cintas_location_table, 'Consignor Code')
   accrual_df = merger.merge(accrual_df, cintas_location_table, 'Consignee Code')
   # --- Clean up the codes ---
   formatter = CodeFormatter()
   accrual_df = formatter.pad_codes(accrual_df, 'Consignor Code', 'Consignee Code')
   # --- Populate Type Codes ---
   type_mapper = TypeMapper()
   accrual_df = type_mapper.map_types(accrual_df, cintas_location_table, 'Consignor Code', 'Consignor Type')
   accrual_df = type_mapper.map_types(accrual_df, cintas_location_table, 'Consignee Code', 'Consignee Type')
   cleaner = TypeCleaner()
   accrual_df = cleaner.fill_non_cintas(accrual_df, 'Consignor Type', 'Consignee Type')
   # --- Matrix mapping for Assigned Location Code ---
   matrix_mapper = MatrixMapper()
   accrual_df['Assigned Location Code'] = accrual_df.apply(matrix_mapper.determine_profit_center, axis=1)
   # --- Join profit/cost centers from complete location table ---
   accrual_df = accrual_df.merge(
       complete_location_table[['Loc Code', 'Prof_Cntr', 'Cost_Cntr']],
       left_on='Assigned Location Code',
       right_on='Loc Code',
       how='left'
   )
   accrual_df.rename(columns={'Prof_Cntr': 'Profit Center EJ', 'Cost_Cntr': 'Cost Center EJ'}, inplace=True)
   # --- Account # EJ rule ---
   # --- Account # EJ rule ---
   accrual_df['Account # EJ'] = accrual_df.apply(
       lambda row: 621000 if 'G59' in str(row.get('Profit Center EJ', '')) 
       else (621000 if row.get('Consignee Code') == row.get('Assigned Location Code') else 621020),
       axis=1
   )
 
   # --- De-dupe on Invoice Number + Paid Amount (only if both exist) ---
   if {'Invoice Number', 'Paid Amount'}.issubset(accrual_df.columns):
       accrual_df = accrual_df.drop_duplicates(subset=['Invoice Number', 'Paid Amount'])
   # Ensure Profit Center and Profit Center EJ are strings
   accrual_df['Profit Center'] = accrual_df['Profit Center'].astype("string")
   accrual_df['Profit Center EJ'] = accrual_df['Profit Center EJ'].astype("string")
   accrual_df['Automation Accuracy'] = accrual_df.apply(lambda row: (1 if (pd.notna(row['Profit Center']) and pd.notna(row['Profit Center EJ']) and row['Profit Center'] == row['Profit Center EJ']) else 0), axis=1)
   # --- Column ordering (if present) ---
   first_cols = [
       'Profit Center', 'Cost Center', 'Account #','Automation Accuracy',
       'Profit Center EJ', 'Cost Center EJ', 'Account # EJ'
   ]
   ordered = [c for c in first_cols if c in accrual_df.columns] + [c for c in accrual_df.columns if c not in first_cols]
   accrual_df = accrual_df[ordered]
   return accrual_df

# ========= Dynamic UI: Accrual vs Weekly Audit =========

st.markdown("### What file are you processing?")

file_kind = st.radio("Select one:", ["Accrual", "Weekly Audit"], horizontal=True)

today = dt.date.today()

if file_kind == "Accrual":

    # default to previous month

    prev_month = (today.replace(day=1) - dt.timedelta(days=1))

    years  = list(range(today.year - 3, today.year + 2))

    months = list(range(1, 13))

    c1, c2 = st.columns(2)

    with c1:

        sel_year = st.selectbox("Year", years, index=years.index(prev_month.year))

    with c2:

        sel_month = st.selectbox(

            "Month", months,

            format_func=lambda m: calendar.month_abbr[m],

            index=prev_month.month - 1

        )

    upload_types = ["xlsx"]

else:

    c1, c2 = st.columns(2)

    with c1:

        batch_num = st.text_input("Batch Number").strip()

    with c2:

        week_of_month = st.selectbox("Week of Month", [1, 2, 3, 4, 5], index=0)

    # Weekly Audit can come as text or csv too

    upload_types = ["txt", "text", "csv", "xlsx"]

st.markdown("### Upload workbook")

file = st.file_uploader("Select a file", type=upload_types)

# ---------- helper: read uploaded file into DataFrame ----------

import csv

def _read_weekly_text_to_df(ufile) -> pd.DataFrame:

    """

    Convert uploaded .txt/.csv into a DataFrame.

    Auto-detects delimiter, preserves all fields as strings.

    """

    # Peek a sample for delimiter sniffing

    sample = ufile.read(4096)

    ufile.seek(0)

    try:

        sniff = csv.Sniffer().sniff(sample.decode("utf-8", errors="ignore"))

        sep = sniff.delimiter

    except Exception:

        # common fallbacks

        sep = "\t" if b"\t" in sample else ","

    return pd.read_csv(ufile, sep=sep, dtype=str, engine="python", encoding="latin1", keep_default_na=False)

def _read_uploaded(file, kind: str) -> pd.DataFrame:

    name = (file.name or "").lower()

    if kind == "Accrual":

        return pd.read_excel(file)  # your accrual is xlsx

    # Weekly Audit

    if name.endswith((".txt", ".text", ".csv")):

        return _read_weekly_text_to_df(file)

    else:

        return pd.read_excel(file)  # allow xlsx for weekly too

# ======== Missing-reference upload fallback (unchanged except shown names) ========

# (keep your existing reference-upload block here)

# ========= Auto-run once a file is uploaded =========

if file is None:

    st.info(f"Upload your {'Accrual .xlsx' if file_kind=='Accrual' else 'Weekly Audit (.txt/.csv/.xlsx)'} file.")

    st.stop()

# Read upload

try:

    input_df = _read_uploaded(file, file_kind)

except Exception as e:

    st.error(f"Could not read the uploaded file: {e}")

    st.stop()

st.info(f"Loaded **{len(input_df):,}** rows. Processing automatically…")

# Load references & run

try:

    location_codes, cintas_loc_tbl, complete_loc_tbl = load_reference_tables()

except Exception as e:

    st.error(f"Reference load error: {e}")

    st.stop()

with st.spinner("Running accrual re-coding…"):

    try:

        result_df = run_pipeline(input_df.copy(), cintas_loc_tbl, complete_loc_tbl, location_codes)

    except Exception as e:

        st.error(f"Pipeline error: {e}")

        st.stop()

st.success(f"Done! Processed **{len(result_df):,}** rows.")

# ========= Build filenames from choices =========

if file_kind == "Accrual":

    mon_label = f"{calendar.month_abbr[sel_month]}-{sel_year}"

    base_name = f"Accrual {mon_label}"

else:

    mon_label = f"{calendar.month_abbr[today.month]}-{today.year}"

    base_name = f"Weekly Audit Batch {batch_num or 'NA'} {mon_label}-W{week_of_month}"

xlsx_name = f"{base_name}.xlsx"

csv_name  = f"{base_name}.csv"

# ========= Downloads =========

xls_bytes = _try_export_excel(result_df)

st.download_button(

    "⬇️ Download Excel (all rows)",

    data=xls_bytes,

    file_name=xlsx_name,

    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",

)

st.download_button(

    "⬇️ Download CSV (all rows)",

    data=result_df.to_csv(index=False).encode("utf-8"),

    file_name=csv_name,

    mime="text/csv",

)

# ================== Weekly Audit -> Accounting Summary (FINAL COMPLETE) ==================
# ---------- Safe helpers ----------
def _to_series(x):
   if isinstance(x, pd.DataFrame):
       return x.iloc[:, 0] if x.shape[1] else pd.Series([], dtype="object")
   if isinstance(x, pd.Series):
       return x
   return pd.Series(x)
def _text(series):
   s = _to_series(series).astype("object")
   s = s.where(~s.isna(), "")
   return s.astype(str)
def _num(series):
   s = _text(series)
   s = s.str.replace("(", "-", regex=False).str.replace(")", "", regex=False)
   return pd.to_numeric(s, errors="coerce").fillna(0.0).round(2)
def _clean_run_str(series):
   s = _text(series)
   return s.str.strip().str.replace(r"\.0$", "", regex=True)
# ---------- Header normalization (aliases for core + tax/duty) ----------
def _normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
   canon = {}
   for c in df.columns:
       k = c.strip().lower()
       kflat = k.replace(" ", "").replace("_", "")
       # core
       if k == "runnumber":                     canon[c] = "Run Number"
       elif k in ("paid amount", "paid"):       canon[c] = "Amount"
       elif k == "profit center":               canon[c] = "Profit Center"
       elif k == "cost center":                 canon[c] = "Cost Center"
       elif k in ("account #", "account"):      canon[c] = "Account #"
       elif k == "currency":                    canon[c] = "Currency"
       # GST/PST
       elif kflat in ("gstpstpaid","gst/pstpaid".replace("/","")):                    canon[c] = "GST/PST Paid"
       elif kflat in ("gstpstaccount#","gstpstaccount","gstpstacct#","gstpstacct"):    canon[c] = "GST/PST Account #"
       # HST
       elif kflat == "hstpaid":                 canon[c] = "HST Paid"
       elif kflat in ("hstaccount#","hstaccount","hstackt#","hstackt"):                canon[c] = "HST Account #"
       # QST
       elif kflat == "qstpaid":                 canon[c] = "QST Paid"
       elif kflat in ("qstaccount#","qstaccount","qstackt#","qstackt"):                canon[c] = "QST Account #"
       # Duty
       elif kflat == "dutypaid":                canon[c] = "Duty Paid"
       elif kflat in ("dutyaccount#","dutyaccount","dutyacct#","dutyacct"):            canon[c] = "Duty Account #"
   return df.rename(columns=canon)
# ---------- Build one output sheet from one input tab ----------
def _build_currency_sheet(df: pd.DataFrame, force_currency: str, selected_run: str | None) -> pd.DataFrame:
   """
   - Filters to selected_run (Batch Number) if provided
   - Expands GST/PST, HST, QST, Duty into separate rows (same Profit/Cost center)
   - Groups to summary and inserts top balancing row (Account #=240400)
   - Amount rounded to 2 decimals; Account # always text
   """
   df = _normalize_headers(df.copy())
   # Required columns
   req = ["Run Number", "Profit Center", "Cost Center", "Account #", "Amount"]
   missing = [c for c in req if c not in df.columns]
   if missing:
       raise ValueError(f"Edited sheet missing required columns: {missing}")
   # Currency normalize/force
   if "Currency" not in df.columns:
       df["Currency"] = force_currency
   df["Currency"] = _text(df["Currency"]).str.upper().str.strip()
   df.loc[df["Currency"] == "", "Currency"] = force_currency
   # Filter by Run Number (Batch Number)
   df["Run Number"] = _clean_run_str(df["Run Number"])
   run_filter_val = _clean_run_str(pd.Series([selected_run])).iloc[0] if selected_run else None
   if run_filter_val:
       df = df[df["Run Number"] == run_filter_val]
       if df.empty:
           raise ValueError(f"No rows found for Run Number '{selected_run}' in {force_currency} sheet.")
   # Base rows
   base = pd.DataFrame({
       "Run Number":   _clean_run_str(df["Run Number"]),
       "Profit Center":_text(df["Profit Center"]).str.strip(),
       "Cost Center":  _text(df["Cost Center"]).str.strip(),
       "Account #":    _text(df["Account #"]).str.strip(),   # keep as text
       "Currency":     df["Currency"],
       "Amount":       _num(df["Amount"]),
   })
   # Expand GST/HST/QST/Duty rows
   tax_specs = [
       ("GST/PST Paid", "GST/PST Account #", "203063"),
       ("HST Paid",     "HST Account #",     "203064"),
       ("QST Paid",     "QST Account #",     "203065"),
       ("Duty Paid",    "Duty Account #",    None),  # no default for Duty
   ]
   tax_frames = []
   for paid_col, acct_col, default_acct in tax_specs:
       if paid_col in df.columns:
           amt = _num(df[paid_col])
           mask = amt != 0
           if mask.any():
               acct = _text(df[acct_col]) if acct_col in df.columns else pd.Series([""] * len(df), index=df.index)
               acct = acct.str.strip()
               if default_acct is not None:
                   acct = acct.where(acct.replace("", pd.NA).notna(), other=default_acct)
               tax_frames.append(pd.DataFrame({
                   "Run Number":   _clean_run_str(df.loc[mask, "Run Number"]),
                   "Profit Center":_text(df.loc[mask, "Profit Center"]).str.strip(),
                   "Cost Center":  _text(df.loc[mask, "Cost Center"]).str.strip(),
                   "Account #":    _text(acct.loc[mask]).str.strip(),
                   "Currency":     force_currency,
                   "Amount":       amt.loc[mask].round(2),
               }))
   combined = pd.concat([base] + tax_frames, ignore_index=True) if tax_frames else base
   # Group and format
   grouped = (
       combined.groupby(["Profit Center","Cost Center","Account #","Currency"], dropna=False, as_index=False)["Amount"]
               .sum()
   )
   for c in ["Order","Segment","Bus. Area"]:
       grouped[c] = ""
   out_run = run_filter_val or (_clean_run_str(combined["Run Number"]).mode().iloc[0] if not combined.empty else "")
   grouped["Run Number"] = out_run
   # Column order; ensure Account # is text
   grouped["Account #"] = _text(grouped["Account #"]).str.strip()
   grouped = grouped[["Run Number","Profit Center","Cost Center","Order",
                      "Account #","Bus. Area","Segment","Currency","Amount"]]
   grouped["Amount"] = grouped["Amount"].round(2)
   # Top balancing header row
   neg_sum = -float(grouped["Amount"].sum().round(2))
   header = {
       "Run Number": out_run,
       "Profit Center": "686",
       "Cost Center": "",
       "Order": "",
       "Account #": "240400",  # text
       "Bus. Area": "",
       "Segment": "",
       "Currency": force_currency,
       "Amount": round(neg_sum, 2),
   }
   return pd.concat([pd.DataFrame([header]), grouped], ignore_index=True)
# ---------- UI: re-attach edited workbook; filter by Batch Number; export with Account # as TEXT ----------
if file_kind == "Weekly Audit":
   st.markdown("### Attach edited Weekly Audit file (optional)")
   edited_file = st.file_uploader(
       "Drop your manually edited Weekly Audit .xlsx here (tabs: 'USD' or 'USA' and 'CAD').",
       type=["xlsx", "csv", "txt", "text"],
       key="edited_weekly_audit_tabs"
   )
   def _read_edited_any(ufile):
       name = (ufile.name or "").lower()
       if name.endswith(".xlsx"):
           return "xlsx", pd.ExcelFile(ufile)
       if name.endswith((".txt",".text",".csv")):
           return "single", _read_weekly_text_to_df(ufile)
       return "single", pd.read_excel(ufile)
   source_label, usd_df, cad_df = "", None, None
   if edited_file is not None:
       try:
           kind, payload = _read_edited_any(edited_file)
           if kind == "xlsx":
               xls: pd.ExcelFile = payload
               lower = {s.lower(): s for s in xls.sheet_names}
               usd_key = lower.get("usd") or lower.get("usa")
               cad_key = lower.get("cad")
               if usd_key and cad_key:
                   usd_df = pd.read_excel(xls, usd_key)
                   cad_df = pd.read_excel(xls, cad_key)
                   source_label = "Edited (USD/USA & CAD tabs)"
                   st.success(f"Edited workbook loaded: USD rows = {len(usd_df):,}, CAD rows = {len(cad_df):,}.")
               else:
                   st.error("Workbook must contain sheets named 'USD' (or 'USA') and 'CAD'.")
           else:
               one = payload
               usd_df, cad_df = one.copy(), one.copy()
               source_label = "Edited (single sheet)"
               st.warning("No Excel tabs detected; using single sheet for both USD & CAD.")
       except Exception as e:
           st.error(f"Couldn't read the edited file: {e}")
   else:
       if st.button("Or build accounting summary from the current processed rows"):
           usd_df, cad_df = result_df.copy(), result_df.copy()
           source_label = "From Current"
       else:
           st.info("Attach your edited .xlsx or click the button to use current rows.")
   # Batch Number from UI (Run filter)
   selected_run = (batch_num or "").strip() if file_kind == "Weekly Audit" else ""
   if usd_df is not None and cad_df is not None:
       try:
           usd_sheet = _build_currency_sheet(usd_df, "USD", selected_run if selected_run else None)
           cad_sheet = _build_currency_sheet(cad_df, "CAD", selected_run if selected_run else None)
           bio = io.BytesIO()
           with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
               # Write sheets
               usd_sheet.to_excel(writer, index=False, sheet_name="USD")
               cad_sheet.to_excel(writer, index=False, sheet_name="CAD")
               # --- Force Account # columns to TEXT in Excel (no .0) ---
               workbook = writer.book
               text_fmt = workbook.add_format({'num_format': '@'})  # '@' = text
               for sheet_name, df_out in {"USD": usd_sheet, "CAD": cad_sheet}.items():
                   ws = writer.sheets[sheet_name]
                   acct_idx = df_out.columns.get_loc("Account #")  # 0-based index
                   ws.set_column(acct_idx, acct_idx, None, text_fmt)
           bio.seek(0)
           st.download_button(
               "⬇️ Download Accounting Summary (USD & CAD)",
               data=bio.read(),
               file_name=f"{base_name} - Accounting Summary (Run {selected_run or 'auto'}) - {source_label}.xlsx",
               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
               help="Filters to the Batch Number, expands GST/HST/QST/Duty, and preserves Account # as text."
           )
       except Exception as e:
           st.error(f"Weekly Audit accounting summary failed: {e}")
