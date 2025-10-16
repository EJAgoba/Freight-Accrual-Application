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



# ================== Weekly Audit → Accounting Summary (Direct Columns, No Normalization) ==================
import re
def _text(series):
   return series.astype(str).replace("nan", "")
def _num(series):
   s = series.astype(str)
   s = s.str.replace("(", "-", regex=False).str.replace(")", "", regex=False)
   return pd.to_numeric(s, errors="coerce").fillna(0.0).round(2)
def _clean_acct(series):
   """Force account number to text and strip any .0 or decimals"""
   s = series.astype(str).str.strip()
   s = s.str.replace(r"\.0$", "", regex=True)
   s = s.str.replace(r"\..*$", "", regex=True)
   return s
def _build_currency_sheet(df: pd.DataFrame, force_currency: str, selected_run: str | None) -> pd.DataFrame:

    # Verify required columns exist

    required = [

        "RunNumber", "Profit Center", "Cost Center",

        "Account #", "Currency", "Total Paid Minus Duty and CAD Tax"

    ]

    for c in required:

        if c not in df.columns:

            raise ValueError(f"Edited sheet missing required column: '{c}'")

    # Detect Paid column (either 'Paid' or 'Paid Amount')

    paid_col = None

    for candidate in ["Paid", "Paid Amount"]:

        if candidate in df.columns:

            paid_col = candidate

            break

    if not paid_col:

        raise ValueError("Missing 'Paid' or 'Paid Amount' column — one of them must exist.")

    # Filter by RunNumber if provided

    if selected_run:

        df = df[df["RunNumber"].astype(str).str.strip() == str(selected_run).strip()]

        if df.empty:

            raise ValueError(f"No rows found for RunNumber {selected_run}")

    # Base detail rows

    base = pd.DataFrame({

        "Run Number": df["RunNumber"].astype(str).str.strip(),

        "Profit Center": df["Profit Center"].astype(str).str.strip(),

        "Cost Center": df["Cost Center"].astype(str).str.strip(),

        "Account #": _clean_acct(df["Account #"]),

        "Currency": df["Currency"].astype(str).str.upper().str.strip(),

        "Amount": _num(df["Total Paid Minus Duty and CAD Tax"]),

    })

    # Header negative total (Paid/Paid Amount column)

    header_amount = round(-_num(df[paid_col]).sum(), 2)
    header = {
       "Run Number": str(selected_run or df["RunNumber"].iloc[0]),
       "Profit Center": "686",
       "Cost Center": "",
       "Order": "",
       "Account #": "240400",
       "Bus. Area": "",
       "Segment": "",
       "Currency": force_currency,
       "Amount": header_amount,
   }
   # Tax/Duty expansion
   tax_specs = [
       ("GST/PST Paid", "GST/PST Account #", "203063"),
       ("HST Paid", "HST Account #", "203064"),
       ("QST Paid", "QST Account #", "203065"),
       ("Duty Paid", "Duty Account #", None),
   ]
   tax_frames = []
   for paid_col, acct_col, default_acct in tax_specs:
       if paid_col in df.columns:
           amt = _num(df[paid_col])
           mask = amt != 0
           if mask.any():
               acct = _clean_acct(df[acct_col]) if acct_col in df.columns else ""
               if default_acct:
                   acct = acct.where(acct.replace("", pd.NA).notna(), other=default_acct)
               tax_frames.append(pd.DataFrame({
                   "Run Number": df.loc[mask, "RunNumber"].astype(str).str.strip(),
                   "Profit Center": df.loc[mask, "Profit Center"].astype(str).str.strip(),
                   "Cost Center": df.loc[mask, "Cost Center"].astype(str).str.strip(),
                   "Account #": _clean_acct(acct.loc[mask] if isinstance(acct, pd.Series) else acct),
                   "Currency": force_currency,
                   "Amount": amt.loc[mask].round(2),
               }))
   combined = pd.concat([base] + tax_frames, ignore_index=True) if tax_frames else base
   # Group and finalize
   grouped = (
       combined.groupby(["Profit Center","Cost Center","Account #","Currency"], dropna=False, as_index=False)["Amount"]
       .sum()
   )
   for col in ["Order","Bus. Area","Segment"]:
       grouped[col] = ""
   grouped["Run Number"] = str(selected_run or df["RunNumber"].iloc[0])
   grouped["Account #"] = _clean_acct(grouped["Account #"])
   grouped["Amount"] = grouped["Amount"].round(2)
   # Add header row
   out_df = pd.concat([pd.DataFrame([header]), grouped], ignore_index=True)
   out_df = out_df[["Run Number","Profit Center","Cost Center","Order","Account #","Bus. Area","Segment","Currency","Amount"]]
   return out_df
# ---------- Run and export ----------
if file_kind == "Weekly Audit":
   st.markdown("### Attach edited Weekly Audit file (must contain 'USD'/'USA' and 'CAD' tabs)")
   edited_file = st.file_uploader("Drop your edited Weekly Audit file here", type=["xlsx"])
   if edited_file is not None:
       xls = pd.ExcelFile(edited_file)
       lower = {s.lower(): s for s in xls.sheet_names}
       usd_key = lower.get("usd") or lower.get("usa")
       cad_key = lower.get("cad")
       if not (usd_key and cad_key):
           st.error("Workbook must contain both 'USD' (or 'USA') and 'CAD' sheets.")
       else:
           usd_df = pd.read_excel(xls, usd_key)
           cad_df = pd.read_excel(xls, cad_key)
           st.success(f"Loaded: USD rows = {len(usd_df):,}, CAD rows = {len(cad_df):,}")
           try:
               usd_sheet = _build_currency_sheet(usd_df, "USD", batch_num)
               cad_sheet = _build_currency_sheet(cad_df, "CAD", batch_num)
               bio = io.BytesIO()
               with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
                   usd_sheet.to_excel(writer, index=False, sheet_name="USD")
                   cad_sheet.to_excel(writer, index=False, sheet_name="CAD")
                   workbook = writer.book
                   text_fmt = workbook.add_format({'num_format': '@'})
                   for name, df_out in {"USD": usd_sheet, "CAD": cad_sheet}.items():
                       ws = writer.sheets[name]
                       acct_idx = df_out.columns.get_loc("Account #")
                       ws.set_column(acct_idx, acct_idx, None, text_fmt)
                       for r, val in enumerate(df_out["Account #"].astype(str).tolist(), start=1):
                           ws.write_string(r, acct_idx, val)
               bio.seek(0)
               st.download_button(
                   "⬇️ Download Accounting Summary (USD & CAD)",
                   data=bio.read(),
                   file_name=f"{base_name} - Accounting Summary (Run {batch_num or 'auto'}) - Direct.xlsx",
                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
               )
           except Exception as e:
               st.error(f"Weekly Audit accounting summary failed: {e}")

