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
   accrual_df['Account # EJ'] = accrual_df.apply(
       lambda row: 621000 if row.get('Consignee Code') == row.get('Assigned Location Code') else 621020,
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



# ================== Weekly Audit -> Accounting Summary (with edited-file reattach) ==================

def weekly_audit_to_accounting_sheets(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Your 7-step month-end rules.
    Renames 'RunNumber' -> 'Run Number' and 'Paid Amount' -> 'Amount'.
    """
    # Normalize the two key headers + common ones
    rename_map = {}
    for col in df.columns:
        key = col.strip().lower()
        if key == "runnumber":
            rename_map[col] = "Run Number"
        elif key == "paid amount":
            rename_map[col] = "Amount"
        elif key == "profit center":
            rename_map[col] = "Profit Center"
        elif key == "cost center":
            rename_map[col] = "Cost Center"
        elif key in ("account #", "account"):
            rename_map[col] = "Account #"
        elif key == "currency":
            rename_map[col] = "Currency"
    df = df.rename(columns=rename_map)

    needed = ["Run Number", "Profit Center", "Cost Center", "Account #", "Currency", "Amount"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(f"Weekly Audit summary requires columns: {needed}. Missing: {missing}")

    work = df[needed].copy()
    work["Profit Center"] = work["Profit Center"].astype(str).str.strip()
    work["Cost Center"]   = work["Cost Center"].astype(str).str.strip()
    work["Account #"]     = work["Account #"].astype(str).str.strip()
    work["Currency"]      = work["Currency"].astype(str).str.upper().str.strip()
    work["Amount"] = pd.to_numeric(
        work["Amount"].astype(str).str.replace("(", "-", regex=False).str.replace(")", "", regex=False),
        errors="coerce"
    ).fillna(0.0)

    grouped = (
        work.groupby(["Profit Center", "Cost Center", "Account #", "Currency"], dropna=False, as_index=False)["Amount"]
        .sum()
    )
    for c in ["Order", "Segment", "Bus. Area"]:
        grouped[c] = ""

    run_number_value = work["Run Number"].mode().iloc[0] if not work["Run Number"].mode().empty else ""
    grouped["Run Number"] = run_number_value

    final_cols = ["Run Number", "Profit Center", "Cost Center", "Order",
                  "Account #", "Bus. Area", "Segment", "Currency", "Amount"]
    grouped = grouped[final_cols]

    def build_currency_sheet(cur: str) -> pd.DataFrame:
        g = grouped[grouped["Currency"] == cur.upper()].copy()
        neg_sum = -float(g["Amount"].sum())
        top_row = {
            "Run Number": run_number_value,
            "Profit Center": "686",
            "Cost Center": "240400",
            "Order": "",
            "Account #": "",
            "Bus. Area": "",
            "Segment": "",
            "Currency": cur.upper(),
            "Amount": neg_sum,
        }
        return pd.concat([pd.DataFrame([top_row]), g], ignore_index=True)

    return {"USD": build_currency_sheet("USD"), "CAD": build_currency_sheet("CAD")}

# --- UI to reattach edited Weekly Audit file ---
if file_kind == "Weekly Audit":
    st.markdown("### Attach edited Weekly Audit file (optional)")
    edited_file = st.file_uploader(
        "Drop your manually edited Weekly Audit file here (.xlsx, .csv, .txt)",
        type=["xlsx", "csv", "txt", "text"],
        key="edited_weekly_audit"
    )

    # Helper to read the edited file using your existing logic
    def _read_edited(ufile):
        name = (ufile.name or "").lower()
        if name.endswith((".txt", ".text", ".csv")):
            return _read_weekly_text_to_df(ufile)
        return pd.read_excel(ufile)

    # If edited file provided, build from it; else allow building from current processed rows
    source_df = None
    source_label = ""
    if edited_file is not None:
        try:
            source_df = _read_edited(edited_file)
            source_label = "Edited"
            st.success(f"Edited file loaded: {len(source_df):,} rows.")
        except Exception as e:
            st.error(f"Couldn't read the edited file: {e}")

    else:
        # Optional button to generate from the in-app processed data
        if st.button("Or build accounting summary from the current processed rows"):
            source_df = result_df.copy()
            source_label = "From Current"
        else:
            st.info("Attach your edited file, or click the button above to use the current processed rows.")

    if source_df is not None:
        try:
            sheets = weekly_audit_to_accounting_sheets(source_df)
            bio_acc = io.BytesIO()
            with pd.ExcelWriter(bio_acc, engine="xlsxwriter") as writer:
                for name, sheet in sheets.items():
                    sheet.to_excel(writer, index=False, sheet_name=name)
            bio_acc.seek(0)
            accounting_bytes = bio_acc.read()

            acct_name = f"{base_name} - Accounting Summary ({source_label}).xlsx"
            st.download_button(
                "⬇️ Download Accounting Summary (USD & CAD)",
                data=accounting_bytes,
                file_name=acct_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                help="Builds two tabs (USD & CAD) from your edited Weekly Audit file, following the month-end rules."
            )
        except Exception as e:
            st.error(f"Weekly Audit accounting summary failed: {e}")
