# app.py
import io
import datetime as dt
import pandas as pd
import streamlit as st


from extract_codes import Extractor
from address_merge import CombinedAddress
from address_crossref import Merger
from clean_codes import CodeFormatter
from map_types import TypeMapper, TypeCleaner
from coding_matrix import SPECIAL_TYPE_MAPPINGS, Coding_Matrix
from matrix_map import MatrixMapper
from location_codes import location_codes

# ========= Cintas UI Theming =========
st.set_page_config(
    page_title="Cintas Accrual Re-Coding",
    page_icon="💼",
    layout="wide",
    initial_sidebar_state="expanded",
)

CINTAS_BLUE = "#003DA5"      # primary
CINTAS_RED = "#C8102E"       # accent
CINTAS_GRAY = "#F4F6F8"

st.markdown(
    f"""
    <style>
        .block-container {{
            padding-top: 1.5rem;
            padding-bottom: 2rem;
        }}
        .cintas-header {{
            background: linear-gradient(90deg, {CINTAS_BLUE} 0%, #1f5ed6 100%);
            color: white; padding: 18px 20px; border-radius: 10px;
            margin-bottom: 1rem;
        }}
        .cintas-badge {{
            display:inline-block; padding: 3px 8px; border-radius: 6px;
            background:{CINTAS_RED}; color:white; font-size:0.78rem; font-weight:600;
        }}
        .stButton>button {{
            background:{CINTAS_BLUE}; color:white; border:none;
        }}
        .stDownloadButton>button {{
            background:{CINTAS_RED}; color:white; border:none;
        }}
        .cintas-card {{
            background:{CINTAS_GRAY}; padding:14px; border-radius:10px;
            border:1px solid #e3e8ef;
        }}
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="cintas-header">
        <h2 style="margin:0;">Cintas Logistics - Accrual Re-Coding Tool</h2>
        <div style="opacity:0.85">Upload A3's Weekly Audit File or the Monthly Accrual File.</div>
    </div>
    """,
    unsafe_allow_html=True,
)

# ========= Paths to bundled reference tables =========
CINTAS_LOCATION_TABLE_FILE = "MY LOCATION TABLE.xlsx"
COMPLETE_LOCATION_TABLE_FILE = "Coding_CintasLocation 02.06.25.xlsx"

# ========= Helpers =========
@st.cache_data(show_spinner=False)
def load_cintas_tables():
    """Load the two reference tables that ship with the app."""
    cintas_location_table = pd.read_excel(CINTAS_LOCATION_TABLE_FILE)
    complete_location_table = pd.read_excel(COMPLETE_LOCATION_TABLE_FILE)
    return cintas_location_table, complete_location_table

def run_pipeline(accrual_df, cintas_location_table, complete_location_table):
    """Your pipeline exactly, with minor robustness checks."""
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

    cintas_location_table['Combined Address'] = cintas_location_table['Combined Address'].str.upper()
    accrual_df['Consignee Combined Address'] = accrual_df['Consignee Combined Address'].str.upper()
    accrual_df['Consignor Combined Address'] = accrual_df['Consignor Combined Address'].str.upper()

    # --- Cross-reference combined address (your Merger logic uses the codes) ---
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

    # --- Column ordering (if present) ---
    first_cols = ['Profit Center', 'Cost Center', 'Account #',
                  'Profit Center EJ', 'Cost Center EJ', 'Account # EJ']
    other_cols = [c for c in accrual_df.columns if c not in first_cols]
    ordered = [c for c in first_cols if c in accrual_df.columns] + other_cols
    accrual_df = accrual_df[ordered]

    # --- De-dupe on Invoice Number + Paid Amount (only if columns exist) ---
    if {'Invoice Number', 'Paid Amount'}.issubset(accrual_df.columns):
        accrual_df = accrual_df.drop_duplicates(subset=['Invoice Number', 'Paid Amount'])

    return accrual_df

# ========= Sidebar =========
with st.sidebar:
    st.markdown("### Settings")
    st.caption("The app ships with the Cintas reference tables and `location_codes`.\nUpload only your weekly Accrual workbook.")
    st.markdown(f"**Cintas Location Table:** `{CINTAS_LOCATION_TABLE_FILE}`")
    st.markdown(f"**Complete Location Table:** `{COMPLETE_LOCATION_TABLE_FILE}`")
    st.markdown('<span class="cintas-badge">Bundled</span>', unsafe_allow_html=True)

# ========= Main Upload & Run =========
import io, datetime as dt, calendar

import pandas as pd

import streamlit as st

st.markdown("#### What file are you processing?")

file_kind = st.radio("Select one:", ["Accrual", "Weekly Audit"], horizontal=True)

today = dt.date.today()

# ---- Defaults / controls for each branch ----

if file_kind == "Accrual":

    # default = previous month

    prev_month = (today.replace(day=1) - dt.timedelta(days=1))

    years  = list(range(today.year - 3, today.year + 2))

    months = list(range(1, 13))

    col1, col2 = st.columns(2)

    with col1:

        sel_year = st.selectbox("Select Accrual Year", years, index=years.index(prev_month.year))

    with col2:

        sel_month = st.selectbox(

            "Select Accrul Month",

            months,

            format_func=lambda m: calendar.month_abbr[m],

            index=prev_month.month - 1

        )

elif file_kind == "Weekly Audit":

    col1, col2 = st.columns(2)

    with col1:

        batch_num = st.text_input("Batch Number").strip()

    with col2:

        week_of_month = st.selectbox("Week of Month", [1, 2, 3, 4, 5], index=0)

st.markdown("#### Upload workbook")

file = st.file_uploader("Choose an Excel file (.xlsx)", type=["xlsx"])

def make_filename():

    if file_kind == "Accrual":

        mon_label = f"{calendar.month_abbr[sel_month]}-{sel_year}"

        return f"Accrual {mon_label}"

    else:

        mon_label = f"{calendar.month_abbr[today.month]}-{today.year}"

        wlabel = f"W{week_of_month}"

        b = batch_num or "NA"

        return f"Weekly Audit Batch {b} {mon_label}-{wlabel}"

if file is not None:

    try:

        accrual_df = pd.read_excel(file)

        st.info(f"Loaded **{len(accrual_df):,}** rows. Processing automatically…")

        with st.spinner("Running accrual re-coding…"):

            cintas_location_table, complete_location_table = load_cintas_tables()

            result_df = run_pipeline(accrual_df.copy(), cintas_location_table, complete_location_table)

        st.success(f"Done! Processed **{len(result_df):,} rows**.")

        st.session_state["result_df"] = result_df.copy()

        # ----- Build filenames from choices -----

        base = make_filename()

        xlsx_name = f"{base}.xlsx"

        csv_name  = f"{base}.csv"

        # ----- Excel download (try xlsxwriter, fallback to openpyxl) -----

        xls_bytes = io.BytesIO()

        try:

            with pd.ExcelWriter(xls_bytes, engine="xlsxwriter") as writer:

                result_df.to_excel(writer, index=False, sheet_name="Re-Coded")

        except Exception:

            with pd.ExcelWriter(xls_bytes, engine="openpyxl") as writer:

                result_df.to_excel(writer, index=False, sheet_name="Re-Coded")

        xls_bytes.seek(0)

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

    except Exception as e:

        st.error(f"Error: {e}")

else:

    st.info("Select options above, then upload your workbook (.xlsx).")

 