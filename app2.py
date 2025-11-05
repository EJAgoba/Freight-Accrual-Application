# app.py

import calendar

import datetime as dt

import pandas as pd

import streamlit as st

# === Existing core module ===

from redwood_accrual import render_redwood_accrual_ui

# === Modular imports ===

from constants import APP_TITLE

from theme import theme_css

from references import ReferenceLoader

from upload_readers import UploadReader

from pipeline import PipelineRunner

from weekly_audit import WeeklyAuditBuilder

from exporters import Exporter

import io, hashlib
def _file_hash(upload) -> tuple[str, bytes]:
   """Read uploaded file once and return (hash, bytes)."""
   data = upload.read()
   upload.seek(0)
   return hashlib.sha1(data).hexdigest(), data
@st.cache_data(show_spinner=False, ttl=3600, max_entries=10)
def read_any_cached(name: str, data: bytes) -> pd.DataFrame:
   """Cached universal file reader to reduce reprocessing."""
   buf = io.BytesIO(data)
   name = name.lower()
   if name.endswith((".xls", ".xlsx", ".xlsm")):
       return pd.read_excel(buf, dtype=str, engine="openpyxl")
   import csv
   sample = data[:4096]
   try:
       sniff = csv.Sniffer().sniff(sample.decode("utf-8", errors="ignore"))
       sep = sniff.delimiter
   except Exception:
       sep = "\t" if b"\t" in sample else ","
   return pd.read_csv(io.BytesIO(data), sep=sep, dtype=str, engine="python", encoding="latin1", keep_default_na=False)


# ================= Page / Theme (light only) =================

st.set_page_config(page_title=APP_TITLE, page_icon="💼", layout="wide")

st.markdown(theme_css(), unsafe_allow_html=True)

# -------- Single hero header with centered Cintas logo --------

LOGO_SRC = "https://raw.githubusercontent.com/EJAgoba/Freight-Accrual-Application/main/assets/CTAS_BIG.D-709d2754 (1).png"  # put your logo at this path or use a full https:// URL

header_html = f"""
<div class="app-header">
<img class="app-logo" src="{LOGO_SRC}" alt="Cintas Logo">
<div class="app-title">Accrual Re-Coding Tool</div>
<div class="app-subtitle">Upload A3’s Accrual / Weekly Audit workbook. References auto-load from this folder.</div>
</div>

"""

st.markdown(header_html, unsafe_allow_html=True)


# ================= Cached references =================

@st.cache_data(show_spinner=False)

def load_reference_tables():

    """Loads: location_codes, MY LOCATION TABLE, and Complete Coding table."""

    return ReferenceLoader().load()


# ================= Redwood Accrual =================

render_redwood_accrual_ui(load_reference_tables, PipelineRunner().run)


# ================= Main: Accrual vs Weekly Audit =================

st.header("Accrual and Weekly Audit Processing")

file_kind = st.radio("Select one:", ["Accrual", "Weekly Audit"], horizontal=True)

today = dt.date.today()

if file_kind == "Accrual":

    # default to previous month

    prev_month = (today.replace(day=1) - dt.timedelta(days=1))

    years = list(range(today.year - 3, today.year + 2))

    months = list(range(1, 13))

    c1, c2 = st.columns(2)

    with c1:

        sel_year = st.selectbox("Year", years, index=years.index(prev_month.year))

    with c2:

        sel_month = st.selectbox(

            "Month",

            months,

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

    # Weekly Audit allows multiple formats

    upload_types = ["txt", "text", "csv", "xlsx"]

st.markdown("### Upload workbook")

file = st.file_uploader("Select a file", type=upload_types)

if file is None:

    st.info(f"Upload your {'Accrual .xlsx' if file_kind=='Accrual' else 'Weekly Audit (.txt/.csv/.xlsx)'} file.")

    st.stop()

# Read upload

reader = UploadReader()

try:

    input_df = reader.read(file, file_kind)

except Exception as e:

    st.error(f"Could not read the uploaded file: {e}")

    st.stop()

st.info(f"Loaded **{len(input_df):,}** rows. Processing automatically…")

# Load references

try:

    location_codes, cintas_loc_tbl, complete_loc_tbl = load_reference_tables()

except Exception as e:

    st.error(f"Reference load error: {e}")

    st.stop()

# Run pipeline

runner = PipelineRunner()

with st.spinner("Running accrual re-coding…"):

    try:

        result_df = runner.run(input_df.copy(), cintas_loc_tbl, complete_loc_tbl, location_codes)

    except Exception as e:

        st.error(f"Pipeline error: {e}")

        st.stop()

st.success(f"Done! Processed **{len(result_df):,}** rows.")

# Build filenames

if file_kind == "Accrual":

    mon_label = f"{calendar.month_abbr[sel_month]}-{sel_year}"

    base_name = f"Accrual {mon_label}"

else:

    mon_label = f"{calendar.month_abbr[today.month]}-{today.year}"

    base_name = f"Weekly Audit Batch {batch_num or 'NA'} {mon_label}-W{week_of_month}"

xlsx_name = f"{base_name}.xlsx"

csv_name  = f"{base_name}.csv"

# Downloads

exporter = Exporter()

st.download_button(

    "⬇️ Download Excel (all rows)",

    data=exporter.export_full_excel(result_df),

    file_name=xlsx_name,

    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",

)

st.download_button(

    "⬇️ Download CSV (all rows)",

    data=exporter.export_csv(result_df),

    file_name=csv_name,

    mime="text/csv",

)

# ================= Weekly Audit → Accounting Summary =================

if file_kind == "Weekly Audit":

    st.markdown("### Attach edited Weekly Audit file (must contain 'USD'/'USA' and 'CAD' tabs)")

    edited_file = st.file_uploader("Drop your edited Weekly Audit file here", type=["xlsx"], key="edited_wa")

    if edited_file is not None:

        try:

            xls = pd.ExcelFile(edited_file)

            names_lower = {s.lower(): s for s in xls.sheet_names}

            usd_key = names_lower.get("usd") or names_lower.get("usa")

            cad_key = names_lower.get("cad")

            if not (usd_key and cad_key):

                st.error("Workbook must contain both 'USD' (or 'USA') and 'CAD' sheets.")

            else:

                usd_df = pd.read_excel(xls, usd_key)

                cad_df = pd.read_excel(xls, cad_key)

                st.success(f"Edited workbook loaded: USD rows = {len(usd_df):,}, CAD rows = {len(cad_df):,}.")

                builder = WeeklyAuditBuilder()

                selected_run = (batch_num or "").strip() or None

                usd_sheet = builder.build_currency_sheet(usd_df, "USD", selected_run)

                cad_sheet = builder.build_currency_sheet(cad_df, "CAD", selected_run)

                packed = builder.pack_accounting_summary(usd_sheet, cad_sheet)

                st.download_button(

                    "⬇️ Download Accounting Summary (USD & CAD)",

                    data=packed,

                    file_name=f"{base_name} - Accounting Summary (Run {batch_num or 'auto'}).xlsx",

                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",

                    help="Header = negative of Paid/Paid Amount; details = Total Paid Minus Duty and CAD Tax; Account # is text."

                )

        except Exception as e:

            st.error(f"Weekly Audit accounting summary failed: {e}")








