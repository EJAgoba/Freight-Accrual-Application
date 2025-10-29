import calendar

import datetime as dt

import pandas as pd

import streamlit as st

# === Your existing modules that stay as-is ===

from redwood_accrual import render_redwood_accrual_ui

# === New modules (this refactor) ===

from constants import APP_TITLE, CINTAS_BLUE, APP_HEADER_HTML

from theme import THEME_CSS

from references import ReferenceLoader

from upload_readers import UploadReader

from pipeline import PipelineRunner

from weekly_audit import WeeklyAuditBuilder

from exporters import Exporter

# ========== Page / Theme ==========

st.set_page_config(page_title=APP_TITLE, page_icon="💼", layout="wide")

st.markdown(THEME_CSS, unsafe_allow_html=True)

st.markdown(APP_HEADER_HTML, unsafe_allow_html=True)

# ========== Caches ==========

@st.cache_data(show_spinner=False)

def load_reference_tables():

    return ReferenceLoader().load()

# ========== Redwood (unchanged) ==========

render_redwood_accrual_ui(load_reference_tables, PipelineRunner().run)

# ========== Dynamic UI: Accrual vs Weekly Audit ==========

st.header("Accrual and Weekly Audit Processing")

file_kind = st.radio("Select one:", ["Accrual", "Weekly Audit"], horizontal=True)

today = dt.date.today()

if file_kind == "Accrual":

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

    upload_types = ["txt", "text", "csv", "xlsx"]

st.markdown("### Upload workbook")

file = st.file_uploader("Select a file", type=upload_types)

# ======== Early stop if no file ========

if file is None:

    st.info(f"Upload your {'Accrual .xlsx' if file_kind=='Accrual' else 'Weekly Audit (.txt/.csv/.xlsx)'} file.")

    st.stop()

# ======== Read upload ========

reader = UploadReader()

try:

    input_df = reader.read(file, file_kind)

except Exception as e:

    st.error(f"Could not read the uploaded file: {e}")

    st.stop()

st.info(f"Loaded **{len(input_df):,}** rows. Processing automatically…")

# ======== Load references ========

try:

    location_codes, cintas_loc_tbl, complete_loc_tbl = load_reference_tables()

except Exception as e:

    st.error(f"Reference load error: {e}")

    st.stop()

# ======== Run pipeline ========

runner = PipelineRunner()

with st.spinner("Running accrual re-coding…"):

    try:

        result_df = runner.run(input_df.copy(), cintas_loc_tbl, complete_loc_tbl, location_codes)

    except Exception as e:

        st.error(f"Pipeline error: {e}")

        st.stop()

st.success(f"Done! Processed **{len(result_df):,}** rows.")

# ======== Filenames ========

if file_kind == "Accrual":

    mon_label = f"{calendar.month_abbr[sel_month]}-{sel_year}"

    base_name = f"Accrual {mon_label}"

else:

    mon_label = f"{calendar.month_abbr[today.month]}-{today.year}"

    base_name = f"Weekly Audit Batch {batch_num or 'NA'} {mon_label}-W{week_of_month}"

xlsx_name = f"{base_name}.xlsx"

csv_name  = f"{base_name}.csv"

# ======== Downloads ========

exporter = Exporter()

xls_bytes = exporter.export_full_excel(result_df)

st.download_button(

    "⬇️ Download Excel (all rows)",

    data=xls_bytes,

    file_name=xlsx_name,

    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",

)

st.download_button(

    "⬇️ Download CSV (all rows)",

    data=exporter.export_csv(result_df),

    file_name=csv_name,

    mime="text/csv",

)

# ======== Weekly Audit → Accounting Summary ========

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
 
