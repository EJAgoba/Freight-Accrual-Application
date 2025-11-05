import io

import pandas as pd

import numpy as np

import streamlit as st

# ==========================================

# =========== UNIVERSAL FILE READER =========

# ==========================================

def _read_any(upload) -> pd.DataFrame:

    """Safely read TXT, CSV, or Excel into DataFrame (no open file handles)."""

    name = (upload.name or "").lower()

    data = upload.read()

    upload.seek(0)

    buf = io.BytesIO(data)

    if name.endswith((".xls", ".xlsx", ".xlsm")):

        return pd.read_excel(buf, dtype=str, engine="openpyxl")

    # Detect separator

    import csv

    sample = data[:4096]

    try:

        sniff = csv.Sniffer().sniff(sample.decode("utf-8", errors="ignore"))

        sep = sniff.delimiter

    except Exception:

        sep = "\t" if b"\t" in sample else ","

    return pd.read_csv(io.BytesIO(data), sep=sep, dtype=str, encoding="latin1", engine="python", keep_default_na=False)


# ==========================================

# ============ REDWOOD ACCRUAL UI ===========

# ==========================================

def render_redwood_accrual_ui(load_reference_tables, pipeline_runner):

    """Renders Redwood Accrual section inside Streamlit app."""

    st.header("Redwood Accrual")

    st.caption("Upload A3 and Redwood files to identify shipments in Redwood not found in A3, then apply full accrual logic.")

    # ---------------------- Uploads ----------------------

    a3_file = st.file_uploader("Upload A3 file (TXT / CSV / Excel)", type=["txt", "text", "csv", "xls", "xlsx", "xlsm"], key="rw_a3")

    redwood_file = st.file_uploader("Upload Redwood file (TXT / CSV / Excel)", type=["txt", "text", "csv", "xls", "xlsx", "xlsm"], key="rw_rw")

    if not (a3_file and redwood_file):

        st.info("Please upload both the A3 and Redwood files to continue.")

        st.stop()

    # ---------------------- Load Files Safely ----------------------

    try:

        a3_df = _read_any(a3_file)

        rw_df = _read_any(redwood_file)

    except Exception as e:

        st.error(f"Error reading file(s): {e}")

        st.stop()

    st.info(f"A3 rows: **{len(a3_df):,}**, Redwood rows: **{len(rw_df):,}**")

    # ---------------------- Normalize BOL Columns ----------------------

    a3_bol_col = next((c for c in a3_df.columns if "bol" in c.lower()), None)

    rw_bol_col = next((c for c in rw_df.columns if "bol" in c.lower()), None)

    if not a3_bol_col or not rw_bol_col:

        st.error("Could not find BOL column in one or both files.")

        st.stop()

    # Clean and normalize BOL values

    def normalize_bol(series):

        return series.astype(str).str.strip().str.upper().str.replace(r'[^A-Z0-9]', '', regex=True)

    a3_bols = normalize_bol(a3_df[a3_bol_col])

    rw_df["__BOL__"] = normalize_bol(rw_df[rw_bol_col])

    # ---------------------- Filter: Redwood NOT in A3 ----------------------

    rw_filtered = rw_df.loc[~rw_df["__BOL__"].isin(a3_bols)].copy()

    rw_filtered.drop(columns=["__BOL__"], inplace=True, errors="ignore")

    st.success(f"Filtered Redwood rows not in A3: **{len(rw_filtered):,}**")

    if len(rw_filtered) == 0:

        st.warning("No new Redwood rows found (all BOLs exist in A3).")

        st.stop()

    # ---------------------- Rename Columns for Consistency ----------------------

    rename_map = {

        "Origin Address": "Origin Address1",

        "Origin State": "Origin State Code",

        "Destination Address": "Dest Address1",

        "Destination City": "Dest City",

        "Destination State": "Dest State Code",

        "Origin Facility": "Consignor",

        "Destination Facility": "Consignee"

    }

    rw_filtered.rename(columns={k: v for k, v in rename_map.items() if k in rw_filtered.columns}, inplace=True)

    # ---------------------- Load References ----------------------

    try:

        location_codes, my_location_table, coding_matrix = load_reference_tables()

    except Exception as e:

        st.error(f"Error loading reference tables: {e}")

        st.stop()

    # ---------------------- Run Accrual Logic ----------------------

    with st.spinner("Running location matching and coding logic..."):

        try:

            result_df = pipeline_runner(rw_filtered.copy(), my_location_table.copy(), coding_matrix.copy(), list(location_codes))

        except Exception as e:

            st.error(f"Error running accrual logic: {e}")

            st.stop()

    # ---------------------- GL Build (ProfitCenter.CostCenter.Account) ----------------------

    if not all(c in my_location_table.columns for c in ["Loc Code", "Profit Center", "Cost Center"]):

        st.warning("MY LOCATION TABLE missing one or more of these columns: Loc Code, Profit Center, Cost Center.")

    else:

        gl_lookup = my_location_table[["Loc Code", "Profit Center", "Cost Center"]].drop_duplicates()

        gl_lookup["Account"] = gl_lookup["Cost Center"].apply(lambda x: "EJ" if pd.notna(x) else "")

        gl_lookup["GL Code"] = gl_lookup["Profit Center"].astype(str) + "." + gl_lookup["Cost Center"].astype(str) + "." + gl_lookup["Account"].astype(str)

        result_df = result_df.merge(gl_lookup[["Loc Code", "GL Code"]], how="left", left_on="Assigned Location Code", right_on="Loc Code")

    # ---------------------- Create Pivot Summary ----------------------

    spend_col = next((c for c in result_df.columns if "spend" in c.lower() or "amount" in c.lower()), None)

    if spend_col:

        try:

            result_df[spend_col] = pd.to_numeric(result_df[spend_col], errors="coerce").fillna(0)

            pivot_df = result_df.pivot_table(index="GL Code", values=spend_col, aggfunc="sum").reset_index()

        except Exception:

            st.warning("Could not create pivot table (invalid Spend column).")

            pivot_df = pd.DataFrame()

    else:

        st.warning("No Spend column found — skipping pivot summary.")

        pivot_df = pd.DataFrame()

    st.success(f"Done! Redwood Accrual processed **{len(result_df):,}** rows.")

    # ---------------------- Prepare Downloads ----------------------

    from io import BytesIO

    import tempfile

    excel_buf = BytesIO()

    with pd.ExcelWriter(excel_buf, engine="openpyxl") as writer:

        result_df.to_excel(writer, index=False, sheet_name="Redwood Accrual")

        if not pivot_df.empty:

            pivot_df.to_excel(writer, index=False, sheet_name="Pivot Summary")

    excel_buf.seek(0)

    csv_buf = io.StringIO()

    result_df.to_csv(csv_buf, index=False)

    csv_buf.seek(0)

    st.download_button(

        "⬇️ Download Redwood Accrual (Excel w/ Pivot Summary)",

        data=excel_buf,

        file_name="Redwood_Accrual_Processed.xlsx",

        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

    )

    st.download_button(

        "⬇️ Download Redwood Accrual (CSV)",

        data=csv_buf.getvalue(),

        file_name="Redwood_Accrual_Processed.csv",

        mime="text/csv"

    )
For Sale Page
 
