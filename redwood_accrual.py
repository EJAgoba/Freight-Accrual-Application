# redwood_accrual.py

from __future__ import annotations

import io

from typing import Callable, Iterable

import pandas as pd

import streamlit as st

# ---------------- UI Entrypoint ----------------

def render_redwood_accrual_ui(

    load_reference_tables: Callable[[], tuple[list[str], pd.DataFrame, pd.DataFrame]],

    run_pipeline: Callable[[pd.DataFrame, pd.DataFrame, pd.DataFrame, list[str]], pd.DataFrame],

) -> None:

    """

    Renders a third option outside of 'Accrual' and 'Weekly Audit':

    'Redwood Accrual' — compares A3 vs Redwood by BOL, keeps only Redwood rows

    whose BOLs are NOT present in A3, normalizes a couple of headers, then

    runs your existing accrual coding pipeline to assign location codes.

    Call from app.py after your other UI has rendered, e.g.:

        from redwood_accrual import render_redwood_accrual_ui

        render_redwood_accrual_ui(load_reference_tables, run_pipeline)

    """

    st.markdown("---")

    st.header("Redwood Accrual")

    c1, c2 = st.columns(2)

    with c1:

        a3_file = st.file_uploader("Upload A3 Excel File (.xlsx)", type=["xlsx"], key="redwood_a3")

    with c2:

        redwood_file = st.file_uploader("Upload Redwood Excel File (.xlsx)", type=["xlsx"], key="redwood_rw")

    run_btn = st.button("Run Redwood Accrual", type="primary")

    if not run_btn:

        return

    if a3_file is None or redwood_file is None:

        st.error("Please upload **both** the A3 Excel file and the Redwood Excel file.")

        return

    # ---- Read uploads ----

    try:

        a3_df = pd.read_excel(a3_file, dtype=str)

    except Exception as e:

        st.error(f"Could not read A3 Excel: {e}")

        return

    try:

        rw_df = pd.read_excel(redwood_file, dtype=str)

    except Exception as e:

        st.error(f"Could not read Redwood Excel: {e}")

        return

    st.info(f"A3 rows: **{len(a3_df):,}**, Redwood rows: **{len(rw_df):,}**")

    # ---- Find BOL columns (tolerant) ----

    a3_bol_col = _find_first_col(a3_df, ["BOL Number", "BOL", "BOLNumber", "Pro/BOL", "Pro / BOL"])

    rw_bol_col = _find_first_col(rw_df, ["BOL Number", "BOL", "BOLNumber", "Pro/BOL", "Pro / BOL"])

    if a3_bol_col is None or rw_bol_col is None:

        st.error("Could not find a BOL column in one or both files. Please ensure a 'BOL Number' (or similar) column exists.")

        return

    # ---- Build A3 BOL set (strings, trimmed, non-empty) ----

    a3_bols = (

        a3_df[a3_bol_col]

        .astype(str)

        .str.strip()

        .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})

        .dropna()

        .unique()

        .tolist()

    )

    a3_bol_set = set(a3_bols)

    # ---- Filter Redwood to BOLs not in A3 ----

    rw_df["_BOL_KEY"] = (

        rw_df[rw_bol_col]

        .astype(str)

        .str.strip()

        .fillna("")

    )

    filtered = rw_df[~rw_df["_BOL_KEY"].isin(a3_bol_set)].drop(columns=["_BOL_KEY"], errors="ignore")

    st.success(f"Filtered Redwood rows not in A3 by BOL: **{len(filtered):,}**")

    if filtered.empty:

        st.warning("Nothing to process: all Redwood BOLs already exist in A3.")

        return

    # ---- Rename the two headers per spec ----

    # 1) Origin Address  -> Origin Addresss (triple 's' to match your pipeline)

    # 2) origin state    -> Origin State Code

    #    (case-insensitive; also accept 'Origin State')

    filtered = _rename_case_insensitive(filtered, {

        "Origin Address": "Origin Addresss",

        "origin address": "Origin Addresss",

        "Origin State": "Origin State Code",

        "origin state": "Origin State Code",

    })

    # ---- Ensure minimum columns exist for your pipeline ----

    needed = [

        "Consignor", "Consignee",

        "Consignor Code", "Consignee Code",

        "Dest Address1", "Dest City", "Dest State Code",

        "Origin Addresss", "Origin City", "Origin State Code",

        "Profit Center", "Cost Center", "Account #",

    ]

    for col in needed:

        if col not in filtered.columns:

            filtered[col] = ""

    # ---- Load references and run your existing pipeline ----

    try:

        location_codes, cintas_loc_tbl, complete_loc_tbl = load_reference_tables()

    except Exception as e:

        st.error(f"Reference load error: {e}")

        return

    with st.spinner("Assigning location codes (Matrix / Type mapping)…"):

        try:

            result_df = run_pipeline(filtered.copy(), cintas_loc_tbl, complete_loc_tbl, location_codes)

        except Exception as e:

            st.error(f"Pipeline error: {e}")

            return

    st.success(f"Done! Redwood Accrual processed **{len(result_df):,}** rows.")

    # ---- Downloads (XLSX + CSV) ----

    xls_bytes = _to_xlsx_bytes(result_df, sheet_name="Redwood Accrual")

    st.download_button(

        "⬇️ Download Redwood Accrual (Excel)",

        data=xls_bytes,

        file_name="Redwood Accrual - Not In A3.xlsx",

        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",

    )

    st.download_button(

        "⬇️ Download Redwood Accrual (CSV)",

        data=result_df.to_csv(index=False).encode("utf-8"),

        file_name="Redwood Accrual - Not In A3.csv",

        mime="text/csv",

    )

# ---------------- Helpers ----------------

def _find_first_col(df: pd.DataFrame, candidates: Iterable[str]) -> str | None:

    """Return the first candidate that exists in df.columns (case-sensitive first, then case-insensitive)."""

    for c in candidates:

        if c in df.columns:

            return c

    lower_map = {c.lower(): c for c in df.columns}

    for c in candidates:

        if c.lower() in lower_map:

            return lower_map[c.lower()]

    return None

def _rename_case_insensitive(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:

    """

    Case-insensitive, safe header rename.

    mapping keys are the *desired source names* in any case; values are final names.

    """

    rename_real: dict[str, str] = {}

    lower_map = {c.lower(): c for c in df.columns}

    for src_mixed, target in mapping.items():

        real = lower_map.get(src_mixed.lower())

        if real:

            rename_real[real] = target

    return df.rename(columns=rename_real)

def _to_xlsx_bytes(df: pd.DataFrame, sheet_name: str = "Sheet1") -> bytes:

    bio = io.BytesIO()

    try:

        with pd.ExcelWriter(bio, engine="xlsxwriter") as w:

            df.to_excel(w, index=False, sheet_name=sheet_name)

    except Exception:

        bio = io.BytesIO()

        with pd.ExcelWriter(bio, engine="openpyxl") as w:

            df.to_excel(w, index=False, sheet_name=sheet_name)

    bio.seek(0)

    return bio.read()
 
