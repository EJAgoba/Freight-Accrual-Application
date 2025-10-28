# redwood_accrual.py

from __future__ import annotations

import io

import csv

from typing import Callable, Iterable

import pandas as pd

import streamlit as st

# ====================== CONFIG ======================

HEADER_MAP = {

    "destination facility": "Consignee",

    "origin address":       "Origin Addresss",   # triple 's' per your pipeline

    "origin state":         "Origin State Code",

    "destination address":  "Dest Address1",

    "destination city":     "Dest City",

    "destination state":    "Dest State Code",

    "origin facility":      "Consignor",

}

REVERT_MAP = {

    "Consignee":            "Destination Facility",

    "Origin Addresss":      "Origin Address",

    "Origin State Code":    "Origin State",

    "Dest Address1":        "Destination Address",

    "Dest City":            "Destination City",

    "Dest State Code":      "Destination State",

    "Consignor":            "Origin Facility",

}

BOL_CANDIDATES = ["BOL Number", "BOL", "Pro/BOL", "Pro / BOL", "Pro", "Reference"]

REQUIRED_PIPELINE_COLS = [

    "Consignor", "Consignee",

    "Consignor Code", "Consignee Code",

    "Dest Address1", "Dest City", "Dest State Code",

    "Origin Addresss", "Origin City", "Origin State Code",

    "Profit Center", "Cost Center", "Account #",

]

# ====================== MAIN UI ======================

def render_redwood_accrual_ui(

    load_reference_tables: Callable[[], tuple[list[str], pd.DataFrame, pd.DataFrame]],

    run_pipeline: Callable[[pd.DataFrame, pd.DataFrame, pd.DataFrame, list[str]], pd.DataFrame],

) -> None:

    st.header("Redwood Accrual")

    c1, c2 = st.columns(2)

    with c1:

        a3_file = st.file_uploader(

            "Upload **A3** file (TXT / CSV / Excel)",

            type=["txt", "text", "csv", "xls", "xlsx", "xlsm"],

            key="rw_a3",

        )

    with c2:

        redwood_file = st.file_uploader(

            "Upload **Redwood** file (TXT / CSV / Excel)",

            type=["txt", "text", "csv", "xls", "xlsx", "xlsm"],

            key="rw_rw",

        )

    if not st.button("Run Redwood Accrual", type="primary"):

        return

    if a3_file is None or redwood_file is None:

        st.error("Please upload both the A3 file and the Redwood file.")

        return

    # ---- Read both files ----

    try:

        a3_df = _read_any(a3_file)

        rw_df = _read_any(redwood_file)

    except Exception as e:

        st.error(f"Error reading file(s): {e}")

        return

    st.info(f"A3 rows: **{len(a3_df):,}**, Redwood rows: **{len(rw_df):,}**")

    # ---- Detect BOL columns ----

    a3_bol_col = _find_first_col(a3_df, BOL_CANDIDATES)

    rw_bol_col = _find_first_col(rw_df, BOL_CANDIDATES)

    if a3_bol_col is None or rw_bol_col is None:

        st.error("Could not find a valid BOL / Pro-BOL column in one or both files.")

        return

    # ---- Filter Redwood for BOLs not in A3 ----

    a3_bols = (

        a3_df[a3_bol_col].astype(str).str.strip()

        .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})

        .dropna().unique().tolist()

    )

    rw_df["_BOL_KEY"] = rw_df[rw_bol_col].astype(str).str.strip().fillna("")

    filtered = rw_df[~rw_df["_BOL_KEY"].isin(a3_bols)].drop(columns=["_BOL_KEY"], errors="ignore")

    st.success(f"Filtered Redwood rows not in A3: **{len(filtered):,}**")

    if filtered.empty:

        st.warning("Nothing to process — all Redwood BOLs exist in A3.")

        return

    # ---- Normalize headers for pipeline ----

    filtered = _normalize_headers(filtered, HEADER_MAP)

    for col in REQUIRED_PIPELINE_COLS:

        if col not in filtered.columns:

            filtered[col] = ""

    # ---- Load references & run pipeline ----

    try:

        location_codes, my_location_table, complete_loc_tbl = load_reference_tables()

    except Exception as e:

        st.error(f"Reference load error: {e}")

        return

    with st.spinner("Running Redwood Accrual logic..."):

        try:

            result_df = run_pipeline(filtered.copy(), my_location_table, complete_loc_tbl, location_codes)

        except Exception as e:

            st.error(f"Pipeline error: {e}")

            return

    # ---- Build GL using Account # EJ ----

    result_df["GL"] = (

        result_df["Profit Center EJ"].astype(str).str.strip() + "." +

        result_df["Cost Center EJ"].astype(str).str.strip() + "." +

        result_df["Account # EJ"].astype(str).str.strip()

    ).str.strip(".")

    # ---- Build Pivot Summary (GL + Sum of Spend) ----

    pivot_df = _build_pivot_summary(result_df)

    # ---- Revert headers for export ----

    export_df = _revert_headers(result_df, REVERT_MAP)

    st.success(f"✅ Done! Processed {len(result_df):,} rows with GL and Pivot Summary added.")

    # ---- Export Excel ----

    xls_bytes = _to_multi_sheet_xlsx({

        "Redwood Accrual": export_df,

        "Pivot Summary": pivot_df

    })

    st.download_button(

        "⬇️ Download Redwood Accrual (Excel w/ Pivot Summary)",

        data=xls_bytes,

        file_name="Redwood Accrual - Not In A3.xlsx",

        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",

    )

    # Optional CSV

    st.download_button(

        "⬇️ Download Redwood Accrual (CSV)",

        data=export_df.to_csv(index=False).encode("utf-8"),

        file_name="Redwood Accrual - Not In A3.csv",

        mime="text/csv",

    )

# ====================== HELPERS ======================

def _read_any(upload) -> pd.DataFrame:

    """Read TXT, CSV, or Excel automatically."""

    name = (upload.name or "").lower()

    if name.endswith((".xls", ".xlsx", ".xlsm")):

        return pd.read_excel(upload, dtype=str)

    head = upload.read(4096)

    upload.seek(0)

    try:

        sniff = csv.Sniffer().sniff(head.decode("utf-8", errors="ignore"))

        sep = sniff.delimiter

    except Exception:

        sep = "\t" if b"\t" in head else ","

    df = pd.read_csv(upload, sep=sep, dtype=str, engine="python", encoding="latin1", keep_default_na=False)

    upload.seek(0)

    return df

def _find_first_col(df: pd.DataFrame, candidates: Iterable[str]) -> str | None:

    for c in candidates:

        if c in df.columns:

            return c

    lower = {col.lower(): col for col in df.columns}

    for c in candidates:

        real = lower.get(c.lower())

        if real:

            return real

    return None

def _normalize_headers(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:

    df = df.copy()

    lower_to_real = {c.lower(): c for c in df.columns}

    for left, right in mapping.items():

        src = lower_to_real.get(left)

        if not src:

            continue

        if right in df.columns:

            mask = df[right].isna() | (df[right].astype(str).str.strip() == "")

            df.loc[mask, right] = df.loc[mask, src]

        else:

            df[right] = df[src]

    return df

def _revert_headers(df: pd.DataFrame, reverse_map: dict[str, str]) -> pd.DataFrame:

    rename_map = {src: tgt for src, tgt in reverse_map.items() if src in df.columns}

    return df.rename(columns=rename_map)

def _build_pivot_summary(df: pd.DataFrame) -> pd.DataFrame:

    """Pivot summary: GL + Sum of Spend."""

    if df.empty:

        return pd.DataFrame(columns=["GL", "Sum of Spend"])

    if "GL" not in df.columns:

        return pd.DataFrame(columns=["GL", "Sum of Spend"])

    # Pick best spend-like column

    spend_candidates = ["Paid", "Paid Amount", "Amount", "Total", "Spend", "Charge", "Total Paid Minus Duty and CAD Tax"]

    spend_col = next((c for c in spend_candidates if c in df.columns), None)

    if not spend_col:

        return pd.DataFrame(columns=["GL", "Sum of Spend"])

    tmp = df.copy()

    tmp["_Spend_"] = pd.to_numeric(

        tmp[spend_col].astype(str).str.replace("(", "-", regex=False).str.replace(")", "", regex=False),

        errors="coerce"

    ).fillna(0)

    summary = (

        tmp.groupby("GL", dropna=False)["_Spend_"]

        .sum()

        .reset_index(name="Sum of Spend")

    )

    summary["Sum of Spend"] = summary["Sum of Spend"].round(2)

    return summary.sort_values("GL").reset_index(drop=True)

def _to_multi_sheet_xlsx(sheets: dict[str, pd.DataFrame]) -> bytes:

    bio = io.BytesIO()

    try:

        with pd.ExcelWriter(bio, engine="xlsxwriter") as w:

            for name, df in sheets.items():

                df.to_excel(w, index=False, sheet_name=name)

    except Exception:

        bio = io.BytesIO()

        with pd.ExcelWriter(bio, engine="openpyxl") as w:

            for name, df in sheets.items():

                df.to_excel(w, index=False, sheet_name=name)

    bio.seek(0)

    return bio.read()
