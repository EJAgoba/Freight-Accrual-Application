# redwood_accrual.py

from __future__ import annotations

import io

import csv

from typing import Callable, Iterable

import pandas as pd

import streamlit as st

# ================================== Config ==================================

# Left -> Right mapping used to normalize for the pipeline (case-insensitive on LEFT)

HEADER_MAP = {

    "destination facility": "Consignee",

    "origin address":       "Origin Addresss",   # triple 's' per your pipeline

    "origin state":         "Origin State Code",

    "destination address":  "Dest Address1",

    "destination city":     "Dest City",

    "destination state":    "Dest State Code",

    "origin facility":      "Consignor",

}

# Right -> Left mapping to REVERT headers back for the final export

REVERT_MAP = {

    "Consignee":            "Destination Facility",

    "Origin Addresss":      "Origin Address",

    "Origin State Code":    "Origin State",

    "Dest Address1":        "Destination Address",

    "Dest City":            "Destination City",

    "Dest State Code":      "Destination State",

    "Consignor":            "Origin Facility",

}

# Candidate BOL headers (case-insensitive search)

BOL_CANDIDATES = ["BOL Number", "BOL", "BOLNumber", "Pro/BOL", "Pro / BOL", "Pro", "Reference"]

# Minimum columns your pipeline expects (we'll create blanks if missing)

REQUIRED_PIPELINE_COLS = [

    "Consignor", "Consignee",

    "Consignor Code", "Consignee Code",

    "Dest Address1", "Dest City", "Dest State Code",

    "Origin Addresss", "Origin City", "Origin State Code",

    "Profit Center", "Cost Center", "Account #",

]

# Possible column names in MY LOCATION TABLE used to build the GL string

MY_LOC_LOC_CODE_CANDS = ["Loc Code", "Location Code", "Loc_Code"]

MY_LOC_PROFIT_CANDS   = ["Prof_Cntr", "Profit Center", "Profit Cntr", "Profit_Center"]

MY_LOC_COST_CANDS     = ["Cost_Cntr", "Cost Center", "Cost Cntr", "Cost_Center"]

MY_LOC_ACCT_CANDS     = ["Account #", "Account", "Account#", "GL Account", "Acct", "GL"]

# =============================== Public UI ==================================

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

    # Optional sheet selectors (appear only when an Excel file is uploaded)

    a3_sheet = _sheet_picker(a3_file, "A3") if _looks_like_excel(a3_file) else None

    rw_sheet = _sheet_picker(redwood_file, "Redwood") if _looks_like_excel(redwood_file) else None

    run_btn = st.button("Run Redwood Accrual", type="primary")

    if not run_btn:

        return

    if a3_file is None or redwood_file is None:

        st.error("Please upload **both** files (A3 and Redwood).")

        return

    # ---- Read uploads (any format) ----

    try:

        a3_df = _read_any(a3_file, sheet_name=a3_sheet)

    except Exception as e:

        st.error(f"Could not read A3 file: {e}")

        return

    try:

        rw_df = _read_any(redwood_file, sheet_name=rw_sheet)

    except Exception as e:

        st.error(f"Could not read Redwood file: {e}")

        return

    st.info(f"A3 rows: **{len(a3_df):,}**, Redwood rows: **{len(rw_df):,}**")

    # ---- Find BOL columns ----

    a3_bol_col = _find_first_col(a3_df, BOL_CANDIDATES)

    rw_bol_col = _find_first_col(rw_df, BOL_CANDIDATES)

    if a3_bol_col is None or rw_bol_col is None:

        st.error("Could not find a BOL/pro reference column in one or both files.")

        return

    # ---- A3 BOL set ----

    a3_bol_set = set(

        a3_df[a3_bol_col]

        .astype(str).str.strip()

        .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})

        .dropna()

        .tolist()

    )

    # ---- Keep Redwood rows whose BOL is NOT in A3 ----

    rw_df["_BOL_KEY"] = rw_df[rw_bol_col].astype(str).str.strip().fillna("")

    filtered = rw_df[~rw_df["_BOL_KEY"].isin(a3_bol_set)].drop(columns=["_BOL_KEY"], errors="ignore")

    st.success(f"Filtered Redwood rows not in A3 by BOL: **{len(filtered):,}**")

    if filtered.empty:

        st.warning("Nothing to process: all Redwood BOLs already exist in A3.")

        return

    # ---- Normalize headers per mapping (to run your pipeline) ----

    filtered = _normalize_headers(filtered, HEADER_MAP, drop_sources=False)

    # ---- Ensure required pipeline columns exist ----

    for col in REQUIRED_PIPELINE_COLS:

        if col not in filtered.columns:

            filtered[col] = ""

    # ---- Load references & run your pipeline ----

    try:

        # NOTE: Here, table[1] is MY LOCATION TABLE from app.py

        location_codes, my_location_table, complete_loc_tbl = load_reference_tables()

    except Exception as e:

        st.error(f"Reference load error: {e}")

        return

    with st.spinner("Assigning location codes (matrix/type mapping)…"):

        try:

            result_df = run_pipeline(filtered.copy(), my_location_table, complete_loc_tbl, location_codes)

        except Exception as e:

            st.error(f"Pipeline error: {e}")

            return

    # ---- Build GL using MY LOCATION TABLE (by Assigned Location Code) ----

    try:

        result_df = _attach_gl_from_my_location(result_df, my_location_table)

    except Exception as e:

        st.warning(f"GL build warning: {e}")

    # ---- Build Pivot Summary ----

    pivot_df = _build_pivot_summary(result_df)

    # ---- Revert the normalized headers back to original names for export ----

    export_df = result_df.copy()

    export_df = _revert_headers(export_df, REVERT_MAP)

    st.success(f"Done! Redwood Accrual processed **{len(result_df):,}** rows.")

    st.caption("Export includes reverted headers and a Pivot Summary sheet.")

    # ---- Single Excel with two sheets (Redwood Accrual + Pivot Summary) ----

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

    # Also offer CSV for the main sheet

    st.download_button(

        "⬇️ Download Redwood Accrual (CSV)",

        data=export_df.to_csv(index=False).encode("utf-8"),

        file_name="Redwood Accrual - Not In A3.csv",

        mime="text/csv",

    )

# ============================== File Reading =================================

def _looks_like_excel(upload) -> bool:

    if upload is None:

        return False

    name = (upload.name or "").lower()

    return name.endswith((".xls", ".xlsx", ".xlsm"))

def _sheet_picker(upload, label_prefix: str) -> str | None:

    """If Excel with multiple sheets, offer a selectbox to pick a sheet."""

    if not _looks_like_excel(upload):

        return None

    try:

        xls = pd.ExcelFile(upload)

        sheets = xls.sheet_names

        if not sheets:

            return None

        default_idx = 0

        return st.selectbox(f"{label_prefix} sheet", sheets, index=default_idx, key=f"{label_prefix}_sheet")

    except Exception:

        return None

def _read_any(upload, sheet_name: str | None = None) -> pd.DataFrame:

    """

    Read TXT/CSV (delimiter-sniffed) or Excel (optionally selected sheet).

    Returns all columns as strings to avoid type issues.

    """

    name = (upload.name or "").lower()

    if _looks_like_excel(upload):

        try:

            upload.seek(0)

        except Exception:

            pass

        try:

            if sheet_name:

                return pd.read_excel(upload, sheet_name=sheet_name, dtype=str)

            xls = pd.ExcelFile(upload)

            first = xls.sheet_names[0]

            return pd.read_excel(xls, sheet_name=first, dtype=str)

        finally:

            try:

                upload.seek(0)

            except Exception:

                pass

    # TXT/CSV path: sniff delimiter

    try:

        head = upload.read(4096)

        upload.seek(0)

        try:

            sniff = csv.Sniffer().sniff(head.decode("utf-8", errors="ignore"))

            sep = sniff.delimiter

        except Exception:

            sep = "\t" if b"\t" in head else ","

        df = pd.read_csv(upload, sep=sep, dtype=str, engine="python", encoding="latin1", keep_default_na=False)

        return df

    finally:

        try:

            upload.seek(0)

        except Exception:

            pass

# ============================== GL + Pivot ===================================

def _attach_gl_from_my_location(df: pd.DataFrame, my_loc: pd.DataFrame) -> pd.DataFrame:

    """

    Attach GL using MY LOCATION TABLE and 'Assigned Location Code':

      GL = "<profit>.<cost>.<account>"

    """

    # Find columns in MY LOCATION TABLE

    loc_code_col = _find_first_col(my_loc, MY_LOC_LOC_CODE_CANDS)

    profit_col   = _find_first_col(my_loc, MY_LOC_PROFIT_CANDS)

    cost_col     = _find_first_col(my_loc, MY_LOC_COST_CANDS)

    acct_col     = _find_first_col(my_loc, MY_LOC_ACCT_CANDS)

    if loc_code_col is None:

        raise ValueError("MY LOCATION TABLE missing 'Loc Code'–like column.")

    if profit_col is None:

        raise ValueError("MY LOCATION TABLE missing Profit Center column.")

    if cost_col is None:

        raise ValueError("MY LOCATION TABLE missing Cost Center column.")

    if acct_col is None:

        raise ValueError("MY LOCATION TABLE missing Account column.")

    # Trim/uppercase keys for merge tolerance

    left = df.copy()

    right = my_loc[[loc_code_col, profit_col, cost_col, acct_col]].copy()

    left_key = "Assigned Location Code"

    if left_key not in left.columns:

        raise ValueError("Result is missing 'Assigned Location Code'.")

    left["_KEY_"] = left[left_key].astype(str).str.strip().str.upper()

    right["_KEY_"] = right[loc_code_col].astype(str).str.strip().str.upper()

    merged = left.merge(

        right[["_KEY_", profit_col, cost_col, acct_col]],

        on="_KEY_", how="left", suffixes=("", "_MYLOC")

    ).drop(columns=["_KEY_"], errors="ignore")

    # Build GL string; keep as text

    for c in (profit_col, cost_col, acct_col):

        merged[c] = merged[c].astype(str).str.strip()

    merged["GL"] = (

        merged[profit_col].fillna("").astype(str).str.strip() + "." +

        merged[cost_col].fillna("").astype(str).str.strip()   + "." +

        merged[acct_col].fillna("").astype(str).str.strip()

    ).str.strip(".")

    # Optional: also surface the three parts with canonical names for visibility

    merged.rename(columns={

        profit_col: "Profit Center (MY LOC)",

        cost_col:   "Cost Center (MY LOC)",

        acct_col:   "Account # (MY LOC)",

    }, inplace=True)

    return merged

def _build_pivot_summary(df: pd.DataFrame) -> pd.DataFrame:

    """

    Create a compact pivot-like summary:

      - Group by Assigned Location Code and GL

      - Include row count

      - Attempt to include SUM of first numeric spend-like column if available

    """

    if df.empty:

        return pd.DataFrame(columns=["Assigned Location Code", "GL", "Count"])

    # Count

    grp_cols = [c for c in ["Assigned Location Code", "GL"] if c in df.columns]

    if not grp_cols:

        return pd.DataFrame(columns=["Assigned Location Code", "GL", "Count"])

    base = df.groupby(grp_cols, dropna=False).size().reset_index(name="Count")

    # Try to add one useful sum column if present

    numeric_candidates = [

        "Paid", "Paid Amount", "Amount", "Total", "Charge", "Spend",

        "Total Paid Minus Duty and CAD Tax"

    ]

    num_col = next((c for c in numeric_candidates if c in df.columns), None)

    if num_col:

        # make numeric; keep NaNs as 0 for sum

        val = pd.to_numeric(df[num_col].astype(str)

                            .str.replace("(", "-", regex=False)

                            .str.replace(")", "", regex=False),

                            errors="coerce").fillna(0.0)

        tmp = df.copy()

        tmp["_VAL_"] = val

        sum_df = tmp.groupby(grp_cols, dropna=False)["_VAL_"].sum().reset_index(name=f"Sum of {num_col}")

        base = base.merge(sum_df, on=grp_cols, how="left")

    # Sort for readability

    base = base.sort_values(grp_cols + ["Count"]).reset_index(drop=True)

    return base

# ============================== Header helpers ===============================

def _find_first_col(df: pd.DataFrame, candidates: Iterable[str]) -> str | None:

    """Return the first candidate in df.columns (case-sensitive first, then case-insensitive)."""

    for c in candidates:

        if c in df.columns:

            return c

    lower_map = {c.lower(): c for c in df.columns}

    for c in candidates:

        real = lower_map.get(str(c).lower())

        if real:

            return real

    return None

def _normalize_headers(

    df: pd.DataFrame,

    mapping: dict[str, str],

    drop_sources: bool = False

) -> pd.DataFrame:

    """

    Case-insensitive left->right header normalization.

    - If target exists, fill only blank target cells from source (no overwrite).

    - If target doesn't exist, create it from source.

    - Optionally drop the original left-side columns (we keep them for later revert).

    """

    df = df.copy()

    lower_to_real = {c.lower(): c for c in df.columns}

    def _blank_mask(s: pd.Series) -> pd.Series:

        return s.isna() | (s.astype(str).str.strip() == "")

    sources_to_drop = []

    for left_lower, right_name in mapping.items():

        src_real = lower_to_real.get(left_lower)

        if not src_real:

            continue

        if right_name in df.columns:

            mask = _blank_mask(df[right_name])

            df.loc[mask, right_name] = df.loc[mask, src_real]

        else:

            df[right_name] = df[src_real]

        sources_to_drop.append(src_real)

    if drop_sources:

        drop_cols = [c for c in sources_to_drop if c not in mapping.values()]

        df = df.drop(columns=drop_cols, errors="ignore")

    return df

def _revert_headers(df: pd.DataFrame, reverse_map: dict[str, str]) -> pd.DataFrame:

    """

    Revert normalized (RIGHT) column names back to their original (LEFT) names for export.

    Only rename columns that exist.

    """

    rename_real = {src: tgt for src, tgt in reverse_map.items() if src in df.columns}

    return df.rename(columns=rename_real)

# ============================== Writers ======================================

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
For Sale Page
 
