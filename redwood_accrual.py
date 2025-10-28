# redwood_accrual.py

from __future__ import annotations

import io

import csv

from typing import Callable, Iterable

import pandas as pd

import streamlit as st

# ================================== Config ==================================

# Left -> Right header mapping (case-insensitive on the left)

HEADER_MAP = {

    "destination facility": "Consignee",

    "origin address":       "Origin Addresss",   # triple 's' per your pipeline

    "origin state":         "Origin State Code",

    "destination address":  "Dest Address1",

    "destination city":     "Dest City",

    "destination state":    "Dest State Code",

    "origin facility":      "Consignor",

}

# Drop the original (left-side) columns after mapping?

DROP_SOURCE_COLUMNS = True

# Candidate BOL headers (case-insensitive search)

BOL_CANDIDATES = ["BOL Number", "BOL", "BOLNumber", "Pro/BOL", "Pro / BOL", "Pro", "Reference", "Load Number"]

# Minimum columns your pipeline expects (we'll create blanks if missing)

REQUIRED_PIPELINE_COLS = [

    "Consignor", "Consignee",

    "Consignor Code", "Consignee Code",

    "Dest Address1", "Dest City", "Dest State Code",

    "Origin Addresss", "Origin City", "Origin State Code",

    "Profit Center", "Cost Center", "Account #",

]

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

    # ---- Normalize headers per mapping ----

    filtered = _normalize_headers(filtered, HEADER_MAP, drop_sources=DROP_SOURCE_COLUMNS)

    # ---- Ensure required pipeline columns exist ----

    for col in REQUIRED_PIPELINE_COLS:

        if col not in filtered.columns:

            filtered[col] = ""

    # ---- Load references & run your pipeline ----

    try:

        location_codes, cintas_loc_tbl, complete_loc_tbl = load_reference_tables()

    except Exception as e:

        st.error(f"Reference load error: {e}")

        return

    with st.spinner("Assigning location codes (matrix/type mapping)…"):

        try:

            result_df = run_pipeline(filtered.copy(), cintas_loc_tbl, complete_loc_tbl, location_codes)

        except Exception as e:

            st.error(f"Pipeline error: {e}")

            return

    st.success(f"Done! Redwood Accrual processed **{len(result_df):,}** rows.")

    # ---- Downloads ----

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

        # If we can't inspect, fall back to first sheet silently

        return None

def _read_any(upload, sheet_name: str | None = None) -> pd.DataFrame:

    """

    Read TXT/CSV (delimiter-sniffed) or Excel (optionally selected sheet).

    Returns all columns as strings to avoid type issues.

    """

    name = (upload.name or "").lower()

    if _looks_like_excel(upload):

        try:

            # Reset pointer for re-use

            upload.seek(0)

        except Exception:

            pass

        try:

            if sheet_name:

                return pd.read_excel(upload, sheet_name=sheet_name, dtype=str)

            # No sheet specified: read first sheet

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

        # Peek for sniffing

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

# ============================== Helpers ======================================

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

    drop_sources: bool = True

) -> pd.DataFrame:

    """

    Case-insensitive left->right header normalization.

    - If target exists, fill only blank target cells from source (no overwrite).

    - If target doesn't exist, create it from source.

    - Optionally drop the original left-side columns.

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
