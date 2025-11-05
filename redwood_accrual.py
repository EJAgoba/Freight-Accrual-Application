# redwood_accrual.py
from __future__ import annotations
import io, csv, re, hashlib
from typing import Callable, Iterable
import pandas as pd
import streamlit as st

# ====================== CONFIG ======================

# Redwood -> pipeline header normalization
HEADER_MAP = {
    "destination facility": "Consignee",
    "origin address":       "Origin Addresss",   # triple 's' per your pipeline
    "origin state":         "Origin State Code",
    "destination address":  "Dest Address1",
    "destination city":     "Dest City",
    "destination state":    "Dest State Code",
    "origin facility":      "Consignor",
}

# Revert pipeline headers back to Redwood names for export
REVERT_MAP = {
    "Consignee":            "Destination Facility",
    "Origin Addresss":      "Origin Address",
    "Origin State Code":    "Origin State",
    "Dest Address1":        "Destination Address",
    "Dest City":            "Destination City",
    "Dest State Code":      "Destination State",
    "Consignor":            "Origin Facility",
}

# Candidate BOL column names (case-sensitive first, then case-insensitive)
BOL_CANDIDATES = ["BOL Number", "BOL", "BOLNumber", "Pro/BOL", "Pro / BOL", "Pro", "Reference", "B/L", "BL"]

# Columns the pipeline expects; we’ll create blanks if missing
REQUIRED_PIPELINE_COLS = [
    "Consignor","Consignee","Consignor Code","Consignee Code",
    "Dest Address1","Dest City","Dest State Code",
    "Origin Addresss","Origin City","Origin State Code",
    "Profit Center","Cost Center","Account #",
]

# Spend preference order for Pivot
SPEND_CANDIDATES = [
    "Total Paid Minus Duty and CAD Tax",
    "Paid Amount","Paid","Amount","Total","Spend","Charge","Charges"
]

# ====================== PUBLIC UI ======================

def render_redwood_accrual_ui(
    load_reference_tables: Callable[[], tuple[list[str], pd.DataFrame, pd.DataFrame]],
    run_pipeline: Callable[[pd.DataFrame, pd.DataFrame, pd.DataFrame, list[str]], pd.DataFrame],
) -> None:
    st.header("Redwood Accrual")

    c1, c2 = st.columns(2)
    with c1:
        a3_file = st.file_uploader("Upload **A3** file (TXT / CSV / Excel)",
                                   type=["txt","text","csv","xls","xlsx","xlsm"], key="rw_a3")
    with c2:
        redwood_file = st.file_uploader("Upload **Redwood** file (TXT / CSV / Excel)",
                                        type=["txt","text","csv","xls","xlsx","xlsm"], key="rw_rw")

    if not st.button("Run Redwood Accrual", type="primary"):
        return
    if a3_file is None or redwood_file is None:
        st.error("Please upload both the A3 file and the Redwood file.")
        return

    # ---------- Read raw bytes once (lets us cache by content) ----------
    a3_bytes = a3_file.read(); a3_file.seek(0)
    rw_bytes = redwood_file.read(); redwood_file.seek(0)
    a3_name  = (a3_file.name or "").lower()
    rw_name  = (redwood_file.name or "").lower()

    # ---------- Detect BOL columns quickly using cached lightweight reads ----------
    a3_df_head = _cached_read_any(a3_name, a3_bytes)
    rw_df_head = _cached_read_any(rw_name, rw_bytes)
    a3_bol = _find_first_col(a3_df_head, BOL_CANDIDATES)
    rw_bol = _find_first_col(rw_df_head, BOL_CANDIDATES)
    if a3_bol is None or rw_bol is None:
        st.error("Could not find a BOL / Pro-BOL column in one or both files.")
        return

    # ---------- STRICT requirement: KEEP ONLY Redwood BOLs NOT IN A3 (cached) ----------
    filtered = _cached_anti_join(a3_bytes, a3_name, rw_bytes, rw_name, a3_bol, rw_bol)
    st.success(f"Filtered Redwood rows not in A3 by normalized BOL: **{len(filtered):,}**")
    st.caption(f"After anti-join: {len(filtered):,} rows")

    if filtered.empty:
        st.warning("Nothing to process — all Redwood BOLs appear in A3 (after normalization).")
        return

    # ---------- Tag immutable row id BEFORE any pipeline work ----------
    filtered = filtered.reset_index(drop=True).copy()
    filtered["__ROW_ID__"] = filtered.index.astype("int64")

    # ---------- Normalize headers for your pipeline ----------
    filtered = _normalize_headers(filtered, HEADER_MAP)
    for c in REQUIRED_PIPELINE_COLS:
        if c not in filtered.columns:
            filtered[c] = ""

    # ---------- Load references (cached) ----------
    try:
        location_codes, my_location_table, complete_loc_tbl = _cached_refs(load_reference_tables)
    except Exception as e:
        st.error(f"Reference load error: {e}")
        return

    # ---------- Make reference tables unique on likely join keys ----------
    my_location_table  = _dedupe_by_keys(my_location_table,  ["Loc Code", "Combined Address"])
    complete_loc_tbl   = _dedupe_by_keys(complete_loc_tbl,   ["Loc Code"])

    # ---------- Run your pipeline (cached by small content hashes) ----------
    flt_hash = _df_small_hash(filtered, cols=["__BOL_NORM__","__ROW_ID__"])
    my_hash  = _df_small_hash(my_location_table)
    cmp_hash = _df_small_hash(complete_loc_tbl)
    loc_key  = tuple(sorted(location_codes)) if isinstance(location_codes, list) else tuple()

    with st.spinner("Running Redwood accrual logic…"):
        try:
            result = _cached_pipeline(
                flt_hash, loc_key, my_hash, cmp_hash,
                filtered.copy(), my_location_table, complete_loc_tbl, location_codes,
                run_pipeline
            )
        except Exception as e:
            st.error(f"Pipeline error: {e}")
            return

    st.caption(f"After pipeline (pre-collapse): {len(result):,} rows")

    # ---------- Ensure __ROW_ID__ survived ----------
    if "__ROW_ID__" not in result.columns:
        st.error(
            "The pipeline removed '__ROW_ID__'. "
            "Please ensure your merges/selects carry this column through so we can guarantee 1:1 output."
        )
        return

    # ---------- HARD guarantee: final == filtered rowset & order ----------
    final = _enforce_exact_rowset(filtered, result, key="__ROW_ID__")
    st.caption(f"After enforce_exact_rowset: {len(final):,} rows (must equal filtered count {len(filtered):,})")

    if len(final) != len(filtered):
        st.error(f"Row count mismatch after enforcement: filtered={len(filtered):,}, final={len(final):,}")
        return

    # ---------- GL from EJ columns ----------
    for ej in ["Profit Center EJ","Cost Center EJ","Account # EJ"]:
        if ej not in final.columns:
            final[ej] = ""
    final["GL"] = (
        final["Profit Center EJ"].astype(str).str.strip() + "." +
        final["Cost Center EJ"].astype(str).str.strip() + "." +
        final["Account # EJ"].astype(str).str.strip()
    ).str.strip(".")

    # ---------- Pivot: GL + Sum of Spend ----------
    spend_col = next((c for c in SPEND_CANDIDATES if c in final.columns), None)
    if spend_col:
        pivot = _pivot_gl_sum(final, spend_col)
        st.caption(f"Pivot uses spend column: **{spend_col}**")
    else:
        pivot = pd.DataFrame(columns=["GL","Sum of Spend"])
        st.warning("No spend column found for Pivot Summary; sheet will be empty.")

    # ---------- Revert headers for export ----------
    export_df = _revert_headers(final, REVERT_MAP)

    st.success(f"Done! Redwood Accrual processed **{len(export_df):,}** rows (exact match to filtered).")

    # ---------- Export ----------
    xls_bytes = _to_multi_sheet_xlsx({"Redwood Accrual": export_df, "Pivot Summary": pivot})
    st.download_button("⬇️ Download Redwood Accrual (Excel w/ Pivot Summary)",
                       data=xls_bytes,
                       file_name="Redwood Accrual - Not In A3.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    st.download_button("⬇️ Download Redwood Accrual (CSV)",
                       data=export_df.to_csv(index=False).encode("utf-8"),
                       file_name="Redwood Accrual - Not In A3.csv",
                       mime="text/csv")

# ====================== CACHED HELPERS (speedups) ======================

@st.cache_data(show_spinner=False)
def _cached_read_any(name_lower: str, file_bytes: bytes) -> pd.DataFrame:
    """
    Fast, pure read (bytes + name) so Streamlit can cache.
    Uses a quick delimiter guess instead of csv.Sniffer.
    """
    buf = io.BytesIO(file_bytes)
    if name_lower.endswith((".xls",".xlsx",".xlsm")):
        # openpyxl is stable; xlsxwriter is for writing
        return pd.read_excel(buf, dtype=str, engine="openpyxl")
    sample = file_bytes[:4096]
    # very fast heuristic: choose tab if tab density >= comma density
    sep = "\t" if (b"\t" in sample and sample.count(b"\t") >= sample.count(b",")) else ","
    return pd.read_csv(io.BytesIO(file_bytes), sep=sep, dtype=str,
                       engine="python", encoding="latin1", keep_default_na=False)

@st.cache_data(show_spinner=False)
def _cached_anti_join(a3_bytes: bytes, a3_name: str,
                      rw_bytes: bytes, rw_name: str,
                      a3_bol_name: str, rw_bol_name: str) -> pd.DataFrame:
    a3 = _cached_read_any(a3_name, a3_bytes)
    rw = _cached_read_any(rw_name, rw_bytes)

    a3["__BOL_NORM__"] = _normalize_bol_series(a3[a3_bol_name])
    rw["__BOL_NORM__"] = _normalize_bol_series(rw[rw_bol_name])

    a3_keys = a3[["__BOL_NORM__"]].drop_duplicates()

    out = (rw.merge(a3_keys, on="__BOL_NORM__", how="left", indicator=True)
             .loc[lambda d: d["_merge"] == "left_only"]
             .drop(columns=["_merge"])
             .reset_index(drop=True))
    return out

@st.cache_data(show_spinner=False)
def _cached_refs(loader: Callable[[], tuple[list[str], pd.DataFrame, pd.DataFrame]]):
    return loader()

def _df_small_hash(df: pd.DataFrame, cols: list[str] | None = None, n: int = 1000) -> str:
    """
    Small, stable cache key for a DataFrame: hash of CSV head over selected columns.
    """
    if cols is not None:
        cols = [c for c in cols if c in df.columns]
        df = df[cols]
    sample = df.head(n).to_csv(index=False).encode("utf-8", errors="ignore")
    return hashlib.sha1(sample).hexdigest()

@st.cache_data(show_spinner=True)
def _cached_pipeline(flt_hash: str, loc_key: tuple, my_hash: str, cmp_hash: str,
                     filtered: pd.DataFrame,
                     my_location_table: pd.DataFrame,
                     complete_loc_tbl: pd.DataFrame,
                     location_codes: list[str],
                     run_pipeline: Callable[..., pd.DataFrame]) -> pd.DataFrame:
    """
    Cache the pipeline based on small input hashes + refs key.
    Streamlit requires passing the actual DataFrames too, but only the hashes
    determine cache identity.
    """
    _ = (flt_hash, loc_key, my_hash, cmp_hash)  # keys only, ignore at runtime
    return run_pipeline(filtered, my_location_table, complete_loc_tbl, location_codes)

# ====================== CORE HELPERS ======================

def _find_first_col(df: pd.DataFrame, candidates: Iterable[str]) -> str | None:
    for c in candidates:
        if c in df.columns: return c
    lower = {col.lower(): col for col in df.columns}
    for c in candidates:
        real = lower.get(c.lower())
        if real: return real
    return None

def _normalize_bol_series(s: pd.Series) -> pd.Series:
    """Upper, trim, remove non-alnum, strip leading zeros (keep one '0')."""
    t = s.astype(str).str.upper().str.strip()
    t = t.str.replace(r"[^A-Z0-9]", "", regex=True)
    def _lz(x: str) -> str:
        y = x.lstrip("0")
        return y if y else "0"
    return t.map(_lz)

def _normalize_headers(df: pd.DataFrame, mapping: dict[str,str]) -> pd.DataFrame:
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

def _revert_headers(df: pd.DataFrame, rev: dict[str,str]) -> pd.DataFrame:
    rename_map = {src: tgt for src, tgt in rev.items() if src in df.columns}
    return df.rename(columns=rename_map)

def _dedupe_by_keys(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    keys = [k for k in keys if k in df.columns]
    return df.drop_duplicates(subset=keys).copy() if keys else df.copy()

def _first_nonnull(s: pd.Series):
    m = s.first_valid_index()
    return s[m] if m is not None else None

def _collapse_to_one_row(df: pd.DataFrame, key: str) -> pd.DataFrame:
    if key not in df.columns:
        return df.copy()
    out = (df.groupby(key, as_index=False)
             .agg(_first_nonnull))
    return out

def _enforce_exact_rowset(filtered: pd.DataFrame, result: pd.DataFrame, key: str) -> pd.DataFrame:
    """
    Guarantee final == filtered:
    1) Collapse result to one row per key
    2) Reindex to EXACTLY the filtered rowset & order
    3) Union columns and backfill from filtered if pipeline missed anything
    """
    if key not in filtered.columns:
        raise ValueError(f"`filtered` missing key column {key}")
    if key not in result.columns:
        raise ValueError(f"`result` missing key column {key}")

    collapsed = _collapse_to_one_row(result, key=key).set_index(key)
    base = filtered.set_index(key)

    # Reindex to filtered rowset & order
    aligned = collapsed.reindex(base.index)

    # Union columns, prefer pipeline; backfill with filtered values
    all_cols = list(set(aligned.columns) | set(base.columns))
    aligned = aligned.reindex(columns=all_cols)
    final = aligned.combine_first(base)

    return final.reset_index()

def _pivot_gl_sum(df: pd.DataFrame, spend_col: str | None) -> pd.DataFrame:
    if not spend_col:
        return pd.DataFrame(columns=["GL","Sum of Spend"])
    tmp = df.copy()
    tmp["_Spend_"] = pd.to_numeric(
        tmp[spend_col].astype(str).str.replace("(", "-", regex=False).str.replace(")", "", regex=False),
        errors="coerce"
    ).fillna(0.0)
    out = tmp.groupby("GL", dropna=False)["_Spend_"].sum().reset_index(name="Sum of Spend")
    out["Sum of Spend"] = out["Sum of Spend"].round(2)
    return out.sort_values("GL").reset_index(drop=True)

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
