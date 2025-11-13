# extract_codes.py

import pandas as pd

import re

from typing import Iterable


class Extractor:

    """

    Handles code extraction logic:

      - Ensure Consignor/Consignee code columns exist

      - Apply Origin/Dest Type Code priority

      - Extract codes from Consignor/Consignee text

        using a flexible codes_df (supports 'Code', 'Codes', etc.)

    """

    def __init__(self):

        self._code_col_cache: str | None = None

    # ---------- basic column helpers ----------

    def create_columns(self, df: pd.DataFrame) -> None:

        for col in ["Consignor Code", "Consignee Code"]:

            if col not in df.columns:

                df[col] = None

    def lower_columns(self, df: pd.DataFrame, *cols: str) -> None:

        for c in cols:

            if c in df.columns:

                df[c] = df[c].astype(str).str.upper()

    # ---------- codes_df handling ----------

    def _get_codes_series(self, codes) -> pd.Series:

        """

        Accepts:

          - a DataFrame with column 'Code'/'Codes'/etc.

          - OR a simple iterable list of codes.

        Returns uppercase, stripped Series of codes.

        """

        if isinstance(codes, pd.DataFrame):

            if self._code_col_cache and self._code_col_cache in codes.columns:

                col = self._code_col_cache

            else:

                possible_cols = ["Code", "Codes", "Loc Code", "Loc_Code", "Location Code"]

                col = next((c for c in possible_cols if c in codes.columns), None)

                if col is None:

                    raise KeyError(

                        f"codes_df must contain one of {possible_cols}. "

                        f"Found: {list(codes.columns)}"

                    )

                self._code_col_cache = col

            series = (

                codes[col]

                .dropna()

                .astype(str)

                .str.upper()

                .str.strip()

            )

            series = series[series != ""]

            return series

        # If not a DataFrame, assume it's a plain iterable of codes

        if isinstance(codes, Iterable) and not isinstance(codes, (str, bytes)):

            return pd.Series(list(codes), dtype="string").str.upper().str.strip()

        raise TypeError(

            "codes must be a pandas DataFrame or iterable of codes. "

            f"Got {type(codes)}"

        )

    # ---------- priority 1: Origin / Dest Type Code ----------

    def apply_type_code_priority(self, df: pd.DataFrame) -> pd.DataFrame:

        """

        Priority 1:

          - Origin Type Code -> Consignor Code

          - Dest Type Code   -> Consignee Code

        (only if these columns exist)

        """

        if "Origin Type Code" in df.columns:

            origin = df["Origin Type Code"].astype(str).str.strip()

            df["Consignor Code"] = origin.replace({"": None})

        if "Dest Type Code" in df.columns:

            dest = df["Dest Type Code"].astype(str).str.strip()

            df["Consignee Code"] = dest.replace({"": None})

        return df

    # ---------- text extraction ----------

    @staticmethod

    def _needs_fill(code_val, type_val) -> bool:

        """

        We try to extract from text if:

          - code is blank/None

          - OR type is blank

          - OR type contains 'THIRD' (third party)

        """

        code_str = "" if code_val is None else str(code_val).strip()

        type_str = "" if type_val is None else str(type_val).upper().strip()

        if code_str == "" or type_str == "" or "THIRD" in type_str:

            return True

        return False

    @staticmethod

    def _extract_from_text(text: object, codes_series: pd.Series) -> str | None:

        """

        Strict matching: full token match only.

        No partial matches (e.g. '67' will not match inside '67N').

        """

        if text is None:

            return None

        t = str(text).upper()

        tokens = re.findall(r"[A-Z0-9]+", t)

        # Check longer codes first so '067N' wins over '67'

        for code in sorted(codes_series.unique(), key=len, reverse=True):

            if code in tokens:

                return code

        return None

    def extract_from_consignor_consignee(

        self,

        df: pd.DataFrame,

        codes_df,

    ) -> pd.DataFrame:

        """

        Priority 2:

          - For rows where code still needs fill (see _needs_fill),

            search Consignor/Consignee text for a code in codes_df.

        """

        codes_series = self._get_codes_series(codes_df)

        # Consignor

        if "Consignor" in df.columns:

            df["Consignor Code"] = df.apply(

                lambda row: self._extract_from_text(row["Consignor"], codes_series)

                if self._needs_fill(

                    row.get("Consignor Code"),

                    row.get("Consignor Type", ""),

                )

                else row.get("Consignor Code"),

                axis=1,

            )

        # Consignee

        if "Consignee" in df.columns:

            df["Consignee Code"] = df.apply(

                lambda row: self._extract_from_text(row["Consignee"], codes_series)

                if self._needs_fill(

                    row.get("Consignee Code"),

                    row.get("Consignee Type", ""),

                )

                else row.get("Consignee Code"),

                axis=1,

            )

        return df
 
