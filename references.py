# references.py

from pathlib import Path

import pandas as pd

from typing import Tuple

from io_utils import FileIO

from constants import (

    CINTAS_LOCATION_TABLE_FILE,

    COMPLETE_LOCATION_TABLE_FILE,

    LOCATION_CODES_CANDIDATES,

)


class ReferenceLoader:

    """

    Loads:

      - Location codes workbook  -> DataFrame (column 'Code')

      - MY LOCATION TABLE.xlsx   -> Cintas location table

      - Complete coding table    -> Full loc/profit/cost centers

    """

    def load(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

        codes_path = self._find_codes_path()

        df_codes_raw = pd.read_excel(codes_path)

        # Normalize column names for easier matching

        df_codes_raw.columns = [c.strip() for c in df_codes_raw.columns]

        possible_cols = ["Codes", "Code", "Loc Code", "Loc_Code", "Location Code"]

        code_col = next((c for c in possible_cols if c in df_codes_raw.columns), None)

        if code_col is None:

            raise ValueError(

                f"Location Codes file '{codes_path.name}' must contain one of {possible_cols}. "

                f"Found columns: {list(df_codes_raw.columns)}"

            )

        codes_df = df_codes_raw[[code_col]].rename(columns={code_col: "Code"})

        codes_df["Code"] = (

            codes_df["Code"]

            .astype(str)

            .str.upper()

            .str.strip()

        )

        codes_df = codes_df[codes_df["Code"] != ""]

        cintas_location_table = FileIO.read_excel_here(CINTAS_LOCATION_TABLE_FILE)

        complete_location_table = FileIO.read_excel_here(COMPLETE_LOCATION_TABLE_FILE)

        return codes_df, cintas_location_table, complete_location_table

    @staticmethod

    def _find_codes_path() -> Path:

        """

        Find the location codes Excel based on LOCATION_CODES_CANDIDATES.

        """

        base = Path(__file__).resolve().parent

        for fname in LOCATION_CODES_CANDIDATES:

            p = base / fname

            if p.exists():

                return p

        raise FileNotFoundError(

            "Location Codes Excel not found in app folder. "

            f"Expected one of: {', '.join(LOCATION_CODES_CANDIDATES)}"

        )
 
