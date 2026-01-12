# pipeline.py

import re
import pandas as pd
import streamlit as st

from address_merge import CombinedAddress
from address_crossref import Merger
from clean_codes import CodeFormatter
from map_types import TypeMapper, TypeCleaner
from matrix_map import MatrixMapper

DEBUG = False


class PipelineRunner:
    """Full accrual re-coding pipeline."""

    def run(
        self,
        accrual_df: pd.DataFrame,
        cintas_location_table: pd.DataFrame,
        complete_location_table: pd.DataFrame,
        location_codes_df,  # can be DF, Series, or list-like
    ) -> pd.DataFrame:

        # ===============================================================
        # 0. Normalize column names + required columns
        # ===============================================================
        accrual_df = accrual_df.rename(
            columns=lambda c: c.strip() if isinstance(c, str) else c
        )
        cintas_location_table = cintas_location_table.rename(
            columns=lambda c: c.strip() if isinstance(c, str) else c
        )
        complete_location_table = complete_location_table.rename(
            columns=lambda c: c.strip() if isinstance(c, str) else c
        )

        for col in ["Consignor Code", "Consignee Code", "Consignor Type", "Consignee Type"]:
            if col not in accrual_df.columns:
                accrual_df[col] = pd.NA

        # ===============================================================
        # 1. Prepare codes (always a DataFrame with 'Code')
        # ===============================================================
        if isinstance(location_codes_df, (list, tuple)):
            codes_series = pd.Series(location_codes_df, dtype="string")
            location_codes_df = pd.DataFrame({"Code": codes_series})
        elif isinstance(location_codes_df, pd.Series):
            location_codes_df = pd.DataFrame({"Code": location_codes_df})
        elif isinstance(location_codes_df, pd.DataFrame):
            pass
        else:
            raise TypeError(
                "location_codes_df must be a DataFrame, Series, or list-like. "
                f"Got: {type(location_codes_df)}"
            )

        if "Code" not in location_codes_df.columns:
            if "Codes" in location_codes_df.columns:
                location_codes_df = location_codes_df.rename(columns={"Codes": "Code"})
            else:
                raise ValueError(
                    "location_codes_df must contain a 'Code' column (or 'Codes' which "
                    "will be renamed to 'Code'). "
                    f"Found columns: {list(location_codes_df.columns)}"
                )

        codes_series = (
            location_codes_df["Code"]
            .dropna()
            .astype(str)
            .str.strip()
            .str.upper()
            .unique()
        )
        codes_sorted = sorted(codes_series, key=len, reverse=True)

        def find_code_in_text(text):
            """Return first matching code found as a token in text."""
            t = str(text or "").upper()
            tokens = re.findall(r"[A-Z0-9]+", t)
            for code in codes_sorted:
                if code in tokens:
                    return code
            return None

        def _blank_or_na(s: pd.Series) -> pd.Series:
            return s.isna() | (s.astype(str).str.strip() == "")

        # ===============================================================
        # 2. PRIORITY RULE – Org/Dest Type Code FIRST
        #    Omnitrans exception only if Carrier Name exists
        # ===============================================================
        if "Carrier Name" in accrual_df.columns:
            carrier_ok = (
                accrual_df["Carrier Name"]
                .astype(str)
                .str.strip()
                .str.lower()
                .ne("omnitrans")
            )
        else:
            carrier_ok = pd.Series(True, index=accrual_df.index)

        if "Org Type Code" in accrual_df.columns:
            org_val = accrual_df["Org Type Code"].astype(str).str.strip()
            mask = org_val.ne("") & carrier_ok
            accrual_df.loc[mask, "Consignor Code"] = org_val[mask].str.upper()

        if "Dest Type Code" in accrual_df.columns:
            dest_val = accrual_df["Dest Type Code"].astype(str).str.strip()
            mask = dest_val.ne("") & carrier_ok
            accrual_df.loc[mask, "Consignee Code"] = dest_val[mask].str.upper()

        # ===============================================================
        # 3. Extract from text ONLY IF type is blank or THIRD-PARTY
        # ===============================================================
        def needs_text_extract(type_col: pd.Series) -> pd.Series:
            return type_col.isna() | type_col.astype(str).str.upper().str.contains("THIRD", na=False)

        cons_extract_mask = needs_text_extract(accrual_df["Consignor Type"]) & _blank_or_na(accrual_df["Consignor Code"])
        dest_extract_mask = needs_text_extract(accrual_df["Consignee Type"]) & _blank_or_na(accrual_df["Consignee Code"])

        if "Consignor" in accrual_df.columns:
            accrual_df.loc[cons_extract_mask, "Consignor Code"] = (
                accrual_df.loc[cons_extract_mask, "Consignor"].apply(find_code_in_text)
            )

        if "Consignee" in accrual_df.columns:
            accrual_df.loc[dest_extract_mask, "Consignee Code"] = (
                accrual_df.loc[dest_extract_mask, "Consignee"].apply(find_code_in_text)
            )

        # ===============================================================
        # 3B. If a code exists but is NOT in the location table, force fallback
        #     to address ("ladders") merge by blanking it out + setting a flag.
        # ===============================================================
        # Identify the location-code column in your location table
        loc_code_col = None
        for c in ["Loc Code", "LOC CODE", "Location Code", "LocCode", "Code"]:
            if c in cintas_location_table.columns:
                loc_code_col = c
                break

        cons_force_addr = pd.Series(False, index=accrual_df.index)
        dest_force_addr = pd.Series(False, index=accrual_df.index)

        if loc_code_col is not None:
            valid_loc_codes = set(
                cintas_location_table[loc_code_col]
                .dropna()
                .astype(str)
                .str.strip()
                .str.upper()
                .unique()
            )

            cons_norm = accrual_df["Consignor Code"].astype(str).str.strip().str.upper()
            dest_norm = accrual_df["Consignee Code"].astype(str).str.strip().str.upper()

            cons_has = cons_norm.ne("") & cons_norm.ne("NAN") & accrual_df["Consignor Code"].notna()
            dest_has = dest_norm.ne("") & dest_norm.ne("NAN") & accrual_df["Consignee Code"].notna()

            cons_invalid = cons_has & (~cons_norm.isin(valid_loc_codes))
            dest_invalid = dest_has & (~dest_norm.isin(valid_loc_codes))

            # Force these rows to go through address merge
            cons_force_addr = cons_invalid.copy()
            dest_force_addr = dest_invalid.copy()

            # Blank invalid so downstream logic treats them as missing
            accrual_df.loc[cons_invalid, "Consignor Code"] = pd.NA
            accrual_df.loc[dest_invalid, "Consignee Code"] = pd.NA

        # ===============================================================
        # 4. Combined Address
        # ===============================================================
        comb = CombinedAddress()

        comb.create_combined_address_accrual(
            cintas_location_table,
            "Combined Address",
            "Loc_Address",
            "Loc_City",
            "Loc_ST",
        )

        comb.create_combined_address_accrual(
            accrual_df,
            "Consignee Combined Address",
            "Dest Address1",
            "Dest City",
            "Dest State Code",
        )

        comb.create_combined_address_accrual(
            accrual_df,
            "Consignor Combined Address",
            "Origin Addresss",  # keeping your original column name
            "Origin City",
            "Origin State Code",
        )

        cintas_location_table["Combined Address"] = (
            cintas_location_table["Combined Address"].astype(str).str.upper()
        )
        accrual_df["Consignee Combined Address"] = (
            accrual_df["Consignee Combined Address"].astype(str).str.upper()
        )
        accrual_df["Consignor Combined Address"] = (
            accrual_df["Consignor Combined Address"].astype(str).str.upper()
        )

        # ===============================================================
        # 5. Address merging ("ladders") AFTER extraction:
        #    - rows where code is still blank AND (type blank/THIRD)
        #    - OR rows flagged as "invalid code" (force address merge)
        # ===============================================================
        merger = Merger()

        addr_consignor_mask = (
            (_blank_or_na(accrual_df["Consignor Code"]) & needs_text_extract(accrual_df["Consignor Type"]))
            | cons_force_addr
        )
        addr_consignee_mask = (
            (_blank_or_na(accrual_df["Consignee Code"]) & needs_text_extract(accrual_df["Consignee Type"]))
            | dest_force_addr
        )

        # Run merge only on needed subsets, then write results back
        if addr_consignor_mask.any():
            tmp = merger.merge(
                accrual_df.loc[addr_consignor_mask].copy(),
                cintas_location_table,
                "Consignor Code",
            )
            if "Consignor Code" in tmp.columns:
                accrual_df.loc[addr_consignor_mask, "Consignor Code"] = tmp["Consignor Code"].values

        if addr_consignee_mask.any():
            tmp = merger.merge(
                accrual_df.loc[addr_consignee_mask].copy(),
                cintas_location_table,
                "Consignee Code",
            )
            if "Consignee Code" in tmp.columns:
                accrual_df.loc[addr_consignee_mask, "Consignee Code"] = tmp["Consignee Code"].values

        # ===============================================================
        # 6. Format codes
        # ===============================================================
        formatter = CodeFormatter()
        accrual_df = formatter.pad_codes(accrual_df, "Consignor Code", "Consignee Code")

        # ===============================================================
        # 7. Type Mapping
        # ===============================================================
        mapper = TypeMapper()

        accrual_df = mapper.map_types(
            accrual_df, cintas_location_table, "Consignor Code", "Consignor Type"
        )
        accrual_df = mapper.map_types(
            accrual_df, cintas_location_table, "Consignee Code", "Consignee Type"
        )

        cleaner = TypeCleaner()
        accrual_df = cleaner.fill_non_cintas(accrual_df, "Consignor Type", "Consignee Type")

        # ===============================================================
        # 8. Matrix Mapping
        # ===============================================================
        matrix = MatrixMapper()
        accrual_df["Assigned Location Code"] = accrual_df.apply(
            matrix.determine_profit_center, axis=1
        )

        # ===============================================================
        # 9. Join Profit/Cost Center
        # ===============================================================
        accrual_df = accrual_df.merge(
            complete_location_table[["Loc Code", "Prof_Cntr", "Cost_Cntr"]],
            left_on="Assigned Location Code",
            right_on="Loc Code",
            how="left",
        )

        accrual_df.rename(
            columns={"Prof_Cntr": "Profit Center EJ", "Cost_Cntr": "Cost Center EJ"},
            inplace=True,
        )

        # ===============================================================
        # 10. Account # EJ
        # ===============================================================
        accrual_df["Account # EJ"] = accrual_df.apply(
            lambda row: 621000
            if "G59" in str(row.get("Profit Center EJ", ""))
            else (
                621000
                if row.get("Consignee Code") == row.get("Assigned Location Code")
                else 621020
            ),
            axis=1,
        )

        # ===============================================================
        # 11. De-dupe
        # ===============================================================
        if {"Invoice Number", "Paid Amount"}.issubset(accrual_df.columns):
            accrual_df = accrual_df.drop_duplicates(subset=["Invoice Number", "Paid Amount"])

        # ===============================================================
        # 12. Automation Accuracy
        # ===============================================================
        if {"Profit Center", "Profit Center EJ"}.issubset(accrual_df.columns):
            match = (
                (accrual_df["Profit Center"] == accrual_df["Profit Center EJ"])
                & accrual_df["Profit Center"].notna()
                & accrual_df["Profit Center EJ"].notna()
            )
            accrual_df["Automation Accuracy"] = match.astype(int)
        else:
            accrual_df["Automation Accuracy"] = 0

        # ===============================================================
        # 13. Column Ordering
        # ===============================================================
        first_cols = [
            "Profit Center",
            "Cost Center",
            "Account #",
            "Automation Accuracy",
            "Profit Center EJ",
            "Cost Center EJ",
            "Account # EJ",
        ]

        final_cols = [c for c in first_cols if c in accrual_df.columns] + [
            c for c in accrual_df.columns if c not in first_cols
        ]

        return accrual_df[final_cols]
