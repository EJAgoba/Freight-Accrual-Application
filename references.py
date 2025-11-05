# references.py

from pathlib import Path

import hashlib

import pandas as pd

from constants import CINTAS_LOCATION_TABLE_FILE, COMPLETE_LOCATION_TABLE_FILE

def _canon_code(s: pd.Series) -> pd.Series:

    """Normalize codes (remove spaces, uppercase, strip)"""

    return (s.astype(str)

              .str.strip()

              .str.upper()

              .str.replace(r"\s+", "", regex=True))

def _file_hash(p: Path) -> str:

    """Generate hash so cache refreshes when Excel file changes"""

    return hashlib.sha1(p.read_bytes()).hexdigest()

class ReferenceLoader:

    def load(self):

        here = Path(__file__).parent

        loc_path  = here / CINTAS_LOCATION_TABLE_FILE

        comp_path = here / COMPLETE_LOCATION_TABLE_FILE

        # hash for Streamlit cache invalidation

        _ = (_file_hash(loc_path), _file_hash(comp_path))

        # --- Read as text to keep leading zeros like '061R' ---

        loc_tbl  = pd.read_excel(loc_path, dtype=str)

        comp_tbl = pd.read_excel(comp_path, dtype=str)

        # --- Clean up both tables ---

        if "Loc Code" in loc_tbl.columns:

            loc_tbl["Loc Code"] = _canon_code(loc_tbl["Loc Code"])

        for c in ["Prof_Cntr", "Cost_Cntr", "Profit Center", "Cost Center"]:

            if c in loc_tbl.columns:

                loc_tbl[c] = loc_tbl[c].astype(str).str.strip().str.upper()

        if "Loc Code" in comp_tbl.columns:

            comp_tbl["Loc Code"] = _canon_code(comp_tbl["Loc Code"])

            comp_tbl = comp_tbl.drop_duplicates(subset=["Loc Code"])

        # location_codes is optional; can return empty if unused

        location_codes = []

        return location_codes, loc_tbl, comp_tbl
 
