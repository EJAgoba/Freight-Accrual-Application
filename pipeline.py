# pipeline.py
from __future__ import annotations
import pandas as pd
from extract_codes import Extractor
from address_merge import CombinedAddress
from address_crossref import Merger
from clean_codes import CodeFormatter
from map_types import TypeMapper, TypeCleaner
from matrix_map import MatrixMapper

# ---------- merge/key helpers ----------
def _canon_code(s: pd.Series) -> pd.Series:
   """
   Make location codes merge-safe:
   - force text (keeps leading zeros)
   - Unicode normalize
   - remove NBSP, zero-width, and ALL whitespace
   - upper-case
   - keep only [A-Z0-9]
   """
   s = s.astype(str).str.normalize("NFKC")
   s = (s
        .str.replace("\u00A0", "", regex=False)   # NBSP
        .str.replace("\u200B", "", regex=False)   # zero-width space
        .str.replace(r"\s+", "", regex=True))     # any whitespace
   s = s.str.strip().str.upper()
   return s.str.replace(r"[^A-Z0-9]", "", regex=True)

class PipelineRunner:
   """Encapsulates the accrual re-coding pipeline as a single callable."""
   def run(
       self,
       accrual_df: pd.DataFrame,
       cintas_location_table: pd.DataFrame,
       complete_location_table: pd.DataFrame,
       location_codes: list[str],
   ) -> pd.DataFrame:
       # --- Extract Location Codes ---
       extractor = Extractor()
       extractor.create_columns(accrual_df)
       extractor.lower_columns(accrual_df, 'Consignor', 'Consignee')
       extractor.prefill_from_loc_columns(accrual_df, location_codes)
       extractor.extract1(accrual_df, 'Consignor', 'Consignor Code', location_codes, only_null=True)
       extractor.extract1(accrual_df, 'Consignee', 'Consignee Code', location_codes, only_null=True)
       # --- Combined Address ---
       comb = CombinedAddress()
       comb.create_combined_address_accrual(
           cintas_location_table, 'Combined Address', 'Loc_Address', 'Loc_City', 'Loc_ST'
       )
       comb.create_combined_address_accrual(
           accrual_df, 'Consignee Combined Address', 'Dest Address1', 'Dest City', 'Dest State Code'
       )
       comb.create_combined_address_accrual(
           accrual_df, 'Consignor Combined Address', 'Origin Addresss', 'Origin City', 'Origin State Code'
       )
       cintas_location_table['Combined Address'] = cintas_location_table['Combined Address'].astype(str).str.upper()
       accrual_df['Consignee Combined Address']  = accrual_df['Consignee Combined Address'].astype(str).str.upper()
       accrual_df['Consignor Combined Address']  = accrual_df['Consignor Combined Address'].astype(str).str.upper()
       # --- Cross reference the combined address ---
       merger = Merger()
       accrual_df = merger.merge(accrual_df, cintas_location_table, 'Consignor Code')
       accrual_df = merger.merge(accrual_df, cintas_location_table, 'Consignee Code')
       # --- Clean up the codes ---
       formatter = CodeFormatter()
       accrual_df = formatter.pad_codes(accrual_df, 'Consignor Code', 'Consignee Code')
       # --- Populate Type Codes ---
       type_mapper = TypeMapper()
       accrual_df = type_mapper.map_types(accrual_df, cintas_location_table, 'Consignor Code', 'Consignor Type')
       accrual_df = type_mapper.map_types(accrual_df, cintas_location_table, 'Consignee Code', 'Consignee Type')
       cleaner = TypeCleaner()
       accrual_df = cleaner.fill_non_cintas(accrual_df, 'Consignor Type', 'Consignee Type')
       # --- Matrix mapping for Assigned Location Code ---
       matrix_mapper = MatrixMapper()
       accrual_df['Assigned Location Code'] = accrual_df.apply(matrix_mapper.determine_profit_center, axis=1)
       # ===================== FIXED MERGE (no shadowing, normalized keys) =====================
       # 1) Canonical join keys
       if 'Assigned Location Code' in accrual_df.columns:
           accrual_df['Assigned Location Code'] = _canon_code(accrual_df['Assigned Location Code'])
       ref = complete_location_table[['Loc Code', 'Prof_Cntr', 'Cost_Cntr']].copy()
       ref['__key'] = _canon_code(ref['Loc Code'])
       ref = ref.drop_duplicates(subset=['__key'])  # guarantee m:1
       # 2) Build left key
       accrual_df['__key'] = accrual_df['Assigned Location Code']
       # 3) Avoid Loc Code shadowing from prior steps
       if 'Loc Code' in accrual_df.columns:
           accrual_df = accrual_df.drop(columns=['Loc Code'])
       # 4) Safe many-to-one merge; keep reference columns
       accrual_df = accrual_df.merge(
           ref[['__key', 'Loc Code', 'Prof_Cntr', 'Cost_Cntr']],
           on='__key',
           how='left',
           validate='m:1',
           suffixes=('', '_ref')
       )
       # 5) Final column names
       accrual_df.rename(
           columns={
               'Prof_Cntr': 'Profit Center EJ',
               'Cost_Cntr': 'Cost Center EJ',
               'Loc Code':  'Loc Code (ref)'
           },
           inplace=True
       )
       # If you want a single "Loc Code" field in the output, prefer the RHS match and
       # fall back to the assigned code (so 061R shows up even if not found).
       accrual_df['Loc Code'] = accrual_df['Loc Code (ref)'].fillna(accrual_df['Assigned Location Code'])
       # --- Account # EJ rule ---
       accrual_df['Account # EJ'] = accrual_df.apply(
           lambda row: 621000 if 'G59' in str(row.get('Profit Center EJ', ''))
           else (621000 if row.get('Consignee Code') == row.get('Assigned Location Code') else 621020),
           axis=1
       )
       # --- De-dupe on Invoice Number + Paid Amount (if present) ---
       if {'Invoice Number', 'Paid Amount'}.issubset(accrual_df.columns):
           accrual_df = accrual_df.drop_duplicates(subset=['Invoice Number', 'Paid Amount'])
       # --- Automation Accuracy ---
       if 'Profit Center' in accrual_df.columns:
           accrual_df['Profit Center'] = accrual_df['Profit Center'].astype("string")
       if 'Profit Center EJ' in accrual_df.columns:
           accrual_df['Profit Center EJ'] = accrual_df['Profit Center EJ'].astype("string")
       accrual_df['Automation Accuracy'] = accrual_df.apply(
           lambda row: (1 if (pd.notna(row.get('Profit Center'))
                              and pd.notna(row.get('Profit Center EJ'))
                              and row.get('Profit Center') == row.get('Profit Center EJ')) else 0), axis=1
       )
       # --- Column ordering (if present) ---
       first_cols = [
           'Profit Center', 'Cost Center', 'Account #', 'Automation Accuracy',
           'Profit Center EJ', 'Cost Center EJ', 'Account # EJ',
           'Assigned Location Code', 'Loc Code'
       ]
       ordered = [c for c in first_cols if c in accrual_df.columns] + \
                 [c for c in accrual_df.columns if c not in first_cols]
       accrual_df = accrual_df[ordered]
       return accrual_df
