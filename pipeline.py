# pipeline.py
import pandas as pd
from extract_codes import Extractor
from address_merge import CombinedAddress
from address_crossref import Merger
from clean_codes import CodeFormatter
from map_types import TypeMapper, TypeCleaner
from matrix_map import MatrixMapper

# ---------- helpers (new) ----------
def _canon_code(s: pd.Series) -> pd.Series:
   """
   Make location codes merge-safe:
   - force text (preserve leading zeros)
   - Unicode normalize
   - remove NBSP, zero-width, and ALL whitespace
   - upper-case
   - keep only [A-Z0-9] (no stray punctuation)
   """
   s = s.astype(str)
   # normalize & strip
   s = s.str.normalize("NFKC")
   s = (s
        .str.replace("\u00A0", "", regex=False)   # NBSP
        .str.replace("\u200B", "", regex=False)   # zero-width space
        .str.replace(r"\s+", "", regex=True))     # any whitespace
   s = s.str.strip().str.upper()
   # keep only alphanumerics (optional but safe)
   s = s.str.replace(r"[^A-Z0-9]", "", regex=True)
   return s

class PipelineRunner:
   """Encapsulates the accrual recoding pipeline as a single callable."""
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
       # 🔒 SAVE what extract.py produced so it can’t be overwritten
       accrual_df['Consignor Code_extract'] = accrual_df['Consignor Code']
       accrual_df['Consignee Code_extract'] = accrual_df['Consignee Code']
       # --- Combined Address ---
       combined_address = CombinedAddress()
       combined_address.create_combined_address_accrual(
           cintas_location_table, 'Combined Address', 'Loc_Address', 'Loc_City', 'Loc_ST'
       )
       combined_address.create_combined_address_accrual(
           accrual_df, 'Consignee Combined Address', 'Dest Address1', 'Dest City', 'Dest State Code'
       )
       combined_address.create_combined_address_accrual(
           accrual_df, 'Consignor Combined Address', 'Origin Addresss', 'Origin City', 'Origin State Code'
       )
       cintas_location_table['Combined Address'] = cintas_location_table['Combined Address'].astype(str).str.upper()
       accrual_df['Consignee Combined Address'] = accrual_df['Consignee Combined Address'].astype(str).str.upper()
       accrual_df['Consignor Combined Address'] = accrual_df['Consignor Combined Address'].astype(str).str.upper()
       # --- Cross reference the combined address ---
       merger = Merger()
       accrual_df = merger.merge(accrual_df, cintas_location_table, 'Consignor Code')
       accrual_df = merger.merge(accrual_df, cintas_location_table, 'Consignee Code')
       # 🔙 RESTORE extractor results wherever they exist (non-null / non-empty)
       for col in ['Consignor Code', 'Consignee Code']:
           extract_col = f'{col}_extract'
           # if extractor gave a value, keep it; otherwise fall back to address-based value
           accrual_df[col] = accrual_df[extract_col].where(
               accrual_df[extract_col].notna() & (accrual_df[extract_col] != ""),
               accrual_df[col]
           )
       accrual_df.drop(columns=['Consignor Code_extract', 'Consignee Code_extract'], inplace=True)
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
       # ========== FIX: make merge keys bulletproof ==========
       # Normalize both join columns
       if 'Assigned Location Code' in accrual_df.columns:
           accrual_df['Assigned Location Code'] = _canon_code(accrual_df['Assigned Location Code'])
       if 'Loc Code' in complete_location_table.columns:
           complete_location_table['Loc Code'] = _canon_code(complete_location_table['Loc Code'])
       # RHS uniqueness to avoid fan-out merges; fail fast if dup keys exist
       if 'Loc Code' in complete_location_table.columns:
           complete_location_table = complete_location_table.drop_duplicates(subset=['Loc Code'])
       # Optional: quick check for 061R/61R presence (comment out if noisy)
       # print("061R in RHS? ", "061R" in set(complete_location_table['Loc Code'].dropna()))
       # --- Join profit/cost centers from complete location table ---
       accrual_df = accrual_df.merge(
           complete_location_table[['Loc Code', 'Prof_Cntr', 'Cost_Cntr']],
           left_on='Assigned Location Code',
           right_on='Loc Code',
           how='left',
           validate='m:1'  # many accrual rows -> one reference row
       )
       accrual_df.rename(columns={'Prof_Cntr': 'Profit Center EJ', 'Cost_Cntr': 'Cost Center EJ'}, inplace=True)
       # --- Account # EJ rule ---
       accrual_df['Account # EJ'] = accrual_df.apply(
           lambda row: 621000 if 'G59' in str(row.get('Profit Center EJ', ''))
           else (621000 if row.get('Consignee Code') == row.get('Assigned Location Code') else 621020),
           axis=1
       )
       # --- De-dupe on Invoice Number + Paid Amount ---
       if {'Invoice Number', 'Paid Amount'}.issubset(accrual_df.columns):
           accrual_df = accrual_df.drop_duplicates(subset=['Invoice Number', 'Paid Amount'])
       # Automation Accuracy
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
           'Profit Center EJ', 'Cost Center EJ', 'Account # EJ'
       ]
       ordered = [c for c in first_cols if c in accrual_df.columns] + \
                 [c for c in accrual_df.columns if c not in first_cols]
       accrual_df = accrual_df[ordered]
       return accrual_df
