import pandas as pd

from extract_codes import Extractor

from address_merge import CombinedAddress

from address_crossref import Merger

from clean_codes import CodeFormatter

from map_types import TypeMapper, TypeCleaner

from matrix_map import MatrixMapper


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

        extractor.extract1(

            accrual_df,

            'Consignor',

            'Consignor Code',

            location_codes,

            only_null=True,

        )

        extractor.extract1(

            accrual_df,

            'Consignee',

            'Consignee Code',

            location_codes,

            only_null=True,

        )

        # --- Combined Address ---

        combined_address = CombinedAddress()

        combined_address.create_combined_address_accrual(

            cintas_location_table,

            'Combined Address',

            'Loc_Address',

            'Loc_City',

            'Loc_ST',

        )

        combined_address.create_combined_address_accrual(

            accrual_df,

            'Consignee Combined Address',

            'Dest Address1',

            'Dest City',

            'Dest State Code',

        )

        combined_address.create_combined_address_accrual(

            accrual_df,

            'Consignor Combined Address',

            'Origin Addresss',

            'Origin City',

            'Origin State Code',

        )

        cintas_location_table['Combined Address'] = (

            cintas_location_table['Combined Address']

            .astype(str)

            .str.upper()

        )

        accrual_df['Consignee Combined Address'] = (

            accrual_df['Consignee Combined Address']

            .astype(str)

            .str.upper()

        )

        accrual_df['Consignor Combined Address'] = (

            accrual_df['Consignor Combined Address']

            .astype(str)

            .str.upper()

        )

        # --- Cross reference the combined address ---

        merger = Merger()

        accrual_df = merger.merge(accrual_df, cintas_location_table, 'Consignor Code')

        accrual_df = merger.merge(accrual_df, cintas_location_table, 'Consignee Code')

        # --- Clean up the codes ---

        formatter = CodeFormatter()

        accrual_df = formatter.pad_codes(accrual_df, 'Consignor Code', 'Consignee Code')

        # --- Populate Type Codes ---

        type_mapper = TypeMapper()

        accrual_df = type_mapper.map_types(

            accrual_df,

            cintas_location_table,

            'Consignor Code',

            'Consignor Type',

        )

        accrual_df = type_mapper.map_types(

            accrual_df,

            cintas_location_table,

            'Consignee Code',

            'Consignee Type',

        )

        cleaner = TypeCleaner()

        accrual_df = cleaner.fill_non_cintas(

            accrual_df,

            'Consignor Type',

            'Consignee Type',

        )

        # --- Matrix mapping for Assigned Location Code ---

        matrix_mapper = MatrixMapper()

        accrual_df['Assigned Location Code'] = accrual_df.apply(

            matrix_mapper.determine_profit_center,

            axis=1,

        )

        # --- Join profit/cost centers from complete location table ---

        accrual_df = accrual_df.merge(

            complete_location_table[['Loc Code', 'Prof_Cntr', 'Cost_Cntr']],

            left_on='Assigned Location Code',

            right_on='Loc Code',

            how='left',

        )

        accrual_df.rename(

            columns={

                'Prof_Cntr': 'Profit Center EJ',

                'Cost_Cntr': 'Cost Center EJ',

            },

            inplace=True,

        )

        # --- Account # EJ rule ---

        accrual_df['Account # EJ'] = accrual_df.apply(

            lambda row: 621000

            if 'G59' in str(row.get('Profit Center EJ', ''))

            else (

                621000

                if row.get('Consignee Code') == row.get('Assigned Location Code')

                else 621020

            ),

            axis=1,

        )

        # --- De-dupe on Invoice Number + Paid Amount (only if both exist) ---

        if {'Invoice Number', 'Paid Amount'}.issubset(accrual_df.columns):

            accrual_df = accrual_df.drop_duplicates(

                subset=['Invoice Number', 'Paid Amount']

            )

        # --- Automation Accuracy (NA-safe) ---

        if {'Profit Center', 'Profit Center EJ'}.issubset(accrual_df.columns):

            pc = accrual_df['Profit Center']

            pc_ej = accrual_df['Profit Center EJ']

            # True when both not NA and equal

            match = pc == pc_ej

            cond = match & pc.notna() & pc_ej.notna()

            # NA → False → 0

            accrual_df['Automation Accuracy'] = cond.fillna(False).astype(int)

        else:

            accrual_df['Automation Accuracy'] = 0

        # --- Column ordering (if present) ---

        first_cols = [

            'Profit Center',

            'Cost Center',

            'Account #',

            'Automation Accuracy',

            'Profit Center EJ',

            'Cost Center EJ',

            'Account # EJ',

        ]

        ordered = (

            [c for c in first_cols if c in accrual_df.columns]

            + [c for c in accrual_df.columns if c not in first_cols]

        )

        accrual_df = accrual_df[ordered]

        return accrual_df


# Optional: keep a function version for backwards compatibility

def run_pipeline(

    accrual_df: pd.DataFrame,

    cintas_location_table: pd.DataFrame,

    complete_location_table: pd.DataFrame,

    location_codes: list[str],

) -> pd.DataFrame:

    """Function wrapper so you can still call run_pipeline(...) if you want."""

    runner = PipelineRunner()

    return runner.run(accrual_df, cintas_location_table, complete_location_table, location_codes)
 
