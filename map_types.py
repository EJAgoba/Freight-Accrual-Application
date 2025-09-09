import pandas as pd
import numpy as np

"""The script defines two classes — TypeMapper and TypeCleaner — to clean and enrich logistics-related data in a pandas DataFrame.

TypeMapper standardizes location codes by formatting them as 4-digit uppercase strings, merges type information from a reference DataFrame (locations), and fills missing or invalid type entries (like blanks or 'N.A.') with the correct type based on location codes.

TypeCleaner further cleans specified columns by replacing empty or invalid entries with 'NON-CINTAS', ensuring consistency across the dataset.

Together, these tools help maintain clean, complete, and standardized data for logistics operations, especially when dealing with location-based type classifications."""

class TypeMapper:
    def map_types(self, df, locations, code_column, type_column):
        # Step 1: Format codes ONLY if not NaN
        code_mask = df[code_column].notna()
        df.loc[code_mask, code_column] = df.loc[code_mask, code_column].astype(str).str.zfill(4).str.upper()

        loc_mask = locations['Loc Code'].notna()
        locations.loc[loc_mask, 'Loc Code'] = locations.loc[loc_mask, 'Loc Code'].astype(str).str.zfill(4).str.upper()

        # Step 2: Rename for merge
        renamed_locations = locations[['Loc Code', 'Type_Code']].rename(columns={
            'Loc Code': code_column,
            'Type_Code': type_column + '_new'
        })

        # Step 3: Merge
        merged = pd.merge(df, renamed_locations, on=code_column, how='left')

        # Step 4: Fill only if original value is missing, blank, or 'N.A.'
        mask = (
            merged[type_column].isna() |
            (merged[type_column].astype(str).str.strip() == '') |
            (merged[type_column].astype(str).str.upper().str.strip() == 'N.A.')
        )
        fill_count = mask.sum()
        merged.loc[mask, type_column] = merged.loc[mask, type_column + '_new']

        print(f"→ Filled {fill_count} missing or blank '{type_column}' using '{code_column}'")

        # Step 5: Drop helper column and return clean result
        merged.drop(columns=[type_column + '_new'], inplace=True)
        return merged
class TypeCleaner:
    def fill_non_cintas(self, df, *columns):
        for col in columns:
            df[col] = df[col].replace(['', ' ', 'N.A.', 'n.a.'], np.nan)
            df[col] = df[col].fillna('NON-CINTAS')
        return df
