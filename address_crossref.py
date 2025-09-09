import pandas as pd

class Merger:
    def merge(self, df, locations, column):
        df.columns = df.columns.str.strip()
        locations.columns = locations.columns.str.strip()

        # Step 1: Filter rows with missing or N.A. values
        mask = (
            df[column].isna() |
            (df[column].astype(str).str.strip() == '') |
            (df[column].astype(str).str.upper().str.strip() == 'N.A.')
        )
        filtered_df = df[mask].copy()
        print(f"→ Found {filtered_df.shape[0]} rows with missing '{column}'")

        # Step 2: Save original index so we can write back later
        filtered_df['original_index'] = filtered_df.index

        # Step 3: Join on the corresponding Combined Address column
        # Step 3: Join on the corresponding Combined Address column
        if 'Consignor' in column:
            join_col = 'Consignor Combined Address'
        elif 'Consignee' in column:
            join_col = 'Consignee Combined Address'
        else:
            raise ValueError("Unknown column type for address matching")

        # Keep only first match from locations_df to avoid many-to-many merge
        locations_df_unique = locations.drop_duplicates(subset='Combined Address', keep='first')

        merged = pd.merge(
            left=filtered_df,
            right=locations_df_unique[['Loc Code', 'Combined Address']],
            left_on=join_col,
            right_on='Combined Address',
            how='inner'
        )

        # # Infer the matching Combined Address column dynamically
        # if 'Consignor' in column:
        #     join_col = 'Consignor Combined Address'
        # elif 'Consignee' in column:
        #     join_col = 'Consignee Combined Address'
        # else:
        #     raise ValueError("Unknown column type for address matching")

        # merged = pd.merge(
        #     left=filtered_df,
        #     right=locations[['Loc Code', 'Combined Address']],
        #     left_on=join_col,
        #     right_on='Combined Address',
        #     how='inner'
        # )
        print(f"→ Merge produced {merged.shape[0]} matched rows for '{column}'")

        # Step 4: Write Loc Code back into the original df[column]
        for _, row in merged.iterrows():
            df.at[row['original_index'], column] = row['Loc Code']

        return df

