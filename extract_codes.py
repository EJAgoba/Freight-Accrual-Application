import re
import numpy as np

class Extractor:
    def create_columns(self, df):
        df[['Consignor Code', 'Consignor Type', 'Consignee Code', 'Consignee Type']] = ""
        df['Consignor Type'] = df['Consignor Type'].astype(str).str.strip().str.upper()
        df['Consignee Type'] = df['Consignee Type'].astype(str).str.strip().str.upper()
        df['Consignor Code'] = df['Consignor Code'].astype(str).str.strip().str.upper()
        df['Consignee Code'] = df['Consignee Code'].astype(str).str.strip().str.upper()

    def lower_columns(self, df, *columns):
        for c in columns:
            if c in df:
                df[c] = df[c].astype(str).str.lower()

    def _normalize_set(self, codes):
        # build a fast membership set for location codes
        return set(str(x).strip().upper() for x in codes if str(x).strip() != '')

    def prefill_from_loc_columns(self, df, location_codes,
                                 org_col='Org Type Code', dest_col='Dest Type Code'):
        """Prefill Consignor/Consignee Code from Org/Dest loc columns if present & valid."""
        locset = self._normalize_set(location_codes)

        has_org = org_col in df.columns
        has_dest = dest_col in df.columns

        if has_org:
            org_vals = df[org_col].astype(str).str.strip().str.upper()
            mask_org_valid = org_vals.isin(locset)
            df.loc[mask_org_valid, 'Consignor Code'] = org_vals

        if has_dest:
            dest_vals = df[dest_col].astype(str).str.strip().str.upper()
            mask_dest_valid = dest_vals.isin(locset)
            df.loc[mask_dest_valid, 'Consignee Code'] = dest_vals

        return has_org or has_dest

    def extract1(self, df, df_column, new_column, location_codes, only_null=False):
        """Your existing extract1 logic; optionally only for rows where new_column is blank."""
        if df_column in df:
            df[df_column] = df[df_column].astype(str).str.lower()

        locset = [re.escape(str(x)).lower() for x in location_codes]
        search_pattern = r'\b(' + '|'.join(locset) + r')\b'  # whole-word match

        base_mask = df[df_column].astype(str).str.contains(search_pattern, case=False, na=False)
        if only_null:
            null_mask = df[new_column].replace('', np.nan).isna()
            mask = base_mask & null_mask
        else:
            mask = base_mask

        # Extract first matching code
        df.loc[mask, new_column] = (
            df.loc[mask, df_column]
              .astype(str)
              .str.extract(search_pattern, expand=False)
              .str.upper()
        )

        # normalize empties to NaN
        df[new_column] = df[new_column].replace('', np.nan)












# import re
# import numpy as np

# class Extractor:
#     def create_columns(self, df):
#         df[['Consignor Code', 'Consignor Type', 'Consignee Code', 'Consignee Type']] = ""
#         df['Consignor Type'] = df['Consignor Type'].astype(str).str.strip().str.upper()
#         df['Consignee Type'] = df['Consignee Type'].astype(str).str.strip().str.upper()
#         df['Consignor Code'] = df['Consignor Code'].astype(str).str.strip().str.upper()
#         df['Consignee Code'] = df['Consignee Code'].astype(str).str.strip().str.upper()
#     def lower_columns(self, df, *columns):
#         for i in columns:
#             df[i] = df[i].astype(str).str.lower()
#     def extract1(self, df, column1, new_column, location_codes):
#         location_patterns = [re.escape(str(x).lower()) for x in location_codes]
    
#         # Create regex pattern to match whole words or exact phrases
#         search_pattern = r'\b(' + '|'.join(location_patterns) + r')\b'  # \b ensures whole word match
        
#         mask = df[column1].astype(str).str.contains(r'cintas|millennium', case = False, na = False)
#         # Extract the first matching location code
#         df.loc[mask, new_column] = df.loc[mask, column1].astype(str).str.extract(search_pattern, expand=False)

#         df[new_column] = df[new_column].replace("", np.nan)

    
