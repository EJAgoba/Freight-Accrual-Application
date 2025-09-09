import pandas as pd

# access consignor code and consignee code column
# for each cell value, make four digits
class CodeFormatter:
    def pad_codes(self, df, *columns):
        for col in columns:
            mask = df[col].notna()
            df.loc[mask, col] = df.loc[mask, col].astype(str).str.zfill(4).str.upper()
        return df
