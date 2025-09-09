import pandas as pd
import numpy as np

all_location_codes = pd.read_excel(r"C:\Users\c1354623\OneDrive - Cintas Corporation\Documents\Miscellaneous_Excel_Analysis\Done\New Accrual Logic II\all_location_codes.xlsx")

location_codes = all_location_codes['Codes'].tolist()
