import pandas as pd

class CombinedAddress:
    def get_first_word(self, address):
        if pd.isna(address) or not str(address).strip():
            return ''
        return str(address).strip().split()[0]

    def create_combined_address_accrual(self, df, new_column, street_address, city, state):
        df[new_column] = (
            df[street_address].apply(self.get_first_word) +
            df[city].apply(self.get_first_word) +
            df[state].apply(self.get_first_word)
        )