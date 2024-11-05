import pandas as pd

class Ren_cap_api_2Transform:
    def transform(self, df):
        df["period"] = pd.to_datetime(df["period"])
        return df