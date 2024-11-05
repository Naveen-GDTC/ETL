import pandas as pd

class Co2_emi_apiTransform:
    def transform(self, df):
        df["period"] = pd.to_datetime(df["period"])
        return df