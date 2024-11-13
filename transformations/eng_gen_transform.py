import pandas as pd


class Eng_genTransform:
    def transform(self, df):
        WAEF = self.WAEF_cal(df)
        df['co2_emission_tons'] = df.apply(self.fueltype_calculation, axis=1)
        df['co2_reduction_tons']= df.apply(self.co2_reduction_cal,axis=1,multiplier=WAEF)
        return df
    

    def fueltype_calculation(self,row):
        """
        Calculates the adjusted energy generation based on the fuel type.

        Parameters:
        - row (pd.Series): A row from a DataFrame containing 'generation_MWh' 
        and 'fueltype' columns.

        Returns:
        - float: The adjusted generation value based on the fuel type.
        """
        value = float(row['generation_MWh'])  
        if row['fueltype'] == 'COL':
            return value * 1.03
        elif row['fueltype'] == 'NG':
            return value * 0.42
        elif row['fueltype'] == 'PE':
            return value * 0.93
        else:
            return 0
        

    def WAEF_cal(self,eng_gen):
        """
        Calculates the Weighted Average Emission Factor (WAEF) for non-renewable 
        energy generation based on fuel types.

        Parameters:
        - eng_gen (pd.DataFrame): A DataFrame containing energy generation data 
        with columns 'fueltype' and 'generation_MWh'.

        Returns:
        - float: The weighted average emission factor for coal, natural gas, 
        and oil, adjusted for their respective efficiencies
        """
        non_renew_mwh = sum(eng_gen[eng_gen['fueltype'].isin(['COL','NG','OIL'])]['generation_MWh'])
        col_ef = round((sum(eng_gen[eng_gen['fueltype']=='COL']['generation_MWh'])/non_renew_mwh)*1.03,4)
        ng_ef = round((sum(eng_gen[eng_gen['fueltype']=='NG']['generation_MWh'])/non_renew_mwh)*0.42,4)
        oil_ef = round((sum(eng_gen[eng_gen['fueltype']=='OIL']['generation_MWh'])/non_renew_mwh)*0.93,4)
        return col_ef+ng_ef+oil_ef
    

    def co2_reduction_cal(self,row,multiplier): # T
        """
        Calculates the potential CO2 reduction based on energy generation 
        from renewable sources.

        Parameters:
        - row (pd.Series): A row from a DataFrame containing 'generation_MWh' 
        and 'fueltype' columns.
        - multiplier (float): A factor representing the amount of CO2 reduction 
        per megawatt-hour of energy generated.(Weighted average emission factor)

        Returns:
        - float: The calculated CO2 reduction for the given row.
        """
        value = float(row['generation_MWh'])  
        if row['fueltype'] in ['WAT','SUN','WND']:
            return value * multiplier
        else:
            return 0