#Cleaner
import pandas as pd
import yaml

class cleaner:

    def __init__(self, cfg: yaml):
        """      
        Args:
            config_path: Path to the YAML configuration file
        """

        self.config = cfg
        self.sources = {src['name']: src for src in self.config.get('sources', [])}
    
    def clean(self, df: pd.DataFrame):
        df.drop_duplicates(inplace=True)


        df['num_bll_5plus'].fillna(2,inplace=True)
        df['perc_5plus'].fillna(df['num_bll_5plus']/df['num_screen']*100,inplace=True)
    
    def cleanlead(self, df: pd.DataFrame):
        df.drop_duplicates(inplace=True)


        df['num_bll_5plus'].fillna(2,inplace=True)
        df['perc_5plus'].fillna(df['num_bll_5plus']/df['num_screen']*100,inplace=True)
    
    def cleantax(self, df: pd.DataFrame):
        df.drop_duplicates(inplace=True)

        # Financial columns that should not be negative
        financial_cols = ['principal', 'interest', 'penalty', 'other', 'balance', 'avg_balance']
        
        for col in financial_cols:
            if col in df.columns:
                # Replace negative values with 0
                df.loc[df[col] < 0, col] = 0
                
                # Fill NaN with 0 for financial columns
                df[col].fillna(0, inplace=True)
        
        # Calculate balance from components if balance is missing or 0
        if all(col in df.columns for col in ['principal', 'interest', 'penalty', 'other', 'balance']):
            # Only recalculate if balance is 0 or NaN but components exist
            mask = ((df['balance'] == 0) | (df['balance'].isna())) & (
                (df['principal'] > 0) | (df['interest'] > 0) | 
                (df['penalty'] > 0) | (df['other'] > 0)
            )
            df.loc[mask, 'balance'] = (
                df.loc[mask, 'principal'] + 
                df.loc[mask, 'interest'] + 
                df.loc[mask, 'penalty'] + 
                df.loc[mask, 'other']
            )
        
        # Calculate avg_balance from balance and num_props if missing
        if all(col in df.columns for col in ['balance', 'num_props', 'avg_balance']):
            mask = (df['avg_balance'].isna() | (df['avg_balance'] == 0)) & (
                df['num_props'] > 0
            )
            df.loc[mask, 'avg_balance'] = (
                df.loc[mask, 'balance'] / df.loc[mask, 'num_props']
            )
        
        # Ensure num_props is at least 1 if there's a balance
        if 'num_props' in df.columns and 'balance' in df.columns:
            mask = (df['balance'] > 0) & ((df['num_props'].isna()) | (df['num_props'] == 0))
            df.loc[mask, 'num_props'] = 1
        
        # Round financial values to 2 decimal places
        for col in financial_cols:
            if col in df.columns:
                df[col] = df[col].round(2)
        

