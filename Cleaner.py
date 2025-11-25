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
        

