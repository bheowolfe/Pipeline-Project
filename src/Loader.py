#Loader
import pandas as pd
import yaml
from sqlalchemy import create_engine

class loader:
    
    def __init__(self, cfg: yaml):
        """      
        Args:
            config_path: Path to the YAML configuration file
        """

        self.defaults = cfg.get('defaults', {})
        self.db_url = self.defaults['db_url']
    
    def load(self, df: pd.DataFrame, name: str):
        engine = create_engine(self.db_url)

        try:
            df.to_sql(name, con=engine, if_exists='replace', index=False)
            print("DataFrame successfully written to PostgreSQL.")
        except Exception as e:
            print(f"Error writing DataFrame to PostgreSQL: {e}")
