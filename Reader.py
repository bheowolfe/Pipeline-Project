#Reader
import pandas as pd
import yaml
import requests

url = "https://phl.carto.com/api/v2/sql?q=SELECT%20cartodb_id%20AS%20id,%20zip_code,%20num_screen,%20num_bll_5plus,%20perc_5plus%20FROM%20child_blood_lead_levels_by_zip"

class reader:

    def __init__(self, cfg: yaml):
        """      
        Args:
            config_path: Path to the YAML configuration file
        """

        self.config = cfg
        self.sources = {src['name']: src for src in self.config.get('sources', [])}

    def read(self, source_name: str) -> pd.DataFrame:
        df = pd.DataFrame

        if source_name not in self.sources:
            raise ValueError(f"Source '{source_name}' not found in config")
        
        source_config = self.sources[source_name]
        source_path = self.sources[source_name]['path']
        source_type = self.sources[source_name]['type']


        if source_type == 'api_json':
            df = self.apiReader(source_path)
        elif source_type == 'csv':
            df = self.csvReader(source_path)
        
        return df
        

    def apiReader(self, path: str) -> pd.DataFrame:
        response = requests.get(path)
        scode = response.status_code
        if scode == 200:
            data = response.json()["rows"]

            df = pd.DataFrame(data)
            
            return df
        
        else:
            raise requests.exceptions.HTTPError('Failed to retrieve data. Status Code: ' + str(scode))

    def csvReader(self, path: str) -> pd.DataFrame:
        df = pd.read_csv(path)

        return df
    


    #with open('sources.yml', 'r') as file:
    #    cfg = yaml.safe_load(file)
    #    print(cfg['sources'][0]['path'])
    #    csvReader(cfg)

    #apiReader(url)