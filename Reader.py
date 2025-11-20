#Reader
import pandas as pd
import requests

url = "https://phl.carto.com/api/v2/sql?q=SELECT%20cartodb_id%20AS%20id,%20zip_code,%20num_screen,%20num_bll_5plus,%20perc_5plus%20FROM%20child_blood_lead_levels_by_zip"

def apiReader(url:str) -> pd.DataFrame:
    response = requests.get(url)
    scode = response.status_code
    if scode == 200:
        data = response.json()["rows"]

        df = pd.DataFrame(data)
        
        return df
    
    else:
        raise requests.exceptions.HTTPError('Failed to retrieve data. Status Code: ' + str(scode))

def csvReader() -> pd.DataFrame:
    df = pd.read_csv("real_estate_tax_balances_zip_code.csv")

    return df

#apiReader(url)