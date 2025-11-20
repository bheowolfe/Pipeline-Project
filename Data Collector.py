import requests
import pandas as pd
import json
import psycopg2 as psy
from sqlalchemy import create_engine

def reader(url):
    response = requests.get(url)
    if response.status_code == 200:
        raw = response.json()["rows"]

        conn = psy.connect(
            dbname="hsr",
            user="postgres",
            password="pointLester",
            host="localhost"
        )

        db_connection_str = 'postgresql+psycopg2://postgres:pointLester@localhost:5432/hsr'
        engine = create_engine(db_connection_str)

        #DOESNT DEAL WITH NOT JSON OR CSV
        if(True):
            #data = json.loads(str(raw))
            #print("JSON All CLear")
            dataF = pd.DataFrame(raw)
            print("Pandas all CLear")
            #dataF.fillna('***')
            #print(dataF.to_string)
            #print("check seperation")
            #print(dataF.isnull().sum())
            
            try:
                dataF.to_sql('my_table', con=engine, if_exists='replace', index=False)
                print("DataFrame successfully written to PostgreSQL.")
            except Exception as e:
                print(f"Error writing DataFrame to PostgreSQL: {e}")

        else:
            print("test: didn't work")
    else:
        print("Status Code: " + str(response.status_code))


poke = "https://pokeapi.co/api/v2/pokemon/charizard"
big = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&outputsize=full&apikey=demo"     
Gov = "http://api.census.gov/data/2021/pep/population"
lead = "https://phl.carto.com/api/v2/sql?q=SELECT%20cartodb_id,%20zip_code,%20num_screen,%20num_bll_5plus,%20perc_5plus%20FROM%20child_blood_lead_levels_by_zip"
reader(lead)

