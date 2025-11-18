import requests
import pandas as pd
import json
import psycopg2 as psy

def reader(url):
    response = requests.get(url)
    if response.status_code == 200:
        raw = response.text

        conn = psy.connect(
            dbname="hsr",
            user="postgres",
            password="pointLester",
            host="localhost"
        )

        #DOESNT DEAL WITH NOT JSON OR CSV
        if(raw.startswith("{")):
            data = json.loads(response.text)
            dataF = pd.DataFrame(data)
            dataF.fillna('***')
            #print(dataF.to_string)
            #print("check seperation")
            #print(dataF.isnull().sum())
            
            try:
                dataF.to_sql('my_table', con=conn, if_exists='replace', index=False)
                print("DataFrame successfully written to PostgreSQL.")
            except Exception as e:
                print(f"Error writing DataFrame to PostgreSQL: {e}")

        else:
            print("test: didn't work")
    else:
        print("Status Code: " + str(response.status_code))


poke = "https://pokeapi.co/api/v2/pokemon/charizard"
big = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&outputsize=full&apikey=demo"     

reader(big)

