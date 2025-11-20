import psycopg2 as psy
import pandas as pd
from sqlalchemy import create_engine
import requests
import matplotlib.pyplot as plt
import json


def csvReader() -> pd.DataFrame:
    df = pd.read_csv('iris.csv')

    return df

def trogdorDF() :
    trogdor = {
        'name': ['Trogdor'],
        'title': ['The Burninator'],
        'job': ['Burnination'],
        'hobby': ['Burninating']
    }

    df = pd.DataFrame(trogdor)

    return df

def ibmDF():
    url = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&outputsize=full&apikey=demo"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        imp = []

        for item in data["Time Series (5min)"]:
            imp.append(data["Time Series (5min)"][item])

        df = pd.DataFrame(imp, index=data["Time Series (5min)"])
        df['5. volume'] = df['5. volume'].astype(float)
        df['4. close'] = df['4. close'].astype(float)
        df['3. low'] = df['3. low'].astype(float)
        df['2. high'] = df['2. high'].astype(float)
        df['1. open'] = df['1. open'].astype(float)

        df = df.rename(columns={'1. open': 'open', '2. high': 'high', '3. low': 'low', '4. close': 'close', '5. volume': 'volume'})
        df['date'] = data["Time Series (5min)"].keys()
        df['date'] = df['date'].apply(json.dumps)

        return df
    else:
        return None

def progreSQLTest(df):

    db_connection_str = 'postgresql+psycopg2://postgres:pointLester@localhost:5432/hsr'
    engine = create_engine(db_connection_str)

    conn = psy.connect(
            dbname="hsr",
            user="postgres",
            password="pointLester",
            host="localhost"
    )


    try:
        df.to_sql('ibm', con=engine, if_exists='replace', index=False)
        print("DataFrame successfully written to PostgreSQL.")
    except Exception as e:
        print(f"Error writing DataFrame to PostgreSQL: {e}")

    if conn:
        try:
            cursor = conn.cursor()

            print('test 1')

            cursor.execute("SELECT volume, date FROM ibm WHERE date LIKE '%12:00:00%' ORDER BY date;")

            # Fetch results
            records = cursor.fetchall()  # Fetch all rows
            # records = cursor.fetchone() # Fetch a single row
            # records = cursor.fetchmany(size=20) # Fetch a specified number of rows


            #records.reverse()

            high = [row[0] for row in records]
            date = list(map(lambda x: x[6:-7], [row[1] for row in records]))

            plt.plot(date,high, color='blue')
            plt.title('IBM Stock')
            plt.ylabel('USD (thousands)')
            plt.show()

            #for row in records:
            #    print(row)

        except psy.Error as e:
            print(f"Error executing query: {e}")

        finally:
            # Close the cursor and connection
            if cursor:
                cursor.close()
            if conn:
                conn.close()
                print("PostgreSQL connection closed.")

def ibmPlot(df):
    df.plot(kind='line', x='date', y='low', legend=False, color='red', title='Stocks')
    plt.ylabel('USD (thousands)')
    plt.show()


ibmPlot(ibmDF())

#ibmPlot(ibmDF())