import psycopg2 as psy
import pandas as pd
from SQLAlchemy import create_engine




def progreSQLTest():
    trogdor = {
        'name': ['Trogdor'],
        'title': ['The Burninator'],
        'job': ['Burnination'],
        'hobby': ['Burninating']
    }

    df = pd.DataFrame(trogdor)

    db_connection_str = 'postgresql+psycopg2://postgres:pointLester@localhost:5432/hsr'
    engine = create_engine(db_connection_str)

    #conn = psy.connect(
    #        dbname="hsr",
    #        user="postgres",
    #        password="pointLester",
    #        host="localhost"
    #)

    try:
        df.to_sql('my_table', con=conn, if_exists='replace', index=False)
        print("DataFrame successfully written to PostgreSQL.")
    except Exception as e:
        print(f"Error writing DataFrame to PostgreSQL: {e}")

progreSQLTest()