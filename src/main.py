import yaml
import matplotlib.pyplot as plt
import psycopg2 as psy
import logging
from Reader import reader
from Validator import validator
from Cleaner import cleaner
from Loader import loader


def run():
    logger = logging.getLogger("app")
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)

    logger.handlers.clear()
    logger.addHandler(ch)

    with open('config/sources.yml', 'r') as file:
        cfg = yaml.safe_load(file)
    logger.info('yaml good to go')
    r = reader(cfg)
    csvDF = r.read('tax_csv')    
    logger.info('csv too')
    apiDF = r.read('lead_api')
    logger.info('api is ready as well')
    v = validator(cfg)
    apiDF,invalidSchemaAPI, invalidRulesAPI = v.validate(apiDF,'lead_api')
    csvDF,invalidSchemaCSV, invalidRulesCSV = v.validate(csvDF,'tax_csv')
    c = cleaner(cfg)
    c.clean(apiDF)
    l = loader(cfg)
    l.load(apiDF,"lead_levels")
    l.load(csvDF,"tax_levels")

    logger.info('Rows Rejected for violating Schema:')
    logger.info(invalidSchemaAPI)
    logger.info(invalidSchemaCSV)
    logger.info('Rows Rejected for violating Rules:')
    logger.info(invalidRulesAPI)
    logger.info(invalidRulesCSV)



def graphOut():
    logger = logging.getLogger("app")
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)

    logger.handlers.clear()
    logger.addHandler(ch)

    conn = psy.connect(
            dbname="project_1",
            user="postgres",
            password="pointLester",
            host="localhost"
    )

    if conn:
        try:
            cursor = conn.cursor()

            fig, axs = plt.subplots(2)
            fig.suptitle('Compare')
            
            

            cursor.execute("SELECT zip_code, perc_5plus FROM lead_levels;")

            # Fetch results
            records = cursor.fetchall()  # Fetch all rows

            zip1 = [row[0] for row in records]
            perc = [row[1] for row in records]

            axs[0].bar(zip1, perc, color='red')


            cursor.execute("SELECT zip_code, balance FROM tax_levels ORDER BY zip_code;")

            records = cursor.fetchall()  # Fetch all rows

            zip2 = [row[0] for row in records]
            numprop = [row[1] for row in records]

            axs[1].bar(zip2, numprop, color='blue')

            plt.show()


        except psy.Error as e:
            logger.info(f"Error executing query: {e}")

        finally:
            # Close the cursor and connection
            if cursor:
                cursor.close()
            if conn:
                conn.close()
                logger.info("PostgreSQL connection closed.")

run()
graphOut()