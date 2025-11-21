#Validator
import pandas as pd
import Reader
import yaml


def howManyNull(df):
    print("Missing values per column:")
    print(df.isnull().sum())

def validateAPI(df: pd.DataFrame):
    print("Finish validator for API")

def validateCSV(df: pd.DataFrame):
    print("Finish validator for csv")



#with open('sources.yml', 'r') as file:
#    cfg = yaml.safe_load(file)
#    howManyNull(Reader.csvReader(cfg))