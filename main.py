import yaml
import Reader
import Validator as val

with open('sources.yml', 'r') as file:
    cfg = yaml.safe_load(file)
    print('yaml good to go')
    csvDF = Reader.csvReader(cfg)
    print('csv too')
    apiDF = Reader.apiReader(cfg)
    print('api is ready as well')
    val.howManyNull(apiDF)
    print('everything is in order')