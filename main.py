import yaml
from Reader import reader
from Validator import validator
from Cleaner import cleaner
from Loader import loader


with open('sources.yml', 'r') as file:
    cfg = yaml.safe_load(file)
    print('yaml good to go')
    r = reader(cfg)
    csvDF = r.read('tax_csv')    
    print('csv too')
    apiDF = r.read('lead_api')
    print('api is ready as well')
    v = validator(cfg)
    apiDF,invalidAPI = v.validate(apiDF,'lead_api')
    csvDF,invalidCSV = v.validate(csvDF,'tax_csv')
    print(csvDF)
    c = cleaner(cfg)
    c.clean(apiDF)
    l = loader(cfg)
    l.load(apiDF,"lead_levels")
    l.load(csvDF,"tax_levels")
    print('everything is in order')