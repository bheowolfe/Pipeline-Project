#Validator
import json
from jsonschema import validate

leadSchema = {
              "type" : "object",
              "properties" : {
                "id":{"type":"number","pgtype":"int8"},
                "zip_code":{"type":"number","pgtype":"int4"},
                "num_screen":{"type":"number","pgtype":"int4"},
                "num_bll_5plus":{"type":"number","pgtype":"int4"},
                "perc_5plus":{"type":"number","pgtype":"float8"},
                "data_redacted":{"type":"boolean","pgtype":"bool"}
              },
              "required" : ["id","zip_code"]
             }

taxSchema = {
              "type" : "object",
              "properties" : {
                "objectid":{"type":"number","pgtype":"int8"},
                "zip_code":{"type":"number","pgtype":"int4"},
                "num_props":{"type":"number","pgtype":"int4"},
                "min_period":{"type":"number","pgtype":"int4"},
                "max_period":{"type":"number","pgtype":"int4"},
                "principal":{"type":"number","pgtype":"float32"},
                "interest":{"type":"number","pgtype":"float32"},
                "penalty":{"type":"number","pgtype":"float32"},
                "other":{"type":"number","pgtype":"float32"},
                "balance":{"type":"number","pgtype":"float32"},
                "avg_balance":{"type":"number","pgtype":"float32"}
              },
              "required" : ["objectid","zip_code"]
}



