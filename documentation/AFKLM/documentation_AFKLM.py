import json
import pandas as pd

with open("features_getflights_AFKLM.json", "r") as dataFile:
    jsonData = json.load(dataFile)
    print(jsonData)

#a = jsonData["paths"]['/flightstatus']["get"]["responses"]["200"]["content"]['application/hal+json']['examples']["flights"]['value']
#pd.json_normalize(a, max_level=100, record_path=["errors"]).transpose().to_csv("errors_normalize.csv") # ok
#pd.json_normalize(a['operationalFlights'], max_level=100).transpose().to_csv("operationalFlights_normalize.csv") # ok
#pd.json_normalize(a['operationalFlights'],record_path=["codeShareRelations"], max_level=100).transpose().to_csv("operationalFlights_normalize_codeShareRelations.csv")# ok
#pd.json_normalize(a['operationalFlights'],record_path=["errors"], max_level=100).transpose().to_csv("operationalFlights_normalize_errors.csv") # ok
#pd.json_normalize(a['operationalFlights'],record_path=["flightLegs"], max_level=100).transpose().to_csv("operationalFlights_normalize_flightLegs.csv") #ok

#impd.json_normalize(jsonData["paths"]['/flightstatus']['get']['parameters'],max_level = 100).to_csv("query_parameters.csv")