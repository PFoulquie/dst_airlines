#"https://developer.airfranceklm.com/products/api/flightstatus/api-reference/operations/getFlights"
import requests
import json 
import pickle 
import os 

headers = {
    'API-Key': 'mrhtwrh8kgreevzgfb8arb5w' ,
    'Accept': 'application/hal+json'
}


# Get Flights endpoint
startrange_ = "2026-01-01T09%3A00%3A00.000Z"
endrange_ = "2026-03-01T23%3A59%3A59.000Z"
response = requests.get(url=f"https://api.airfranceklm.com/opendata/flightstatus?startRange={startrange_}&endRange={endrange_}", headers=headers)
af_sample_flight_status = response.content

with open('data/af_sample_flight_status.pkl', 'wb') as f:
    pickle.dump(af_sample_flight_status, f)

with open('data/af_sample_flight_status.pkl', 'rb') as f:
    af_sample_flight_status = pickle.load(f)


dict_af_sample_flight_status = json.loads(af_sample_flight_status.decode('utf-8'))
list_af_sample_flight_status = [f_ for f_ in dict_af_sample_flight_status['operationalFlights']]
# list of dict 


# Get Flight Status details 
#Identification of a specific flight with format: yyyyMMdd+carrierCode+flightNumber+operationalSuffixWhere flightNumber must be 4 digit in length
id_ = list_af_sample_flight_status[1]['id']
response = requests.get(url=f"https://api.airfranceklm.com/opendata/flightstatus{id_}", headers=headers)
response.content
