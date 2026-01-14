# "https://test.api.amadeus.com/v2/schedule/flights?carrierCode=TP&flightNumber=487&scheduledDepartureDate=2026-08-01"

#"https://developers.amadeus.com/self-service/apis-docs/guides/developer-guides/developer-tools/postman/#make-our-preconfigured-scenarios"
#https://developers.amadeus.com/self-service/apis-docs/guides/developer-guides/examples/code-example/



### Code examples 
# Connect 
# Install the Python library from https://pypi.org/project/amadeus
from amadeus import Client, ResponseError
import pickle
# a ne pas laisser dans en dur par la suite 
amadeus = Client(
    client_id='sDscUz9lwUhiKcHjxZwYLZlzXYQUXyDZ',
    client_secret='jhPao1h5qUW8bPdb'
)

# Airline Routes 
try:
    '''
    What are the destinations served by the British Airways (BA)?
    '''
    response = amadeus.airline.destinations.get(airlineCode='BA')
    print(response.data)
except ResponseError as error:
    raise error
sample_airline_routes = response.data

with open('data/amadeus_sample_airline_routes.pkl', 'wb') as f:
    pickle.dump(sample_airline_routes, f)


with open('data/amadeus_sample_airline_routes.pkl', 'rb') as f:
    sample_airline_routes = pickle.load(f)



# Airport routes 
try:
    '''
    What are the destinations served by MAD airport?
    '''
    response = amadeus.airport.direct_destinations.get(departureAirportCode='MAD')
    print(response.data)
except ResponseError as error:
    raise error

sample_airport_routes = response.data

with open('data/amadeus_sample_airport_routes.pkl', 'wb') as f:
    pickle.dump(sample_airport_routes, f)

with open('data/amadeus_sample_airport_routes.pkl', 'rb') as f:
    sample_airport_routes = pickle.load(f)


# Flight delay prediction 
# besoin d'avoir un vrai vol 
try:
    '''
    Will there be a delay from BRU to FRA? If so how much delay?
    '''
    response = amadeus.travel.predictions.flight_delay.get(originLocationCode='NCE', destinationLocationCode='IST',
                                                           departureDate='2026-01-12', departureTime='18:20:00',
                                                           arrivalDate='2026-08-01', arrivalTime='22:15:00',
                                                           aircraftCode='321', carrierCode='TK',
                                                           flightNumber='1816', duration='PT31H10M')
    print(response.data)
except ResponseError as error:
    raise error

# Airport on Time performance 
try:
    '''
    Will there be a delay in the JFK airport on the 1st of December?
    '''
    response = amadeus.airport.predictions.on_time.get(
        airportCode='JFK', date='2026-01-13')
    print(response.data)
except ResponseError as error:
    raise error


# On demand flight status 
try:
    '''
    Returns flight status of a given flight
    '''
    response = amadeus.schedule.flights.get(carrierCode='AZ',
                                            flightNumber='319',
                                            scheduledDepartureDate='2022-03-13')
    print(response.data)
except ResponseError as error:
    raise error