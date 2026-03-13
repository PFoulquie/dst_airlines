import os
import json 
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import String, Date, DateTime


# TODO: 
# intégrér du sqlalchemy pour la création de tables afin de définir les clés primaires et secondaires. Exemple : https://stackoverflow.com/questions/19175311/how-to-create-only-one-table-with-sqlalchemy
# On ajoute donc une étape de création de table avant l'ingestion 


# decorator to print function 
def print_func_name(func):
    def wrapper(*args, **kwargs):
        print('Function', func.__name__)
        result = func(*args, **kwargs)
        return result

    return wrapper


def add_missing_columns(df, table):
    # load expected columns 
    with open(f'{os.getcwd()}/1_ingestion/necessary_columns.json') as json_file:
        necessary_columns = json.load(json_file)

    nc_ = necessary_columns[table] # récupère les colonnes obligatoires 

    missing_cols_ = list(set(nc_) - set(df.columns)) # récupère les missing 
    
    if len(missing_cols_)>0:
        df_missing_columns = pd.DataFrame({c: pd.Series() for c in missing_cols_}) # ajoute autant de colonnes vides 
        df = pd.concat([df, df_missing_columns]) # bind 
        print(f"Add {missing_cols_}")

    return df 


@print_func_name
def load_afklm_raw_json(path, json_file):
    print(json_file)
    with open(f"{path}/{json_file}", 'r') as file:
        data = json.load(file)
        data = data['operationalFlights']
        df = pd.json_normalize(data)
        df["flightId"] = df["airline.code"] + "+" + df["flightNumber"].astype(str) # créé le flightID
        df = df.rename(columns={"id":"scheduledFlightId"})
        return df 

@print_func_name
def connect_to_postgre(db_uri):
    # Connect to local postgre instance for dev 
    conn = psycopg2.connect(db_uri)
    cur = conn.cursor()
    engine = create_engine(db_uri)
    return conn, cur, engine

# preparation des champs codeShareRelation et flightLegs
@print_func_name
def prepare_nested_fields(df, field, id_field):
    prepared_df = df[field].explode()

    # On ajoute l'index pour merger l'id par la suite et garder le lien entre les field et l'id
    prepared_df['index'] = prepared_df.index
    prepared_df = pd.concat([prepared_df, prepared_df.index.to_series()], axis = 1)
    prepared_df.columns=[field, "index"]
    tmp = df[[id_field]]
    tmp = pd.concat([tmp, tmp.index.to_series()], axis = 1)
    tmp.columns=[id_field, "index"]
    prepared_df = pd.merge(prepared_df, tmp, on='index', how="left")
    del tmp
    del prepared_df['index']
    tmp = pd.json_normalize(prepared_df[field]) # Unnest le json 
    del prepared_df[field]
    prepared_df = pd.concat([prepared_df, tmp], axis = 1) # merge le unnest json 

    if field == "codeShareRelations":
        prepared_df = add_missing_columns(prepared_df, "codeShareRelation") 
        prepared_df = prepared_df.loc[-prepared_df.marketingFlightNumber.isna()] # retire les lignes sans flightnumber (inutilisables)
        prepared_df["relatedFlightId"] = prepared_df["airline.code"] + "+" + prepared_df["marketingFlightNumber"].astype(int).astype(str) # reproduit l'id pour les codeshare
        prepared_df["codeShareId"] = prepared_df.apply(lambda row: "+".join(sorted([row["flightId"], row["relatedFlightId"]])) ,axis =1 ) # id unique pour la table 
        prepared_df['marketingFlightNumber'] = prepared_df['marketingFlightNumber'].astype(int).astype(str) # convertit la colonne 
    elif field == "flightLegs":
        prepared_df = add_missing_columns(prepared_df, "flightLeg") 
        prepared_df["flightLegId"] = prepared_df["scheduledFlightId"] + "+" + prepared_df["departureInformation.airport.code"] + "+" + prepared_df["arrivalInformation.airport.code"] # créé l'id 
        prepared_df = prepared_df.loc[-prepared_df.flightLegId.isna()] # retire les lignes sans flightLegId (inutilisables)
        #  convert list to values
        prepared_df['irregularity.delayCode'] = prepared_df['irregularity.delayCode'].apply(lambda x:x[0] if len(x)>0 else "")
        prepared_df['irregularity.delayDuration'] = prepared_df['irregularity.delayDuration'].apply(lambda x:x[0] if len(x)>0 else "00")
        prepared_df['irregularity.delayReasonCodePublic'] = prepared_df['irregularity.delayReasonCodePublic'].apply(lambda x:x[0] if len(x)>0 else "")
        prepared_df['irregularity.delayReasonPublicLangTransl'] = prepared_df['irregularity.delayReasonPublicLangTransl'].apply(lambda x:x[0] if len(x)>0 else "")

    return prepared_df

# Fonction pour traiter les trois types de relations 
@print_func_name
def create_relation_df(relationtype, df):
    cols_ = [col for col in df if col.startswith(f'flightRelations.{relationtype}')] # récupère les colonnes de ce type de relations 
    cols_.append("scheduledFlightId") # ajoute l'id 
    # ici la fonction pour vérifier que toutes les colonnes sont là 
    df = add_missing_columns(df, "flightRelation") 

    df_relations = df[cols_]
    df_relations.insert( df_relations.shape[1], "relationType", f"{relationtype}") # ajoute la colonne qui décrit le type 
    try:
        df_relations = df_relations.loc[-df_relations[f'flightRelations.{relationtype}FlightData.id'].isna()] # retire les lignes vides 
        df_relations["flightRelationId"] = df_relations[f"flightRelations.{relationtype}FlightData.id"] + "+" + df_relations["scheduledFlightId"] # id pour les relations 
        df_relations = df_relations[["flightRelationId","scheduledFlightId",f"flightRelations.{relationtype}FlightData.id","relationType"]] 
        df_relations.columns = ["flightRelationId","scheduledFlightId","relatedScheduledFlightId","relationType"]
    except:
        # df vide si des colonnes sont manquantes 
        df_relations = pd.DataFrame({c: pd.Series(dtype=t) for c, t in {'flightRelationId': 'str', 
                                                                        'scheduledFlightId':"str",
                                                                        'relatedScheduledFlightId': 'str',
                                                                          'relationType': 'str'}.items()})

        pass

    return df_relations

@print_func_name
def ingest_scheduledFlight(df, engine):
    # Ingest scheduledFlight table 
    df = add_missing_columns(df, "scheduledFlight")
    df_scheduled_flight = df[['scheduledFlightId','flightNumber','flightScheduleDate','haul','route','flightStatusPublic','airline.code']]
    df_scheduled_flight.columns = ['scheduledFlightId','flightNumber','flightScheduleDate','haul','route','flightStatusPublic','airlineCode']
    df_scheduled_flight.\
        to_sql('scheduledFlight', con=engine, if_exists='append', 
            index_label = ['scheduledFlightId','flightNumber','flightScheduleDate','haul','route','flightStatusPublic','airlineCode'],
            index=False)
@print_func_name
def ingest_flight(df, engine):
    # Ingest flight table 
    df = add_missing_columns(df, "flight")
    flight = df[["flightId","flightNumber","airline.code"]].drop_duplicates() 
    flight.columns = ["flightId",'flightNumber','airlineCode']
    flight.\
        to_sql('flight', con=engine, if_exists='append',index_label = ["flightId",'flightNumber','airlineCode'], index=False)
    # à compléter par les vols de codesharerelations 

@print_func_name
def ingest_airline(df, engine):
    df = add_missing_columns(df, "airline")
    airline = df[["airline.code","airline.name"]].drop_duplicates()
    airline.columns = ["airlineCode","airlineName"]
    airline.to_sql("airline",con=engine,if_exists='append', index=False)

@print_func_name
def ingest_codeShareRelationFlight(prepared_df, engine):
    # Ingest flights in flight table --> possible doublons à traiter ensuite 
    prepared_df = prepared_df[["relatedFlightId", "marketingFlightNumber","airline.code"]].drop_duplicates()
    prepared_df.columns =  ["flightId",'flightNumber','airlineCode']
    prepared_df.\
        to_sql('flight', con=engine, if_exists='append',index_label = ["flightId",'flightNumber','airlineCode'], index=False)
@print_func_name
def ingest_codeShareRelation(prepared_df, engine):
    # Ingest codeShareRelation table 
    prepared_df[["codeShareId" , "flightId","relatedFlightId"]].\
        to_sql('codeShareRelation', con=engine, if_exists='append',index_label = ["codeShareId" , "flightId","relatedFlightId"], index=False)


# Ingest flightRelation table 
@print_func_name
def ingest_flightRelation(df_relations, engine):
    df_relations.\
        to_sql('flightRelation', con=engine, if_exists='append',index_label = ["flightRelationId","scheduledFlightId","relatedScheduledFlightId","relationType"], index=False)

@print_func_name
def ingest_flightLeg(prepared_df, engine):
    prepared_df = prepared_df[["flightLegId",
            "scheduledFlightId",
            "legStatusPublicLangTransl",
            "statusName",
            "passengerCustomsStatus",
            "serviceType",
            "serviceTypeName",
            "restricted",
            "scheduledFlightDuration",
            "departureDateTimeDifference",
            "arrivalDateTimeDifference",
            "timeToArrival",
            "completionPercentage",
            "timeZoneDifference",
            "aircraft.typeCode",
            "departureInformation.airport.places.aerogareCode", 
            "departureInformation.airport.places.gateNumber" ,
            "departureInformation.airport.places.parkingPosition" ,
            "departureInformation.airport.places.parkingPositionCustomStatus", 
            "departureInformation.airport.places.parkingPositionType", 
            "departureInformation.airport.places.pierCode" ,
            "departureInformation.airport.places.terminalCode", 
            "departureInformation.airport.code" ,
            "departureInformation.times.actual" ,
            "departureInformation.times.latestPublished", 
            "departureInformation.times.modified",
            "departureInformation.times.scheduled" ,
            "departureInformation.airport.places.boardingContactType", 
            "departureInformation.airport.places.boardingPier ",
            "departureInformation.airport.places.boardingTerminal", 
            "departureInformation.airport.places.checkInAerogare", 
            "departureInformation.airport.places.checkInZone", 
            "departureInformation.airport.places.departureTerminal" ,
            "departureInformation.airport.places.paxDepartureGate" ,
            "departureInformation.boardingTimes.actualBoardingOpen", 
            "departureInformation.boardingTimes.firstPaxBoarding", 
            "departureInformation.boardingTimes.gateCloseTime" ,
            "departureInformation.boardingTimes.plannedBoardingTime", 
            "departureInformation.times.actualTakeOffTime", 
            "departureInformation.times.estimatedPublic" ,
            "departureInformation.times.estimatedTakeOffTime",
            "arrivalInformation.airport.places.aerogareCode", 
            "arrivalInformation.airport.places.gateNumber" ,
            "arrivalInformation.airport.places.parkingPosition" ,
            "arrivalInformation.airport.places.parkingPositionCustomStatus", 
            "arrivalInformation.airport.places.parkingPositionType", 
            "arrivalInformation.airport.places.pierCode" ,
            "arrivalInformation.airport.places.terminalCode" ,
            "arrivalInformation.airport.code" ,
            "arrivalInformation.times.actual" ,
            "arrivalInformation.times.latestPublished", 
            "arrivalInformation.times.modified" ,
            "arrivalInformation.times.scheduled", 
            "arrivalInformation.airport.places.arrivalHall" ,
            "arrivalInformation.airport.places.arrivalPositionPier" ,
            "arrivalInformation.airport.places.arrivalPositionTerminal", 
            "arrivalInformation.airport.places.arrivalTerminal" ,
            "arrivalInformation.airport.places.baggageBelt" ,
            "arrivalInformation.airport.places.disembarkingAerogare" ,
            "arrivalInformation.airport.places.disembarkingBusQuantity" ,
            "arrivalInformation.airport.places.disembarkingContactType", 
            "arrivalInformation.airport.places.expectedBagOnBeltTime", 
            "arrivalInformation.airport.places.firstBagOnBeltTime" ,
            "arrivalInformation.airport.places.lastBagOnBeltTime" ,
            "arrivalInformation.times.actualTouchDownTime" ,
            "arrivalInformation.times.estimated.value" ,
            "arrivalInformation.times.estimatedArrival" ,
            "arrivalInformation.times.estimatedTouchDownTime"
    ]]

    prepared_df.columns = ["flightLegId",
            "scheduledFlightId",
            "legStatus",
            "status",
            "passengerCustomStatus",
            "serviceType",
            "serviceTypeName",
            "restricted",
            "scheduledFlightDuration",
            "departureDateTimeDifference",
            "arrivalDateTimeDifference",
            "timeToArrival",
            "completionPercentage",
            "timeZoneDifference",
            "aircraftCode",
            "departureAerogareCode" ,
            "departureGateNumber" ,
            "departureParkingPosition", 
            "departureParkingPositionCustomStatus" ,
            "departureParkingPositionType" ,
            "departurePierCode" ,
            "departureTerminalCode", 
            "departureAirportCode" ,
            "departureActualTime"  ,
            "departureLatestPublishedTime",  
            "departureModifiedTime"  ,
            "departureScheduledTime"  ,
            "departureBoardingContactType", 
            "departureBoardingPier" ,
            "departureBoardingTerminal", 
            "departureCheckInAerogare" ,
            "departureCheckInZone" ,
            "departureTerminal" ,
            "departurePaxDepartureGate", 
            "departureActualBoardingOpen", 
            "departureFirstPaxBoarding" ,
            "departureGateCloseTime" ,
            "departurePlannedBoardingTime", 
            "departureActualTakeOffTime" ,
            "departureEstimatedPublic" ,
            "departureEstimatedTakeOffTime", 
            "arrivalAerogareCode", 
    "arrivalGateNumber" ,
    "arrivalParkingPosition" ,
    "arrivalParkingPositionCustomStatus", 
    "arrivalParkingPositionType", 
    "arrivalPierCode" ,
    "arrivalTerminalCode" ,
    "arrivalAirportCode" ,
    "arrivalActualTime" ,
    "arrivalLatestPublishedTime", 
    "arrivalModifiedTime" ,
    "arrivalScheduledTime", 
    "arrivalHall" ,
    "arrivalPositionPier" ,
    "arrivalPositionTerminal", 
    "arrivalTerminal" ,
    "arrivalBaggageBelt" ,
    "arrivalDisembarkingAerogare" ,
    "arrivalDisembarkingBusQuantity" ,
    "arrivalDisembarkingContactType", 
    "arrivalExpectedBagOnBeltTime", 
    "arrivalFirstBagOnBeltTime" ,
    "arrivalLastBagOnBeltTime" ,
    "arrivalActualTouchDownTime" ,
    "arrivalEstimated" ,
    "arrivalEstimatedArrival" ,
    "arrivalEstimatedTouchDownTime", 
            ]

    prepared_df.\
        to_sql('flightLeg', con=engine, if_exists='append',index=False)

@print_func_name
def ingest_airport(prepared_df, engine):
    # On joint les infos sur les airports de départs et arrivées 
    prepared_df = add_missing_columns(prepared_df, "airport")

    df_departure_airport = prepared_df[[
        "departureInformation.airport.code",
        "departureInformation.airport.name",
        "departureInformation.airport.location.latitude",
        "departureInformation.airport.location.longitude",
        "departureInformation.airport.city.code"
                                    ]]
    df_arrival_airport = prepared_df[[
        "arrivalInformation.airport.code",
        "arrivalInformation.airport.name",
        "arrivalInformation.airport.location.latitude",
        "arrivalInformation.airport.location.longitude",
        "arrivalInformation.airport.city.code"
                                    ]]
    df_departure_airport.columns = [
    "airportCode",
    "airportName", 
    "latitude" ,
    "longitude", 
    "cityCode", 
    ]
    df_arrival_airport.columns = [
    "airportCode",
    "airportName", 
    "latitude" ,
    "longitude", 
    "cityCode", 
    ]
    df_airport = pd.concat([df_departure_airport,df_arrival_airport],axis=0).drop_duplicates()

    df_airport.\
        to_sql('airport', con=engine, if_exists='append',
            index_label = [
    "airportCode",
    "airportName", 
    "latitude" ,
    "longitude", 
    "cityCode", 
    ], 
            index=False)

@print_func_name   
def ingest_city(prepared_df, engine):
        # On joint les infos sur les prepared_df de départs et arrivées 
    prepared_df = add_missing_columns(prepared_df, "city") 

    df_departure_city = prepared_df[[
        "departureInformation.airport.city.code",
        "departureInformation.airport.city.name",
        "departureInformation.airport.city.country.code"
                                    ]]
    df_arrival_city = prepared_df[[
            "arrivalInformation.airport.city.code",
        "arrivalInformation.airport.city.name",
        "arrivalInformation.airport.city.country.code"

                                    ]]
    df_departure_city.columns = [
    "cityCode",
    "cityName", 
    "countryCode" 
    ]
    df_arrival_city.columns = [
    "cityCode",
    "cityName", 
    "countryCode" 
    ]

    df_city = pd.concat([df_departure_city,df_arrival_city],axis=0).drop_duplicates()

    df_city.\
        to_sql('city', con=engine, if_exists='append',
            index_label = [
    "cityCode",
    "cityName", 
    "countryCode" 
    ], 
            index=False)

@print_func_name
def ingest_country(prepared_df, engine):

    # On joint les infos sur les countries de départs et arrivées 
    prepared_df = add_missing_columns(prepared_df, "country") 


    df_departure_country = prepared_df[[
        "departureInformation.airport.city.country.code",
        "departureInformation.airport.city.country.name",
    "departureInformation.airport.city.country.euroCountry",
    "departureInformation.airport.city.country.euCountry",
    "departureInformation.airport.city.country.areaCode"
                                    ]]
    df_arrival_country = prepared_df[[
        "arrivalInformation.airport.city.country.code",
        "arrivalInformation.airport.city.country.name",
    "arrivalInformation.airport.city.country.euroCountry",
    "arrivalInformation.airport.city.country.euCountry",
    "arrivalInformation.airport.city.country.areaCode"
                                    ]]
    df_departure_country.columns = [
    "countryCode" ,
    "countryName",
        "euroCountry" ,
    "euCountry" ,
    "areaCode" 
    ]
    df_arrival_country.columns = [
    "countryCode" ,
    "countryName",
        "euroCountry" ,
    "euCountry" ,
    "areaCode" 
    ]

    df_country = pd.concat([df_departure_country,df_arrival_country],axis=0).drop_duplicates()

    df_country.\
        to_sql('country', con=engine, if_exists='append',
            index_label = [
    "countryCode" ,
    "countryName",
        "euroCountry" ,
    "euCountry" ,
    "areaCode" 
    ], 
            index=False)

@print_func_name
def ingest_aircraft(prepared_df, engine):
    prepared_df = add_missing_columns(prepared_df, "aircraft") 

    df_aircraft = prepared_df[[
    "aircraft.typeCode",
    "aircraft.typeName",
    "aircraft.ownerAirlineCode",
    "aircraft.physicalPaxConfiguration",
    "aircraft.physicalFreightConfiguration",
    "aircraft.operationalConfiguration",
    "aircraft.cockpitCrewEmployer",
    "aircraft.cabinCrewEmployer",
    "aircraft.registration",
    "aircraft.saleableConfiguration",
    "aircraft.subFleetCodeId",
    "aircraft.typeCode",
    "aircraft.typeName",
    ]].drop_duplicates()
    

    df_aircraft.columns = [
    "aircraftCode" ,
    "aircraftName" ,
    "ownerAirlineCode", 
    "physicalPaxConfiguration", 
    "physicalFreightConfiguration", 
    "operationalConfiguration" ,
    "cockpitCrewEmployerCode" ,
    "cabinCrewEmployerCode" ,
    "registration" ,
    "saleableConfiguration" ,
    "subFleetCodeId" ,
    "typeCode" ,
    "typeName" 
    ]

    df_aircraft.\
        to_sql('aircraft', con=engine, if_exists='append',
            index_label = [
    "aircraftCode" ,
    "aircraftName" ,
    "ownerAirlineCode", 
    "physicalPaxConfiguration", 
    "physicalFreightConfiguration", 
    "operationalConfiguration" ,
    "cockpitCrewEmployerCode" ,
    "cabinCrewEmployerCode" ,
    "registration" ,
    "saleableConfiguration" ,
    "subFleetCodeId" ,
    "typeCode" ,
    "typeName" 
    ], 
            index=False)
    
@print_func_name
def ingest_irregularity(prepared_df, engine):
    prepared_df = add_missing_columns(prepared_df, "irregularity") 
    df_irregularity = prepared_df[["flightLegId",
    'irregularity.cancelled',
    "irregularity.cancellationReasonCodePublic",
    'irregularity.delayCode',
    "irregularity.delayDuration"]].drop_duplicates()
    df_irregularity.columns = ["flightLegId","cancelled","cancellationReasonCode","delayCode","delayDuration"]
    df_irregularity.\
        to_sql('irregularity', con=engine, if_exists='append',
            index_label = ["flightLegId","cancelled","cancellationReasonCode","delayCode","delayDuration"], 
            index=False)
    

@print_func_name
def ingest_delay(prepared_df, engine):
    prepared_df = add_missing_columns(prepared_df, "delay") 
    df_delay = prepared_df[['irregularity.delayCode',"irregularity.delayReasonCodePublic"]].drop_duplicates() # attention, voir si c'est unique
    df_delay.columns = ["delayCode","delayReasonCode"]
    df_delay.\
        to_sql('delay', con=engine, if_exists='append',
            index_label = ["delayCode","delayReasonCode"],
            index=False)

@print_func_name
def ingest_delayReason(prepared_df, engine):
    prepared_df = add_missing_columns(prepared_df, "delayReason")
    df_delayreason = prepared_df[["irregularity.delayReasonCodePublic", 
                "irregularity.delayReasonPublicLangTransl"]].drop_duplicates()
    df_delayreason.columns = ["delayReasonCode", "delayReason"]
    df_delayreason.\
        to_sql('delayReason', con=engine, if_exists='append',
            index_label = ["delayReasonCode", "delayReason"],
            index=False)

@print_func_name
def ingest_cancelReason(prepared_df, engine):
    prepared_df = add_missing_columns(prepared_df, "cancellationReason") 
    df_cancelreason = prepared_df[["irregularity.cancellationReasonCodePublic",
    "irregularity.cancellationReasonPublicShort",
    "irregularity.cancellationReasonPublicLong"]].drop_duplicates()
    df_cancelreason.columns = ["cancellationReasonCode","cancellationReasonShort","cancellationReasonLong"]

    df_cancelreason.\
        to_sql('cancelReason', con=engine, if_exists='append',
            index_label = ["cancellationReasonCode","cancellationReasonShort","cancellationReasonLong"],
            index=False)
    


def main(data_path):

    db_uri =  f"postgresql://pierre:data@localhost:5432/dst_db"
    conn, cur, engine = connect_to_postgre(db_uri)

    # load json data
    files_ = os.listdir(data_path)

    for json_file in files_:
    #for i in range(2): # for tests 
     #   json_file = files_[i]
        df = load_afklm_raw_json(data_path, json_file)
    
        # Preparation 
        prepared_df_codeShareRelation = prepare_nested_fields(df, field = "codeShareRelations", id_field = 'flightId')
        prepared_df_flightLeg = prepare_nested_fields(df, field = "flightLegs", id_field = "scheduledFlightId")
        df_relations_previous = create_relation_df("previous",df)
        df_relations_equivalent = create_relation_df("equivalent",df)
        df_relations_onward = create_relation_df("onward",df)

        # Ingestion 
        ingest_scheduledFlight(df, engine)
        ingest_flight(df, engine)
        ingest_airline(df, engine)
        ingest_codeShareRelationFlight(prepared_df_codeShareRelation, engine)
        ingest_codeShareRelation(prepared_df_codeShareRelation, engine)
        ingest_flightRelation(df_relations_previous, engine)
        ingest_flightRelation(df_relations_equivalent, engine)
        ingest_flightRelation(df_relations_onward, engine)
        ingest_flightLeg(prepared_df_flightLeg, engine)
        ingest_airport(prepared_df_flightLeg, engine)
        ingest_city(prepared_df_flightLeg, engine)
        ingest_country(prepared_df_flightLeg, engine)
        ingest_aircraft(prepared_df_flightLeg, engine)
        ingest_irregularity(prepared_df_flightLeg, engine)
        ingest_delay(prepared_df_flightLeg, engine)
        ingest_delayReason(prepared_df_flightLeg, engine)
        ingest_cancelReason(prepared_df_flightLeg, engine)

    

if __name__ == '__main__':
    # execute only if run as the entry point into the program
    data_path = "/home/pierre/Documents/DE_DATASCIENTEST/data_projet_dst_airlines/afklm_large"
    main(data_path)

