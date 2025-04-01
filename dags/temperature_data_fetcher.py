import json
import logging
import requests
import time
from kafka import KafkaProducer

def fetch_temperature():
    """
    Haalt de actuele luchttemperatuurgegevens op uit de API en koppelt deze aan de juiste meetstations.
    De gegevens worden gestructureerd in een lijst met relevante informatie per station.
    """
    
    try:
        # Ophalen van de data vanuit de API
         response = requests.get("https://api.data.gov.sg/v1/environment/air-temperature")
         
         # Controleer of de response succesvol is (HTTP status 200)
         if response.status_code != 200:
             logging.error(f"Fout bij ophalen data via API: {response.status_code}")
             return []  # Retourneer een lege lijst als de API-aanroep mislukt
         
         # Als de response succesvol is, ga dan verder met het verwerken van de data
         data = response.json()

    except requests.exceptions.RequestException as e:
        # Fout bij de verzoeken (bijv. netwerkproblemen of andere uitzonderingen)
        logging.error(f"Netwerkproblemen of andere uitzonderingen bij het ophalen van data via API: {e}")
        return []  # Retourneer een lege lijst bij netwerkproblemen of andere uitzonderingen
    
    # Lijst met beschikbare meetstations en hun locatiegegevens
    stations = data['metadata']['stations']

    # Lijst met temperatuurmetingen per station
    readings = data["items"][0]["readings"]

    # Tijdstip waarop de metingen zijn gedaan
    timestamp = data["items"][0]["timestamp"]

    # Lijst waarin de gestructureerde meetgegevens worden opgeslagen
    structured_data = []
    
    # Doorloop alle temperatuurmetingen
    for reading in readings:
        
        # Haal het station_id en de bijbehorende temperatuur op
        station_id = reading["station_id"]
        temperature = reading["value"]
        
        # Zoek het bijbehorende meetstation op basis van station_id
        for station in stations:
            if station_id == station["id"]:
                # Opslaan van de locatiegegevens en naam van het station
                name = station["name"]
                latitude = station["location"]['latitude']
                longitude = station["location"]['longitude']
                
                # Stop de loop zodra het juiste station is gevonden
                break
        
        # Sla de gecombineerde gegevens op in de lijst
        structured_data.append({
            "station_id": station_id,
            "temperature": temperature,
            "name": name,
            "latitude": latitude,
            "longitude": longitude,
            "timestamp": timestamp
        })

    return structured_data


def json_serializer(data):
    # Automatische JSON conversie van berichten naar Kafka
    return json.dumps(data).encode('utf-8')


def stream_data():
    """
    Streamt temperatuurdata naar een Kafka-topic voor een duur van 1 minuut, dit gebeurt via Airflow
    """
    
    # De Kafka Producer stuurt data naar de Kafka-topic, zodat Consumers deze kunnen verwerken
    # De Kafka Broker is de centrale server binnen Kafka die data ontvangt, opslaat en verspreidt naar de juiste Consumers
    # De onderstaande Producer gebruikt JSON-serialisatie voor het verzenden van data
    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'],
        max_block_ms=5000,
        value_serializer=json_serializer  
    )

    start_time = time.time()

    while True:
        if time.time() > start_time + 120:  # Stop na 2 minuten
            break

        try:
            
            # Haal temperatuurdata op
            temperature = fetch_temperature()
            for station in temperature:
                
                # Stuur JSON-geserialiseerde data naar Kafka
                producer.send('air_temperature', station)
    
                logging.info(f"Verzonden naar Kafka: {json.dumps(temperature, indent=2)}")
            

        except Exception as e:
            logging.error(f'Fout bij verzenden naar Kafka: {e}')
            continue

    producer.close()
    logging.info("Streaming gestopt")
