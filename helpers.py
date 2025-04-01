import logging
import uuid
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Functie om de Cassandra keyspace aan te maken
def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS weather_data
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    print("Keyspace created successfully!")

# Functie om de benodigde tabellen in Cassandra aan te maken
def create_tables(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS weather_data.master_table (
        station_id TEXT PRIMARY KEY,
        name TEXT,
        latitude DOUBLE,
        longitude DOUBLE
    );
    """)

    session.execute("""
    CREATE TABLE IF NOT EXISTS weather_data.temperature_data (
        id UUID PRIMARY KEY,
        station_id TEXT,
        temperature DOUBLE,
        timestamp TIMESTAMP
    );
    """)
    print("Tables created successfully!")

# Functie om een Spark sessie op te zetten
def create_spark_connection():
    try:
        spark = SparkSession.builder \
            .appName('WeatherDataStreaming') \
            .config('spark.jars', "/opt/bitnami/spark/jars/spark-cassandra-connector_2.12-3.4.1.jar,"
                                  "/opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar") \
            .config('spark.cassandra.connection.host', 'cassandra') \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        print("Connected to Spark!")
        return spark
    except Exception as e:
        logging.error(f"Couldn't create the Spark session: {e}")
        return None


# Functie om een verbinding met Kafka op te zetten
def connect_to_kafka(spark):
    try:
        df = spark.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:9092') \
            .option('subscribe', 'air_temperature') \
            .option('startingOffsets', 'earliest') \
            .load()
        print("Connected to Kafka!")
        return df
    except Exception as e:
        logging.error(f"Could not connect to Kafka: {e}")
        return None


# Functie om binnenkomende JSON-berichten uit Kafka om te zetten in een gestructureerde dataframe
def create_selection_df(spark_df):
    schema = StructType([
        StructField("station_id", StringType(), False),
        StructField("temperature", DoubleType(), False),
        StructField("name", StringType(), False),
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
        StructField("timestamp", TimestampType(), False)
    ])
    
    return spark_df.selectExpr("CAST(value AS STRING)") \
                   .select(from_json(col('value'), schema).alias('data')) \
                   .select("data.*")
                   

# Functie om een verbinding met Cassandra op te zetten
def create_cassandra_connection():
    try:
        cluster = Cluster(['cassandra'])
        session = cluster.connect()
        print("Connected to Cassandra!")
        return session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection: {e}")
        return None


def write_to_cassandra(row):
    """
    Schrijft de gegevens naar Cassandra.
    We nemen aan dat 'row' een enkele regel data is.
    """
    # Verkrijg de gegevens uit de row (zoals het binnenkomt in de foreach loop)
    station_id = row.station_id
    name = row.name
    latitude = row.latitude
    longitude = row.longitude
    temperature = row.temperature
    timestamp = row.timestamp

    try:
        # Maak verbinding met Cassandra
        from cassandra.cluster import Cluster
        cluster = Cluster(['cassandra'])  # Verbind naar je Cassandra cluster
        session = cluster.connect('weather_data')  # Verbind met de juiste keyspace

        # Insert of update het weerstation in master_table
        session.execute(
            """
            INSERT INTO master_table (station_id, name, latitude, longitude)
            VALUES (%s, %s, %s, %s)
            """, (station_id, name, latitude, longitude)
        )
        
        # Insert temperatuurdata in temperature_data
        session.execute(
            """
            INSERT INTO temperature_data (id, station_id, temperature, timestamp)
            VALUES (%s, %s, %s, %s)
            """, (uuid.uuid4(), station_id, temperature, timestamp)
        )
        session.shutdown()  # Sluit de sessie af

    except Exception as e:
        logging.error(f"Error inserting data into Cassandra: {e}")


