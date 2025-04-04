import logging
import traceback
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
    CREATE TABLE IF NOT EXISTS weather_data.temperature_data (
        id UUID PRIMARY KEY,
        station_id TEXT,
        name TEXT,
        latitude DOUBLE,
        longitude DOUBLE,
        temperature DOUBLE,
        timestamp TIMESTAMP
    );
    """)
    print("Tables created successfully!")

def create_spark_connection():
    try:        
        # Meer specifieke kafka client versie specificeren
        spark = SparkSession.builder \
            .appName('WeatherDataStreaming') \
            .master("spark://spark-master:7077") \
            .config('spark.jars', "/opt/bitnami/spark/jars/spark-cassandra-connector_2.12-3.5.1.jar,"
                              "/opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.4.1.jar," \
                              "/opt/bitnami/spark/jars/kafka-clients-3.6.1.jar," \
                              "/opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.4.1.jar," \
                              "/opt/bitnami/spark/jars/commons-pool2-2.11.1.jar,"
                              "/opt/bitnami/spark/jars/guava-30.1.1-jre.jar,"
                              "/opt/bitnami/spark/jars/spark-cassandra-connector-assembly_2.12-3.5.1.jar") \
            .config('spark.cassandra.connection.host', 'cassandra') \
            .getOrCreate()
            
        spark.sparkContext.setLogLevel("WARN")
        
        print("Connected to Spark!")
        return spark
    except Exception as e:
        logging.error(f"Couldn't create the Spark session: {e}")
        traceback.print_exc()
        return None


def connect_to_kafka(spark):
    try:
        # Gebruik de correcte broker naam
        print("Attempting to connect to Kafka localhost:29092")
        
        df = spark.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', 'air_temperature') \
            .option('startingOffsets', 'earliest') \
            .load()
        
        print("Connected to Kafka!")
        return df
    except Exception as e:
        logging.error(f"Could not connect to Kafka: {e}")
        traceback.print_exc()
        return None
    

# Functie om binnenkomende JSON-berichten uit Kafka om te zetten in een gestructureerde dataframe
def create_selection_df(spark_df):
    schema = StructType([
        StructField("station_id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
        StructField("temperature", DoubleType(), False),
        StructField("timestamp", TimestampType(), False)
    ])
    
    selection_df = spark_df.selectExpr("CAST(value AS STRING)") \
                   .select(from_json(col('value'), schema).alias('data')) \
                   .select("data.*")
    
    return selection_df


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

        


