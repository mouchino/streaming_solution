import logging
import uuid
from helpers import create_spark_connection, connect_to_kafka, create_selection_df, create_cassandra_connection, create_keyspace, create_tables
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Definieer een UDF (User Defined Function) om een UUID te genereren
generate_uuid = udf(lambda: str(uuid.uuid4()), StringType())

# Hoofdprogramma om de streaming pipeline te starten
def main():
    try:
        spark = create_spark_connection()
        if spark is None:
            logging.error("Spark session could not be created")
            return
        
        session = create_cassandra_connection()
        if session is None:
            logging.error("Cassandra connection could not be created")
            return
        
        # Initialiseer Cassandra keyspace en tabellen
        create_keyspace(session)
        create_tables(session)
        
        # Verbind met Kafka en lees data
        kafka_df = connect_to_kafka(spark)
        if kafka_df is None:
            logging.error("Could not connect to Kafka topic")
            return
        
        
        kafka_df.printSchema()  # Dit toont het schema van het DataFrame
        structured_df = create_selection_df(kafka_df)
        
        streaming_query_temperature = (structured_df.writeStream
                                      .foreachBatch(process_batch)
                                      .option('checkpointLocation', '/tmp/checkpoint')
                                      .start())
   
        streaming_query_temperature.awaitTermination()
    
    except Exception as e:
        logging.error(f"Unexpected error in main: {e}")
        import traceback
        traceback.print_exc()

        
def process_batch(batch_df, batch_id):
    # Log batch information
    print(f"Processing batch {batch_id} with {batch_df.count()} records")
    
    
    # Voeg een ID-kolom toe aan het DataFrame
    batch_df = batch_df.withColumn("id", generate_uuid())
    
    # Write to Cassandra using DataFrame API (not streaming)
    if not batch_df.isEmpty():
        batch_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="temperature_data", keyspace="weather_data") \
            .mode("append") \
            .save()

if __name__ == "__main__":
    main()
    
