import logging
from helpers import create_spark_connection, connect_to_kafka, create_selection_df, create_cassandra_connection, create_keyspace, create_tables, write_to_cassandra


# Hoofdprogramma om de streaming pipeline te starten
def main():
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
    
    # Verbind met Kafka en lees data♀
    kafka_df = connect_to_kafka(spark)
    if kafka_df is None:
        logging.error("Could not connect to Kafka topic")
        return
    
    # Converteer de Kafka-data naar een gestructureerd DataFrame
    structured_df = create_selection_df(kafka_df)
    
    # Roep write_to_cassandra aan voor elke rij
    streaming_query = (structured_df.writeStream
                       .foreach(write_to_cassandra)  # Roep write_to_cassandra aan voor elke regel
                       .outputMode("append")
                       .option("checkpointLocation", "C:/tmp/checkpoint")
                       .start())
    
    streaming_query.awaitTermination()
        
if __name__ == "__main__":
    main()
    
    

