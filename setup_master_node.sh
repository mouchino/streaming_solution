#!/bin/bash

# Zet de shell in 'continue on error' modus (fouten stoppen het script niet)
set +e

echo "Maken van directories..."
mkdir -p /app
mkdir -p /tmp/spark
export SPARK_LOCAL_DIR=/tmp/spark

echo "Installeren van curl..."
apt-get update && apt-get install -y curl

echo "Downloaden van Spark Cassandra Connector..."
curl -o /opt/bitnami/spark/jars/spark-cassandra-connector_2.12-3.4.1.jar \
    https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.12/3.4.1/spark-cassandra-connector_2.12-3.4.1.jar

echo "Downloaden van Spark SQL Kafka Connector..."
curl -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar

echo "Installeren van Python Cassandra-driver..."
pip install cassandra-driver

echo "Installeren van Python Airflow..."
pip install apache-airflow

echo "Installeren van Python PySpark..."
pip install spark pyspark

echo "Setup voltooid!"
