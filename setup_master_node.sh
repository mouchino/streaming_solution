#!/bin/bash

# Zet de shell in 'continue on error' modus (fouten stoppen het script niet)
set +e

echo "Maken van directories..."
mkdir -p /app
mkdir -p /tmp/spark
export SPARK_LOCAL_DIR=/tmp/spark

echo "Setup voltooid!"

echo "Installeren van Python Cassandra-driver..."
pip install cassandra-driver