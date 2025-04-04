# README: Data Pipeline met Apache Airflow, Kafka, Spark en Cassandra

## Overzicht
Deze Docker-gebaseerde setup implementeert een end-to-end data pipeline die temperatuurdata van stations in Singapore verzamelt, verwerkt en opslaat. De architectuur omvat diverse technologieën zoals Apache Airflow, Kafka, Spark en Cassandra om de datastromen te beheren en real-time verwerking mogelijk te maken.

## Architectuur en Componenten

### 1. **Data Source**
We gebruiken een openbare API om temperatuurdata van weerstations in Singapore op te halen. Dit gebeurt via een Airflow DAG, die periodiek data verzamelt en opslaat in een PostgreSQL-database.

### 2. **Apache Airflow**
- **Doel:** Orkestratie van de data pipeline.
- **Werking:**
  - Start de dataverwerking door periodiek data van de API op te halen.
  - Slaat opgehaalde data op in een PostgreSQL-database.
  - Start Kafka-producers om de data naar Kafka-topics te sturen.
- **Configuratie:**
  - `entrypoint.sh`: Installeert vereisten en initialiseert de Airflow database.
  - DAG-scripts in de `dags/` directory: Bevatten de workflowlogica.

### 3. **PostgreSQL**
- **Doel:** Tijdelijke opslag van opgehaalde temperatuurdata voordat deze naar Kafka wordt gestuurd.
- **Gebruikt door:** Airflow om opgehaalde API-data op te slaan.

### 4. **Apache Kafka en Zookeeper**
- **Doel:** Streaming van data tussen verschillende componenten.
- **Werking:**
  - Een Airflow-task stuurt data vanuit PostgreSQL naar een Kafka-topic.
  - Spark leest data real-time uit het Kafka-topic voor verdere verwerking.
  - Zookeeper beheert de Kafka-infrastructuur.
- **Configuratie:**
  - Kafka is ingesteld als broker.
  - Topics worden gebruikt om de data tussen componenten te sturen.
  - `helpers.py` bevat functies voor het verbinden met Kafka.

### 5. **Control Center en Schema Registry**
- **Doel:** Monitoring en schema-management voor Kafka streams.
- **Werking:**
  - Control Center biedt een GUI om Kafka-topics en brokers te monitoren.
  - Schema Registry zorgt voor consistente dataformaten in Kafka.

### 6. **Apache Spark**
- **Doel:** Real-time verwerking van temperatuurdata.
- **Werking:**
  - Spark leest inkomende data van Kafka.
  - Verwerkt de ruwe data en zet deze om naar een gestructureerd formaat.
  - Slaat de verwerkte data op in een Cassandra-database.
- **Configuratie:**
  - `spark_stream_to_cassandra.py` bevat de Spark-logica voor data-verwerking.
  - `master_node_setup.sh` installeert benodigde Spark- en Kafka-connectors.

### 7. **Cassandra**
- **Doel:** Permanente opslag van de verwerkte data.
- **Waarom Cassandra?**
  - Geschikt voor hoge snelheid en real-time streaming workloads.
  - Geoptimaliseerd voor grote hoeveelheden data.
- **Configuratie:**
  - `helpers.py` bevat functies om verbinding te maken en data in te laden.

## Directory-structuur
```
- dags/                    # Bevat Airflow DAG scripts
- scripts/
    - entrypoint.sh        # Startscript voor Airflow
- helpers.py               # Bevat helperfuncties voor Kafka, Cassandra en datavoorbereiding
- spark_stream_to_cassandra.py  # Hoofdscript voor Spark verwerking
- master_node_setup.sh     # Installaties voor de Spark master node
- docker-compose.yml       # Definieert de Docker containers en netwerken
- requirements.txt         # Lijst met benodigde Python-modules
```

## Belangrijke poorten in `docker-compose.yml`
| Service | Poort | Functie |
|---------|------|---------|
| Airflow Webserver | 8080 | GUI voor workflow management |
| PostgreSQL | 5432 | Opslag van opgehaalde data |
| Kafka Broker | 9092 | Distributie van datastromen |
| Zookeeper | 2181 | Beheer van Kafka clusters |
| Spark Master | 7077 | Spark cluster management |
| Cassandra | 9042 | Opslag en ophalen van verwerkte data |
| Control Center | 9021 | Monitoring van Kafka |

## Installatie en Uitvoering
### 1. **Docker containers opstarten**
```sh
docker-compose up -d
```
### 2. **Airflow UI openen en Airflow starten**
Ga naar: [http://localhost:8080](http://localhost:8080) en log in met de admin-gebruikersnaam en wachtwoord.
Start de scheduler met behulp van de Airflow UI

### 3. **Controleer Kafka topics**
Ga naar: [http://localhost:9021](http://localhost:9021) en controleer of de berichten aankomen bij de Topic

### 4. **Spark streaming job starten**
Dit script doet het volgende:

Kopieert Python-bestanden naar een Docker-container:
-spark_stream_to_cassandra.py en helpers.py worden gekopieerd naar de /app/ map van de container streaming_solution-spark-master-1.

Opent een interactieve Bash-shell binnen de container:
-Hierdoor kan je handmatig commando's uitvoeren binnen de container.

Start een Spark-job met spark-submit:
-Voert het script spark_stream_to_cassandra.py uit binnen een Apache Spark-cluster.
-Gebruikt spark://localhost:7077 als Spark-master.
-Zet configuraties voor authenticatie en bestandsbeheer.
-Definieert een specifieke Ivy-cache locatie voor Spark-jars.

```sh
docker cp .\streaming_solution\spark_stream_to_cassandra.py streaming_solution-spark-master-1:/app/ && \
docker cp .\streaming_solution\helpers.py streaming_solution-spark-master-1:/app/ && \
docker exec -it streaming_solution-spark-master-1 /bin/bash 


  spark-submit --verbose --master spark://localhost:7077 \
  --conf spark.hadoop.security.authentication=none \
  --conf spark.hadoop.fs.file.impl.disable.cache=true \
  --conf spark.jars.ivy=/opt/bitnami/spark/.ivy2 \
  /app/spark_stream_to_cassandra.py

```

### 5. **Controleer worker nodes**
Ga naar: [http://localhost:9090](http://localhost:9090) en controleer of de Spark Worker Node actief is en de streaming jobs verwerkt

### 6. **Controleer de Cassandra database**
```sh
# Verbinden met de Cassandra container en de CQL shell starten
docker exec -it cassandra cqlsh -u cassandra -p cassandra

# Binnen de CQL shell, voer de volgende commando's uit:
DESC TABLE weather_data.master_table;
DESC TABLE weather_data.temperature_data;

SELECT * FROM weather_data.master_table;
SELECT * FROM weather_data.temperature_data;

```
## Conclusie
Deze setup biedt een robuuste en schaalbare oplossing voor het ophalen, verwerken en opslaan van real-time data. Dankzij Apache Airflow, Kafka, Spark en Cassandra kunnen we efficiënt data stroomlijnen en verwerken.

