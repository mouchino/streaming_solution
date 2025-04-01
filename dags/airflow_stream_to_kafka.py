from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from temperature_data_fetcher import stream_data

default_args = {
    'owner': 'weather_team',  # De eigenaar van de DAG
    'start_date': datetime(2025, 1, 4, 10, 00)  # De startdatum voor de uitvoering van de DAG. De taak wordt vanaf deze datum gepland
}


# catchup=False betekent dat Airflow geen "gemiste" runs zal inhalen voor het tijdsinterval tussen start_date en nu
# schedule_interval='@once' zorgt ervoor dat de taak slechts één keer draait en niet herhaald wordt

with DAG('temperature_streaming', 
         default_args=default_args,
         schedule_interval='@daily',  
         catchup=False) as temperature_streaming:

    streaming_task = PythonOperator(
        task_id='send_temperature_to_kafka',
        python_callable=stream_data  # Roept de stream_data functie aan die de temperatuurdata naar Kafka verzendt
    )