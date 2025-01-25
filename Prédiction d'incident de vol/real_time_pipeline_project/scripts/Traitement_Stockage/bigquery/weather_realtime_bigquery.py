from confluent_kafka import Consumer, KafkaException, KafkaError
from google.cloud import bigquery
import json
import os

# Configuration GCP
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/user/Desktop/PFA/real_time_pipeline_project/data/primeval-truth-445016-d4-b867bb8dd7d2.json"

# Configuration Kafka
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'traffic_consumer',
    'auto.offset.reset': 'earliest'
}


# Configuration BigQuery
bigquery_client = bigquery.Client()
# Remplacez topic = "traffic_topic" par "weather_topic" et table_id par WEATHER_DATA.
topic = "meteo_topic"
table_id = "primeval-truth-445016-d4.incident_data.WEATHER_DATA"

def clean_data(row):
    return {
        "id_meteo": row.get("id_meteo", 0),
        "quartier": row.get("quartier", "Inconnu"),
        "temperature": row.get("temperature", 0.0),
        "precipitations": row.get("precipitations", 0.0),
        "vent": row.get("vent", 0.0),
        "humidite": row.get("humidite", 0.0),
        "timestamp": row.get("timestamp")
    }

# Insertion dans BigQuery
def insert_to_bigquery(rows):
    cleaned_rows = [clean_data(row) for row in rows]
    errors = bigquery_client.insert_rows_json(table_id, cleaned_rows)
    if errors:
        print(f"Erreur lors de l'insertion : {errors}")
    else:
        print("Données insérées avec succès.")

# Consommation Kafka
consumer = Consumer(kafka_config)
consumer.subscribe([topic])

try:
    print("Consommation des messages depuis Kafka...")
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        message_value = msg.value().decode('utf-8')
        print(f"Message reçu : {message_value}")
        row = json.loads(message_value)
        insert_to_bigquery([row])
except KeyboardInterrupt:
    print("Arrêt du consumer.")
finally:
    consumer.close()
