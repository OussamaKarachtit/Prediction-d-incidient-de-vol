from confluent_kafka import Consumer, KafkaException, KafkaError
from google.cloud import bigquery
import json
import os

# Set the path to your GCP credentials file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/user/Desktop/PFA/real_time_pipeline_project/data/primeval-truth-445016-d4-b867bb8dd7d2.json"

# Kafka consumer configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'incidents_vol_consumer',
    'auto.offset.reset': 'earliest'
}

topic = "incidents_vol_topic"

# BigQuery configuration
bigquery_client = bigquery.Client()
table_id = "primeval-truth-445016-d4.incident_data.INCIDENTS_VOL"

# Initialize Kafka consumer
consumer = Consumer(kafka_config)
consumer.subscribe([topic])

# Data cleaning function
def clean_data(row):
    """
    Cleans and validates the data to match the BigQuery schema.
    """
    cleaned_row = {
        "id_incident": row.get("id_incident"),
        "quartier": row.get("quartier", "Inconnu"),
        "type_vol": row.get("type_vol"),
        "valeur_bien": row.get("valeur_bien"),
        "etat": row.get("etat", "Pas de vol"),
        "age_victime": row.get("age_victime"),
        "sexe_victime": row.get("sexe_victime"),
        "voleur_arrete": row.get("voleur_arrete"),
        "distance_agence_police": row.get("distance_agence_police"),
        "timestamp": row.get("timestamp"),
        "lieu": row.get("lieu")
    }
    return cleaned_row

# Insert data into BigQuery
def insert_to_bigquery(rows):
    cleaned_rows = [clean_data(row) for row in rows]
    errors = bigquery_client.insert_rows_json(table_id, cleaned_rows)
    if errors:
        print(f"BigQuery errors: {errors}")
    else:
        print("Inserted successfully.")

print("Consuming incidents data from Kafka...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"End of partition reached: {msg.topic()} [{msg.partition()}]")
            else:
                raise KafkaException(msg.error())
        else:
            message_value = msg.value().decode('utf-8')
            print(f"Message received: {message_value}")
            try:
                row = json.loads(message_value)
                insert_to_bigquery([row])
            except json.JSONDecodeError as e:
                print(f"JSON parse error: {e}")
except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    consumer.close()


