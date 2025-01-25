from confluent_kafka import Consumer, KafkaException, KafkaError
from google.cloud import bigquery
import json
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/user/Desktop/PFA/real_time_pipeline_project/data/primeval-truth-445016-d4-b867bb8dd7d2.json"

kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'traffic_consumer',
    'auto.offset.reset': 'earliest'
}

topic = "trafic_topic"

bigquery_client = bigquery.Client()
table_id = "primeval-truth-445016-d4.incident_data.TRAFFIC_DATA"

consumer = Consumer(kafka_config)
consumer.subscribe([topic])

def clean_data(row):
    return {
        "id_trafic": row.get("id_trafic"),
        "quartier": row.get("quartier", "Inconnu"),
        "nombre_vehicules": row.get("nombre_vehicules"),
        "niveau_trafic": row.get("niveau_trafic", "Inconnu"),
        "timestamp": row.get("timestamp")
    }

def insert_to_bigquery(rows):
    cleaned_rows = [clean_data(row) for row in rows]
    errors = bigquery_client.insert_rows_json(table_id, cleaned_rows)
    if errors:
        print(f"BigQuery errors: {errors}")
    else:
        print("Inserted successfully.")

print("Consuming traffic data from Kafka...")
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

