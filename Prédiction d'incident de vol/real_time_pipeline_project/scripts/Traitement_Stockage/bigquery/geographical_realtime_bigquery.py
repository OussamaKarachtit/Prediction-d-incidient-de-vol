from confluent_kafka import Consumer, KafkaException, KafkaError
from google.cloud import bigquery
import json
import os

# Chemin vers votre fichier de clé GCP
os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"] = "/home/user/Desktop/PFA/real_time_pipeline_project/data/primeval-truth-445016-d4-b867bb8dd7d2.json"

# Configuration Kafka Consumer
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'kafka_to_bigquery_geographical',
    'auto.offset.reset': 'earliest'
}

topic = "geographie_topic"  # Nom du topic Kafka pour les données géographiques

# Configuration BigQuery
bigquery_client = bigquery.Client()
table_id = "primeval-truth-445016-d4.incident_data.GEOGRAPHICAL_DATA"

# Initialisation du consumer Kafka
consumer = Consumer(kafka_config)
consumer.subscribe([topic])

# Fonction de nettoyage des données
def clean_data(row):
    """
    Nettoie et valide les données pour qu'elles correspondent au schéma BigQuery.
    """
    try:
        cleaned_row = {
            "quartier": row.get("quartier", "Inconnu"),  # Valeur par défaut
            "latitude": float(row["latitude"]),
            "longitude": float(row["longitude"]),
            "superficie": float(row["superficie"])
        }
        return cleaned_row
    except (KeyError, ValueError, TypeError) as e:
        print(f"Erreur de nettoyage des données : {e}")
        return None

# Fonction pour insérer des données dans BigQuery
def insert_to_bigquery(rows):
    """Insère des lignes dans BigQuery après nettoyage."""
    cleaned_rows = [clean_data(row) for row in rows if clean_data(row) is not None]
    if cleaned_rows:
        errors = bigquery_client.insert_rows_json(table_id, cleaned_rows)
        if errors:
            print(f"Erreur lors de l'insertion dans BigQuery : {errors}")
        else:
            print("Données insérées avec succès dans BigQuery.")

# Consommation des messages Kafka
print("Consommation des messages depuis Kafka...")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"Fin de partition atteinte : {msg.topic()} [{msg.partition()}]")
            else:
                raise KafkaException(msg.error())
        else:
            # Conversion du message Kafka en JSON
            message_value = msg.value().decode('utf-8')
            print(f"Message reçu : {message_value}")

            try:
                row = json.loads(message_value)  # Parsing JSON
                insert_to_bigquery([row])  # Insérer dans BigQuery
            except json.JSONDecodeError as e:
                print(f"Erreur de parsing JSON : {e}")

except KeyboardInterrupt:
    print("Arrêt du consumer Kafka...")
finally:
    consumer.close()
