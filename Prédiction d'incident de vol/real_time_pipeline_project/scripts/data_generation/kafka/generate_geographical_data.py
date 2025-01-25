import random
from kafka import KafkaProducer
import json

def generate_geographical_data():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    quartiers = {
        "Maarif": {"latitude": 33.5875, "longitude": -7.6314, "superficie": 4.5},
        "Ain Diab": {"latitude": 33.6053, "longitude": -7.6404, "superficie": 7.2},
        "Sidi Bernoussi": {"latitude": 33.5914, "longitude": -7.5451, "superficie": 5.8},
        "Gauthier": {"latitude": 33.5900, "longitude": -7.6200, "superficie": 3.0},
        "Hay Hassani": {"latitude": 33.5800, "longitude": -7.6700, "superficie": 6.5}
    }

    # Boucle sur tous les quartiers une seule fois
    for quartier, info in quartiers.items():
        data = {
            "quartier": quartier,
            "latitude": info["latitude"],
            "longitude": info["longitude"],
            "superficie": info["superficie"]
        }
        producer.send('geographie_topic', value=data)
        print(f"Envoyé : {data}")

    # Libérer les ressources du producteur
    producer.flush()
    producer.close()
if __name__ == "__main__":
    generate_geographical_data()
