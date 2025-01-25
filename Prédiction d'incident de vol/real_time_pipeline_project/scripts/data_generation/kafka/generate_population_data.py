import random
import time
from kafka import KafkaProducer
import json

def generate_population_data():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Définition des quartiers avec leurs caractéristiques
    quartiers = {
        "Hay Hassani": {"niveau_vie": ["Bas", "Moyen"], "taux_chomage": (15, 25)},
        "Sidi Bernoussi": {"niveau_vie": ["Bas", "Moyen"], "taux_chomage": (15, 25)},
        "Maarif": {"niveau_vie": ["Bas", "Moyen", "Élevé"], "taux_chomage": (10, 20)},
        "Ain Diab": {"niveau_vie": ["Élevé"], "taux_chomage": (5, 10)},
        "Gauthier": {"niveau_vie": ["Élevé"], "taux_chomage": (5, 10)}
    }

    # Générer des données pour chaque quartier une fois
    population_data = []
    for quartier, details in quartiers.items():
        data = {
            "quartier": quartier,
            "densite_population": round(random.uniform(500, 15000), 2),
            "taux_chomage": round(random.uniform(*details["taux_chomage"]), 2),
            "niveau_vie": random.choice(details["niveau_vie"]),
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
        }
        population_data.append(data)

    # Envoyer les données générées à Kafka
    for data in population_data:
        producer.send('population_topic', value=data)
        print(f"Envoyé : {data}")

    # Simuler une mise à jour tous les x temps (exemple : toutes les 24h)
    while True:
        time.sleep(86400)  # 86400 secondes = 1 jour
        for data in population_data:
            # Simuler un léger changement pour refléter une évolution
            data["densite_population"] += round(random.uniform(-100, 100), 2)
            data["taux_chomage"] += round(random.uniform(-0.5, 0.5), 2)

            # Garder les valeurs logiques
            if data["quartier"] in quartiers:
                details = quartiers[data["quartier"]]
                data["taux_chomage"] = max(
                    min(data["taux_chomage"], details["taux_chomage"][1]),
                    details["taux_chomage"][0]
                )
                data["niveau_vie"] = random.choice(details["niveau_vie"])

            data["timestamp"] = time.strftime('%Y-%m-%d %H:%M:%S')
            producer.send('population_topic', value=data)
            print(f"Mise à jour : {data}")

if __name__ == "__main__":
 generate_population_data()


