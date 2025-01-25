import time
from kafka import KafkaProducer
import json
import random
from datetime import datetime

def generate_traffic_data():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    quartiers = ['Maarif', 'Ain Diab', 'Sidi Bernoussi', 'Gauthier', 'Hay Hassani']

    def get_vehicle_count(niveau_trafic):
        """
        Retourne un nombre de véhicules en fonction du niveau de trafic.
        """
        if niveau_trafic == 'Faible':
            return random.randint(10, 50)
        elif niveau_trafic == 'Moyen':
            return random.randint(51, 200)
        elif niveau_trafic == 'Élevé':
            return random.randint(201, 400)
        elif niveau_trafic == 'Très Élevé':
            return random.randint(401, 600)

    def get_traffic_level(current_hour):
        """
        Détermine le niveau de trafic en fonction de l'heure actuelle.
        """
        if 7 <= current_hour < 9 or 17 <= current_hour < 19:  # Heures de pointe
            return random.choice(['Élevé', 'Très Élevé'])
        else:  # Heures creuses
            return random.choice(['Faible', 'Moyen'])

    # Générer des données initiales pour chaque quartier
    traffic_data = []
    for quartier in quartiers:
        current_hour = datetime.now().hour
        niveau_trafic = get_traffic_level(current_hour)
        data = {
            "id_trafic": random.randint(1000, 9999),
            "quartier": quartier,
            "nombre_vehicules": get_vehicle_count(niveau_trafic),
            "niveau_trafic": niveau_trafic,
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
        }
        traffic_data.append(data)

    while True:
        # Envoyer les données actuelles pour tous les quartiers
        for data in traffic_data:
            producer.send('trafic_topic', value=data)
            print(f"Envoyé : {data}")

        # Pause de 2 minutes
        time.sleep(120)

        # Mettre à jour les données pour simuler des changements
        for data in traffic_data:
            current_hour = datetime.now().hour
            data["niveau_trafic"] = get_traffic_level(current_hour)  # Ajuster en fonction de l'heure
            data["nombre_vehicules"] = get_vehicle_count(data["niveau_trafic"])  # Ajuster le nombre de véhicules
            data["timestamp"] = time.strftime('%Y-%m-%d %H:%M:%S')  # Mettre à jour l'heure

if __name__ == "__main__":
    generate_traffic_data()