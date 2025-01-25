import random
import time
from kafka import KafkaProducer
import json


def generate_incidents_vol():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    quartiers = ['Maarif', 'Ain Diab', 'Sidi Bernoussi', 'Gauthier', 'Hay Hassani']
    types_vol = ['Vol de voiture', 'Cambriolage', 'Pickpocket']
    etats = ['En cours', 'Résolu']
    agences_magazins_generiques = ['Wafacash', 'Cashplus', 'Agence bancaire', 'BIM', 'Acima', 'Marjane Market',
                                   'Carrefour Market']
    magasins_special_maarif = ['Zara', 'Pull&Bear', 'Bershka']

    while True:
        for quartier in quartiers:
            # Décision : Vol ou Pas de Vol
            vol_present = random.choice([True, False])

            if vol_present:
                vol_type = random.choice(types_vol)

                # Logique pour définir les valeurs en fonction du type de vol
                if vol_type == 'Pickpocket':
                    valeur_bien = round(random.uniform(50, 1000), 2)  # Maximum 1000
                elif vol_type == 'Vol de voiture':
                    # Les voitures ont une valeur minimale plus élevée
                    valeur_bien = round(random.uniform(100000, 300000), 2)
                elif vol_type == 'Cambriolage':
                    # Différencier la valeur selon le quartier
                    if quartier in ['Maarif', 'Ain Diab', 'Gauthier']:
                        valeur_bien = round(random.uniform(20000, 100000), 2)  # Quartiers riches
                    else:
                        valeur_bien = round(random.uniform(5000, 30000), 2)  # Quartiers populaires
                else:
                    valeur_bien = None  # Cas par défaut (ne devrait pas arriver)

                etat = random.choice(etats)

                # Si l'état est 'En cours', la valeur du bien est inconnue
                if etat == 'En cours':
                    valeur_bien = None

                # Logique pour les cambriolages
                if vol_type == 'Cambriolage':
                    if quartier == 'Maarif':
                        lieu = random.choice(magasins_special_maarif)  # Magasins spécifiques à Maarif
                    else:
                        lieu = random.choice(agences_magazins_generiques)  # Magasins ou agences génériques
                else:
                    lieu = None  # Pas de lieu pour les autres types de vol

                data = {
                    "id_incident": random.randint(1000, 9999),
                    "quartier": quartier,
                    "type_vol": vol_type,
                    "valeur_bien": valeur_bien,
                    "etat": etat,
                    "age_victime": random.randint(18, 70),
                    "sexe_victime": random.choice(['Homme', 'Femme']),
                    "voleur_arrete": random.choice([True, False]),
                    "distance_agence_police": round(random.uniform(0.5, 5.0), 2),
                    "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                    "lieu": lieu
                }
            else:
                # Données pour "Pas de vol"
                data = {
                    "id_incident": None,
                    "quartier": quartier,
                    "type_vol": None,
                    "valeur_bien": None,
                    "etat": "Pas de vol",
                    "age_victime": None,
                    "sexe_victime": None,
                    "voleur_arrete": None,
                    "distance_agence_police": None,
                    "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                    "lieu": None
                }

            # Envoyer les données au topic Kafka
            producer.send('incidents_vol_topic', value=data)
            print(f"Envoyé : {data}")
            print('__________________________')
            time.sleep(20)  # Pause de 2 secondes pour simuler le temps réel


if __name__ == "__main__":
    generate_incidents_vol()
