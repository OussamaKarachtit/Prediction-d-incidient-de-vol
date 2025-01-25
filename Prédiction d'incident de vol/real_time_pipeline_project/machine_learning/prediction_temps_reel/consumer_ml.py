import os
import json
import time
import joblib
import pandas as pd
from google.cloud import bigquery
from confluent_kafka import Consumer, KafkaException
from datetime import datetime
from confluent_kafka import KafkaError
import numpy as np
# Configuration de l'authentification Google Cloud
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/user/Desktop/PFA/real_time_pipeline_project/data/primeval-truth-445016-d4-b867bb8dd7d2.json"

# Initialisation de BigQuery Client
client = bigquery.Client()

# Configuration Kafka
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'incident_prediction_group',
    'auto.offset.reset': 'earliest'
}
topics = ['incidents_vol_topic', 'trafic_topic', 'meteo_topic', 'population_topic']

consumer = Consumer(kafka_config)
consumer.subscribe(topics)

# Charger les modèles entraînés
models = {
    'Maarif': joblib.load('/home/user/Desktop/PFA/real_time_pipeline_project/machine_learning/model/model_Maarif.pkl'),
    'Ain Diab': joblib.load(
        '/home/user/Desktop/PFA/real_time_pipeline_project/machine_learning/model/model_Ain Diab.pkl'),
    'Gauthier': joblib.load(
        '/home/user/Desktop/PFA/real_time_pipeline_project/machine_learning/model/model_Gauthier.pkl'),
    'Hay Hassani': joblib.load(
        '/home/user/Desktop/PFA/real_time_pipeline_project/machine_learning/model/model_Hay Hassani.pkl'),
    'Sidi Bernoussi': joblib.load(
        '/home/user/Desktop/PFA/real_time_pipeline_project/machine_learning/model/model_Sidi Bernoussi.pkl'),
}


# Prétraitement des données pour la prédiction
def preprocess_data(data):
    # Liste des colonnes nécessaires pour la prédiction
    required_columns = [
        'quartier', 'distance_agence_police', 'nombre_vehicules', 'niveau_trafic',
        'temperature', 'precipitations', 'vent', 'humidite',
        'densite_population', 'taux_chomage', 'niveau_vie', 'timestamp'
    ]

    # Vérification des colonnes manquantes et ajout de valeurs par défaut
    for col in required_columns:
        if col not in data.columns:
            data[col] = 0  # Remplir par défaut (peut être ajusté)
            print(f"⚠️ Colonne manquante ajoutée : {col}")

    # Encodage des variables catégoriques
    data['niveau_vie'] = data['niveau_vie'].map({'Bas': 0, 'Moyen': 1, 'Élevé': 2}).fillna(0)
    data['niveau_trafic'] = data['niveau_trafic'].map({'Faible': 0, 'Moyen': 1, 'Élevé': 2, 'Très Élevé': 3}).fillna(0)

    # Ajouter l'heure à partir du timestamp
    data['heure'] = pd.to_datetime(data['timestamp']).dt.hour

    # Supprimer la colonne timestamp après extraction de l'heure
    data.drop(columns=['timestamp'], inplace=True, errors='ignore')

    return data


# Fusionner les données Kafka
def merge_data(dfs):
    required_topics = ['incidents_vol_topic', 'trafic_topic', 'meteo_topic', 'population_topic']

    for topic in required_topics:
        if dfs[topic].empty:
            print(f"⚠️ DataFrame vide pour {topic}. Attente des nouvelles données...")
            return pd.DataFrame()

    # Conversion en datetime et tri pour chaque DataFrame
    for topic in ['incidents_vol_topic', 'trafic_topic', 'meteo_topic']:
        if 'timestamp' in dfs[topic].columns:
            dfs[topic]['timestamp'] = pd.to_datetime(dfs[topic]['timestamp'], errors='coerce')
            dfs[topic] = dfs[topic].sort_values(by=['quartier', 'timestamp'])  # Tri correct

    # Fusion asof incidents et trafic
    merged_df = pd.merge_asof(
        dfs['incidents_vol_topic'].sort_values(by='timestamp'),
        dfs['trafic_topic'].sort_values(by='timestamp'),
        on='timestamp',
        by='quartier',
        direction='backward',
        suffixes=('', '_trafic')
    )

    # Fusion avec météo
    merged_df = pd.merge_asof(
        merged_df.sort_values(by='timestamp'),
        dfs['meteo_topic'].sort_values(by='timestamp'),
        on='timestamp',
        by='quartier',
        direction='backward',
        suffixes=('', '_meteo')
    )

    # Fusion finale avec la population (par quartier uniquement)
    merged_df = merged_df.merge(
        dfs['population_topic'],
        on='quartier',
        how='left',
        suffixes=('', '_population')
    )

    return merged_df






# Stockage des prédictions dans BigQuery
def store_prediction(prediction, quartier, heure):
    table_id = "primeval-truth-445016-d4.incident_data.PREDICTION_INCIDENT"

    # Gérer le cas où la prédiction est scalaire
    if isinstance(prediction, (np.ndarray, list)):  # Si c'est un tableau/liste
        prediction_value = int(prediction[0])
    else:  # Si c'est un scalaire
        prediction_value = int(prediction)

    # Conversion explicite de l'heure
    heure_value = int(heure) if isinstance(heure, (np.integer, np.int32, np.int64)) else heure

    rows_to_insert = [{
        "quartier": quartier,
        "prediction": prediction_value,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "heure": heure_value
    }]

    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        print(f"Erreur lors de l'insertion : {errors}")
    else:
        print(f"Prédiction stockée avec succès pour {quartier}.")


# Prédiction de l'incident en temps réel
# Liste des colonnes utilisées pour l'entraînement
feature_columns = ['nombre_vehicules', 'niveau_trafic', 'temperature',
                   'precipitations', 'vent', 'humidite', 'densite_population',
                   'taux_chomage', 'niveau_vie', 'heure']


# Prédiction de l'incident en temps réel
def predict_incident(df):
    for quartier, model in models.items():
        data = df[df['quartier'] == quartier]

        if not data.empty:
            # Sélectionner uniquement les colonnes nécessaires
            X = data[feature_columns]  # Filtrage des colonnes

            prediction = model.predict(X)
            print(f"🔮 Prédiction pour {quartier} : {prediction}")

            # Stockage de la prédiction
            store_prediction(prediction[0], quartier, X['heure'].iloc[0])


# Consommation et traitement Kafka
def consume_data():
    print("🔄 En attente de messages Kafka...")

    dataframes = {topic: pd.DataFrame() for topic in topics}

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"Fin de la partition atteinte : {msg.topic()} [{msg.partition()}]")
                else:
                    print(f"Erreur Kafka : {msg.error()}")
                    continue

            # Traitement du message
            message_value = json.loads(msg.value().decode('utf-8'))
            topic = msg.topic()

            # Ajouter au dataframe approprié
            dataframes[topic] = pd.concat([dataframes[topic], pd.DataFrame([message_value])], ignore_index=True)

            # Vérification que tous les topics ont reçu des données
            if all(len(df) > 0 for df in dataframes.values()):
                print("✅ Toutes les données sont prêtes pour la fusion.")

                # Fusionner et prédire
                merged_df = merge_data(dataframes)
                df_model = preprocess_data(merged_df)
                predict_incident(df_model)

                # Pause de 10 minutes
                time.sleep(20)

            else:
                print("⚠️ Attente des données pour tous les topics...")

    except KeyboardInterrupt:
        print("Arrêt du consumer Kafka...")
    finally:
        consumer.close()



if __name__ == "__main__":
    consume_data()
