import random
import pandas as pd
from datetime import datetime, timedelta

# Données de population fixes
population_data = [
    {"quartier": "Hay Hassani", "densite_population": 10467.13, "taux_chomage": 17.08, "niveau_vie": "Bas"},
    {"quartier": "Sidi Bernoussi", "densite_population": 13890.23, "taux_chomage": 17.51, "niveau_vie": "Bas"},
    {"quartier": "Maarif", "densite_population": 4252.31, "taux_chomage": 11.92, "niveau_vie": "Bas"},
    {"quartier": "Ain Diab", "densite_population": 3064.59, "taux_chomage": 5.55, "niveau_vie": "Élevé"},
    {"quartier": "Gauthier", "densite_population": 11768.83, "taux_chomage": 8.52, "niveau_vie": "Élevé"}
]

def generate_model_data(num_rows_per_quartier=10000):
    niveaux_trafic = ['Faible', 'Moyen', 'Élevé', 'Très Élevé']
    etats = ['Pas de vol', 'En cours', 'Résolu']
    data = []
    start_date = datetime.now()

    for pop_row in population_data:
        quartier = pop_row['quartier']

        # 500 lignes avec 'Pas de vol'
        for _ in range(num_rows_per_quartier // 2):
            data.append(generate_row(pop_row, start_date, 'Pas de vol', niveaux_trafic))

        # 250 lignes avec 'En cours'
        for _ in range(num_rows_per_quartier // 4):
            data.append(generate_row(pop_row, start_date, 'En cours', niveaux_trafic))

        # 250 lignes avec 'Résolu'
        for _ in range(num_rows_per_quartier // 4):
            data.append(generate_row(pop_row, start_date, 'Résolu', niveaux_trafic))

    # Créer un DataFrame
    df_model = pd.DataFrame(data)

    # Supprimer la colonne distance_agence_police si elle existe
    if 'distance_agence_police' in df_model.columns:
        df_model.drop(columns=['distance_agence_police'], inplace=True)

    return df_model


def generate_row(pop_row, start_date, etat, niveaux_trafic):
    temperature = round(random.uniform(7, 30), 2)
    precipitation = round(random.uniform(0, 3), 2)
    vent = round(random.uniform(5, 40), 2)

    return {
        "quartier": pop_row['quartier'],
        "nombre_vehicules": random.randint(50, 1500),
        "niveau_trafic": random.choice(niveaux_trafic),
        "temperature": temperature,
        "precipitations": precipitation,
        "vent": vent,
        "humidite": round(40 + precipitation * 5, 2),
        "densite_population": pop_row['densite_population'],
        "taux_chomage": pop_row['taux_chomage'],
        "niveau_vie": pop_row['niveau_vie'],
        "etat": etat,
        "timestamp": (start_date - timedelta(minutes=random.randint(0, 10000))).strftime('%Y-%m-%d %H:%M:%S')
    }


# Générer les données et afficher le résultat
df_model = generate_model_data()
print(df_model.head(10))

# Sauvegarder en CSV
df_model.to_csv('/home/user/Desktop/PFA/real_time_pipeline_project/data/ML/df_model.csv', index=False)
