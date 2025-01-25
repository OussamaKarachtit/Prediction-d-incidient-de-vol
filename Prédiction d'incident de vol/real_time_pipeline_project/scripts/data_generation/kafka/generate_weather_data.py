import time
from kafka import KafkaProducer
import json
import requests
from datetime import datetime

def get_weather_data(lat, lon):
    api_key = "7f429fdff0271d795c68b7d69efddbc4"
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "lat": lat,
        "lon": lon,
        "appid": api_key,
        "units": "metric"  # Température en Celsius
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Erreur : {response.status_code}, {response.text}")
        return None

def generate_weather_data():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    quartiers = {
        "Maarif": {"lat": 33.5864851474, "lon": -7.63228035483},
        "Ain Diab": {"lat": 33.5965039240, "lon": -7.6739246301},
        "Sidi Bernoussi": {"lat": 33.60825350563965, "lon": -7.48627336133},
        "Gauthier": {"lat": 33.5904204533, "lon": -7.63173589195},
        "Hay Hassani": {"lat": 33.570887156145176, "lon": -7.67786690221775},
    }

    while True:
        for quartier, coords in quartiers.items():
            data = get_weather_data(coords["lat"], coords["lon"])
            if data:
                weather_entry = {
                    "id_meteo": data.get("id", "N/A"),
                    "quartier": quartier,
                    "temperature": data["main"]["temp"],
                    "precipitations": data.get("rain", {}).get("1h", 0),  # Précipitations sur 1 heure
                    "vent": data["wind"]["speed"],
                    "humidite": data["main"]["humidity"],
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }

                # Envoyer les données au topic Kafka
                producer.send('meteo_topic', value=weather_entry)
                print(f"Envoyé : {weather_entry}")
        print('______')
        # Pause de 3 minutes
        time.sleep(20)

if __name__ == "__main__":
    generate_weather_data()
