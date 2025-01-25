import threading
import generate_geographical_data
import generate_incidents_vol
import generate_population_data
import generate_traffic_data
import generate_weather_data

def run_geographical_data():
    generate_geographical_data.generate_geographical_data()

def run_incidents_vol():
    generate_incidents_vol.generate_incidents_vol()

def run_population_data():
    generate_population_data.generate_population_data()

def run_traffic_data():
    generate_traffic_data.generate_traffic_data()

def run_weather_data():
    generate_weather_data.generate_weather_data()

if __name__ == "__main__":
    # Créer des threads pour chaque script
    threads = [
        threading.Thread(target=run_geographical_data),
        threading.Thread(target=run_incidents_vol),
        threading.Thread(target=run_population_data),
        threading.Thread(target=run_traffic_data),
        threading.Thread(target=run_weather_data),
    ]

    # Lancer tous les threads
    for thread in threads:
        thread.start()

    # Attendre que tous les threads se terminent
    for thread in threads:
        thread.join()

    print("Tous les scripts de génération de données sont terminés.")
