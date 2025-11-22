import requests 
import json
from insert_record import connect_db, create_table, insert_weather_data

api_key = "api_key"

def fetch_weather_data(city_name):
    """Récupère les données météo pour une ville spécifique"""
    
    api_url = f"http://api.weatherstack.com/current?access_key={api_key}&query={city_name}"
    
    try:
        print(f"  Récupération météo pour {city_name}...")
        response = requests.get(api_url)
        response.raise_for_status()
        
        data = response.json()
        
        # Vérifier si l'API a retourné une erreur
        if 'error' in data:
            print(f" Erreur API: {data['error']['info']}")
            return None
        
        # Vérifier si on a les données
        if 'current' not in data or 'location' not in data:
            print(" Données incomplètes")
            return None
        
        # Formatter les données
        weather_info = {
            'city': city_name,
            'temperature': data['current'].get('temperature'),
            'humidity': data['current'].get('humidity'),
            'pressure': data['current'].get('pressure'),
            'wind_speed': data['current'].get('wind_speed'),
            'description': data['current'].get('weather_descriptions', [''])[0],
            'country': data['location'].get('country'),
            'region': data['location'].get('region'),
            'observation_time': data['location'].get('localtime')
        }
        
        print(f" Données récupérées: {city_name} - {weather_info['temperature']}°C")
        return weather_info
        
    except requests.exceptions.RequestException as e:
        print(f" Erreur réseau: {e}")
        return None
    except Exception as e:
        print(f" Erreur inattendue: {e}")
        return None

def main():
    """Fonction principale qui orchestre tout"""
    
    print(" DÉMARRAGE DU SCRAPPING MÉTÉO")
    print("=" * 40)
    
    # 1. Se connecter à la BDD
    conn = connect_db()
    if not conn:
        return
    
    # 2. Créer la table
    create_table(conn)
    
    # 3. Liste des villes à scraper
    cities = ["New York", "Paris", "London", "Tokyo", "Casablanca"]
    
    # 4. Récupérer et stocker les données pour chaque ville
    for city in cities:
        weather_data = fetch_weather_data(city)
        if weather_data:
            insert_weather_data(conn, weather_data)
        print("---")
    
    # 5. Fermer la connexion
    conn.close()
    print(" SCRAPPING TERMINÉ AVEC SUCCÈS!")

if __name__ == "__main__":
    main()
