import psycopg2

def connect_db():
    try:
        # 1. Se connecter à la BDD
        conn = psycopg2.connect(
        host="db",
        port=5432 , 
        database="db_postgres",
        user="amine",
        password="amine10"
        )
        print(" Connexion à PostgreSQL réussie!")
        return conn
    except Exception as e:
        print(f" Erreur de connexion: {e}")
        return None


def create_table(conn):
    """Crée la table si elle n'existe pas"""
    print(" Création de la table si elle n'existe pas...")
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS weather_data (
        id SERIAL PRIMARY KEY,
        city VARCHAR(100) NOT NULL,
        temperature DECIMAL(5,2),
        humidity INTEGER,
        pressure INTEGER,
        wind_speed DECIMAL(5,2),
        description TEXT,
        country VARCHAR(100),
        region VARCHAR(100),
        observation_time VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        conn.commit()
        print(" Table 'weather_data' créée/vérifiée!")
        cursor.close()
    except Exception as e:
        print(f" Erreur création table: {e}")

def insert_weather_data(conn, weather_dict):
    """Insère les données météo dans la table"""
    
    insert_sql = """
    INSERT INTO weather_data 
    (city, temperature, humidity, pressure, wind_speed, description, country, region, observation_time)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    try:
        cursor = conn.cursor()
        cursor.execute(insert_sql, (
            weather_dict['city'],
            weather_dict['temperature'],
            weather_dict['humidity'],
            weather_dict['pressure'],
            weather_dict['wind_speed'],
            weather_dict['description'],
            weather_dict['country'],
            weather_dict['region'],
            weather_dict['observation_time']
        ))
        conn.commit()
        cursor.close()
        print(f" Données insérées pour {weather_dict['city']}!")
        return True
    except Exception as e:
        print(f" Erreur insertion: {e}")
        return False


if __name__ == "__main__":
    conn = connect_db()
    if conn:
        create_table(conn)
        conn.close()