from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta
import sys
import os

# ✅ Ajouter le chemin vers scrape_data
sys.path.insert(0, '/opt/airflow/scrape_data')

# ✅ Debug : Vérifier ce qui est disponible
print("🔍 Debug - Contenu du dossier scrape_data:")
if os.path.exists('/opt/airflow/scrape_data'):
    for file in os.listdir('/opt/airflow/scrape_data'):
        print(f"   - {file}")

# ✅ Import depuis le dossier scrape_data
try:
    from insert_record import connect_db, create_table, insert_weather_data
    from api_request import fetch_weather_data
    print("✅ Import des modules depuis scrape_data réussi!")
except ImportError as e:
    print(f"❌ Erreur d'import: {e}")
    # Afficher plus de détails pour debug
    import traceback
    traceback.print_exc()
    
    # Vérifier les chemins Python
    print("🔍 Chemins Python disponibles:")
    for path in sys.path:
        print(f"   - {path}")
    raise

default_args = {
    'owner': 'amine',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

def setup_database():
    """Task 1: Créer la structure de la base"""
    print("🗄️ Configuration de la base de données...")
    conn = connect_db()
    if conn:
        create_table(conn)
        conn.close()
        print("✅ Base de données configurée!")
    else:
        raise Exception("❌ Impossible de configurer la base de données")

def extract_weather_data_task():
    """Task 2: Extraire les données météo"""
    print("🌤️ Extraction des données météo...")
    
    cities = ["New York", "Paris", "London", "Tokyo", "Casablanca"]
    all_data = []
    
    for city in cities:
        data = fetch_weather_data(city)
        if data:
            all_data.append(data)
        print(f"📡 {city}: {'✅' if data else '❌'}")
    
    if not all_data:
        raise Exception("❌ Aucune donnée récupérée")
    
    print(f"✅ {len(all_data)}/{len(cities)} villes récupérées")
    return all_data

def load_weather_data_task(**context):
    """Task 3: Charger les données dans PostgreSQL"""
    print("💾 Chargement des données...")
    
    # Récupérer les données de la task précédente
    weather_data = context['task_instance'].xcom_pull(task_ids='extract_weather_data')
    
    conn = connect_db()
    if not conn:
        raise Exception("❌ Impossible de se connecter à la BDD")
    
    success_count = 0
    for data in weather_data:
        if insert_weather_data(conn, data):
            success_count += 1
    
    conn.close()
    
    print(f"✅ {success_count}/{len(weather_data)} enregistrements chargés")
    return success_count

def validate_data_task():
    """Task 4: Valider la qualité des données"""
    print("🔍 Validation des données...")
    
    conn = connect_db()
    if not conn:
        raise Exception("❌ Impossible de se connecter à la BDD")
    
    try:
        cursor = conn.cursor()
        
        # Compter les enregistrements
        cursor.execute("SELECT COUNT(*) FROM weather_data")
        total = cursor.fetchone()[0]
        
        # Afficher les dernières données
        cursor.execute("""
            SELECT city, temperature, created_at 
            FROM weather_data 
            ORDER BY created_at DESC 
            LIMIT 3
        """)
        recent = cursor.fetchall()
        
        cursor.close()
        
        print(f"📊 Total enregistrements: {total}")
        print("🆕 Dernières données:")
        for city, temp, date in recent:
            print(f"   🏙️  {city}: {temp}°C - {date}")
        
        if total == 0:
            raise Exception("❌ Aucune donnée dans la base")
            
        print("✅ Validation réussie!")
        return True
        
    except Exception as e:
        print(f"❌ Erreur validation: {e}")
        raise
    finally:
        conn.close()

# DAG definition
with DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='Pipeline ETL pour données météo WeatherStack',
    schedule=timedelta(hours=1),
    catchup=False,
    tags=['weather', 'etl', 'data_engineering']
) as dag:

    start = EmptyOperator(task_id='start')
    
    setup_db = PythonOperator(
        task_id='setup_database',
        python_callable=setup_database
    )
    
    extract_data = PythonOperator(
        task_id='extract_weather_data',
        python_callable=extract_weather_data_task
    )

    dbt_pipeline = DockerOperator(
        task_id='transform_data_pipeline',
        image='ghcr.io/dbt-labs/dbt-postgres:1.9.latest',
        container_name='airflow_dbt_{{ ds_nodash }}',
        api_version='auto',
        auto_remove= 'success',
        command='run --select stg_weather_data.sql ' ,
        mounts= [ Mount(
            source= 'C:/Users/XPRISTO/Documents/weather-data-project/dbt/my_project',
            target= '/usr/app',
            type= 'bind'
        ),
        Mount(
            source= 'C:/Users/XPRISTO/Documents/weather-data-project/dbt/profiles.yml',
            target= '/root/.dbt/profiles.yml',
            type= 'bind'
        ) 
     ] ,
        working_dir='/usr/app',
        docker_url='unix://var/run/docker.sock',
        network_mode='weather-data-project_my-network' , 
        mount_tmp_dir=False

    )

    






    
    load_data = PythonOperator(
        task_id='load_weather_data',
        python_callable=load_weather_data_task
    )
    
    validate_data = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data_task
    )
    
    end = EmptyOperator(task_id='end')

    # Orchestration
    start >> setup_db >> extract_data >> dbt_pipeline >> load_data >> validate_data >> end