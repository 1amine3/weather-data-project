<h1>Weather Data Project</h1>
<img width="1890" height="912" alt="weather_stack" src="https://github.com/user-attachments/assets/e5829c8d-0193-4793-8988-6f160c59f551" />

</br>
<h2>Aperçu </h2>
Le Weather Data Project est un pipeline de données simple en Python pour collecter, stocker, transformer et visualiser des données météo. Il utilise l'API Weatherstack pour scraper les données, PostgreSQL pour le stockage, DBT pour les transformations, Airflow pour l'automatisation, et Superset pour les dashboards.
Idéal pour un projet d'apprentissage en data engineering.

<h2> Fonctionnalités principales</h2>
<li>Extraction de données météo (actuelles, historiques) via Weatherstack. </li>
<li> Stockage en PostgreSQL. </li>
<li> Transformations avec DBT . </li>
<li> Automatisation via Airflow (DAGs quotidiens). </li>
<li> Visualisation interactive avec Superset . </li>

<h2> Technologies utilisées</h2> 
- Python 3.11  </br>
- PostgreSQL 15   </br>
- DBT   </br>
- Apache Airflow 2.x </br>
- Apache Superset </br>
- Weatherstack API </br>
- Docker & Docker Compose </br>

<h2> Prérequis </h2>

Python 3.11+ – pour exécuter les scripts Python. </br>
PostgreSQL 15+ – pour le stockage des données. </br>
Docker & Docker Compose – pour lancer les conteneurs PostgreSQL, Airflow et Superset. </br>
Git – pour cloner le repository et gérer les versions. </br>

<h2> Installation et Configuration</h2>
<h4> Cloner le repository</h4>
git clone https://github.com/1amine3/weather-data-project.git  </br>
cd weather-data-project
<h2>Créer un environnement Python et installer les dépendances </h2>
<h4> Configurer la clé API Weatherstack</h4>
Ouvrir scrape_data/api_request.py et remplacer "VOTRE_CLE_API" par votre clé.

<h3> Lancer les services Docker</h3>
docker-compose up -d
Cela démarre PostgreSQL, Airflow et Superset.

<h3>Vérifier que tout fonctionne </h3>

Accéder à Airflow : http://localhost:8080

Accéder à Superset : http://localhost:8088

Tester l’API Weatherstack  </br>

<img width="1892" height="893" alt="image" src="https://github.com/user-attachments/assets/7d6ac506-87a9-426f-9f74-ab3772e4ca85" />


