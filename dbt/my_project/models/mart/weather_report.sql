{{ config(
    materialized='table',
    unique_key=['city', 'report_date']
) }}

SELECT 
    city,
    country,
    DATE(observation_time) as report_date,
    
    -- Métriques de température
    ROUND(AVG(temperature), 2) as avg_temperature,
    MAX(temperature) as max_temperature,
    MIN(temperature) as min_temperature,
    
    -- Métriques d'humidité
    ROUND(AVG(humidity), 2) as avg_humidity,
    MAX(humidity) as max_humidity,
    MIN(humidity) as min_humidity,
    
    -- Métriques de vent
    ROUND(AVG(wind_speed), 2) as avg_wind_speed,
    MAX(wind_speed) as max_wind_speed,
    
    -- Métriques de pression
    ROUND(AVG(pressure), 2) as avg_pressure,
    
    -- Comptage
    COUNT(*) as measurements_count,
    
    -- Dernière mise à jour
    MAX(created_at) as last_updated

FROM {{ ref('stg_weather_data') }}  -- Référence au staging nettoyé
GROUP BY city, country, DATE(observation_time)
ORDER BY report_date DESC, city, country