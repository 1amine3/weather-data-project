{{ config(
    materialized='table'
) }}

WITH ranked_data AS (
    SELECT 
        id,
        city,
        temperature,
        humidity,
        pressure,
        wind_speed,
        description,
        country,
        region,
        observation_time,
        created_at,
        ROW_NUMBER() OVER (
            PARTITION BY city, country, observation_time 
            ORDER BY created_at DESC
        ) as duplicate_rank
    FROM {{ source('raw_data', 'weather_data') }}
)
SELECT 
    id,
    city,
    temperature,
    humidity,
    pressure,
    wind_speed,
    description,
    country,
    region,
    observation_time,
    created_at
FROM ranked_data
WHERE duplicate_rank = 1