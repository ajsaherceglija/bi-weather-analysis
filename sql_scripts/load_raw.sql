DROP TABLE IF EXISTS dwh_raw.air_quality_categories;
DROP TABLE IF EXISTS dwh_raw.cities;
DROP TABLE IF EXISTS dwh_raw.city_air_quality;
DROP TABLE IF EXISTS dwh_raw.climate_zones;
DROP TABLE IF EXISTS dwh_raw.continents;
DROP TABLE IF EXISTS dwh_raw.countries;
DROP TABLE IF EXISTS dwh_raw.time_zones;
DROP TABLE IF EXISTS dwh_raw.GlobalWeatherRepository;
GO

DECLARE @input_id UNIQUEIDENTIFIER = NEWID();
DECLARE @start_date DATETIME = GETDATE();
DECLARE @end_date DATE = '9999-12-31';

-- Cities 
SELECT 
    *,
    @input_id AS input_id,
    CAST(NULL AS UNIQUEIDENTIFIER) AS update_id,
    @start_date AS start_date,
    @end_date AS end_date,
    1 AS source
INTO dwh_raw.cities
FROM dwh_stg.cities
WHERE city_name IS NOT NULL;

-- Countries 
SELECT 
    *,
    @input_id AS input_id,
    CAST(NULL AS UNIQUEIDENTIFIER) AS update_id,
    @start_date AS start_date,
    @end_date AS end_date,
    1 AS source
INTO dwh_raw.countries
FROM dwh_stg.countries
WHERE country_name IS NOT NULL;

-- Continents 
SELECT 
    *,
    @input_id AS input_id,
    CAST(NULL AS UNIQUEIDENTIFIER) AS update_id,
    @start_date AS start_date,
    @end_date AS end_date,
    1 AS source
INTO dwh_raw.continents
FROM dwh_stg.continents
WHERE continent_name IS NOT NULL;

-- Climate Zones 
SELECT 
    *,
    @input_id AS input_id,
    CAST(NULL AS UNIQUEIDENTIFIER) AS update_id,
    @start_date AS start_date,
    @end_date AS end_date,
    1 AS source
INTO dwh_raw.climate_zones
FROM dwh_stg.climate_zones
WHERE climate_zone_id IS NOT NULL;

-- Time Zones 
SELECT 
    *,
    @input_id AS input_id,
    CAST(NULL AS UNIQUEIDENTIFIER) AS update_id,
    @start_date AS start_date,
    @end_date AS end_date,
    1 AS source
INTO dwh_raw.time_zones
FROM dwh_stg.time_zones
WHERE time_zone_id IS NOT NULL;

-- Air Quality Categories 
SELECT 
    *,
    @input_id AS input_id,
    CAST(NULL AS UNIQUEIDENTIFIER) AS update_id,
    @start_date AS start_date,
    @end_date AS end_date,
    1 AS source
INTO dwh_raw.air_quality_categories
FROM dwh_stg.air_quality_categories
WHERE category_name IS NOT NULL AND pollutant IS NOT NULL;

-- City Air Quality 
SELECT 
    *,
    @input_id AS input_id,
    CAST(NULL AS UNIQUEIDENTIFIER) AS update_id,
    @start_date AS start_date,
    @end_date AS end_date,
    1 AS source
INTO dwh_raw.city_air_quality
FROM dwh_stg.city_air_quality
WHERE pollutant IS NOT NULL;

-- Global Weather Repository 
SELECT 
    *,
    @input_id AS input_id,
    CAST(NULL AS UNIQUEIDENTIFIER) AS update_id,
    @start_date AS start_date,
    @end_date AS end_date,
    2 AS source
INTO dwh_raw.GlobalWeatherRepository
FROM dwh_stg.GlobalWeatherRepository
WHERE location_name IS NOT NULL 
  AND temperature_celsius IS NOT NULL
  AND last_updated IS NOT NULL;
  GO