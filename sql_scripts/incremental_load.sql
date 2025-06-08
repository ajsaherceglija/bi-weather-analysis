DECLARE @load_id UNIQUEIDENTIFIER = NEWID();
DECLARE @now DATETIME = GETDATE();

----------------------------------------------------------------------------------
-- Incremental load for dwh_raw.cities
----------------------------------------------------------------------------------

-- 1. Close rows in raw that do not exist anymore in staging 
UPDATE r
SET 
    end_date = @now,
    update_id = @load_id
FROM dwh_raw.cities r
LEFT JOIN dwh_stg.cities s ON r.city_id = s.city_id
WHERE s.city_id IS NULL
  AND r.end_date = '9999-12-31';

-- 2. Close rows in raw that have changes compared to staging 
UPDATE r
SET 
    end_date = @now,
    update_id = @load_id
FROM dwh_raw.cities r
JOIN dwh_stg.cities s ON r.city_id = s.city_id
WHERE r.end_date = '9999-12-31'
  AND (
      r.city_name <> s.city_name OR
      r.country_id <> s.country_id OR
      r.continent_id <> s.continent_id OR
      r.latitude <> s.latitude OR
      r.longitude <> s.longitude OR
      r.time_zone_id <> s.time_zone_id OR
      r.climate_zone_id <> s.climate_zone_id
  );

-- 3. Insert new and updated rows from staging
INSERT INTO dwh_raw.cities (
    city_id, city_name, country_id, continent_id, latitude, longitude, time_zone_id, climate_zone_id,
    input_id, update_id, start_date, end_date, source
)
SELECT
    s.city_id, s.city_name, s.country_id, s.continent_id, s.latitude, s.longitude, s.time_zone_id, s.climate_zone_id,
    @load_id AS input_id,
    NULL AS update_id,
    @now AS start_date,
    CAST('9999-12-31' AS DATE) AS end_date, 1
FROM dwh_stg.cities s
LEFT JOIN dwh_raw.cities r
    ON s.city_id = r.city_id AND r.end_date = '9999-12-31'
WHERE r.city_id IS NULL
   OR (
      r.city_name <> s.city_name OR
      r.country_id <> s.country_id OR
      r.continent_id <> s.continent_id OR
      r.latitude <> s.latitude OR
      r.longitude <> s.longitude OR
      r.time_zone_id <> s.time_zone_id OR
      r.climate_zone_id <> s.climate_zone_id
   );


----------------------------------------------------------------------------------
-- Incremental load for dwh_raw.countries
----------------------------------------------------------------------------------

UPDATE r
SET 
    end_date = @now,
    update_id = @load_id
FROM dwh_raw.countries r
LEFT JOIN dwh_stg.countries s ON r.country_id = s.country_id
WHERE s.country_id IS NULL
  AND r.end_date = '9999-12-31';

UPDATE r
SET 
    end_date = @now,
    update_id = @load_id
FROM dwh_raw.countries r
JOIN dwh_stg.countries s ON r.country_id = s.country_id
WHERE r.end_date = '9999-12-31'
  AND (
      r.country_name <> s.country_name OR
      r.country_code <> s.country_code OR
      r.currency <> s.currency
  );

INSERT INTO dwh_raw.countries (
    country_id, country_name, country_code, currency,
    input_id, update_id, start_date, end_date, source
)
SELECT
    s.country_id, s.country_name, s.country_code, s.currency,
    @load_id, NULL, @now, CAST('9999-12-31' AS DATE), 1
FROM dwh_stg.countries s
LEFT JOIN dwh_raw.countries r
    ON s.country_id = r.country_id AND r.end_date = '9999-12-31'
WHERE r.country_id IS NULL
   OR (
      r.country_name <> s.country_name OR
      r.country_code <> s.country_code OR
      r.currency <> s.currency
   );


----------------------------------------------------------------------------------
-- Incremental load for dwh_raw.continents
----------------------------------------------------------------------------------

UPDATE r
SET 
    end_date = @now,
    update_id = @load_id
FROM dwh_raw.continents r
LEFT JOIN dwh_stg.continents s ON r.continent_id = s.continent_id
WHERE s.continent_id IS NULL
  AND r.end_date = '9999-12-31';

UPDATE r
SET 
    end_date = @now,
    update_id = @load_id
FROM dwh_raw.continents r
JOIN dwh_stg.continents s ON r.continent_id = s.continent_id
WHERE r.end_date = '9999-12-31'
  AND r.continent_name <> s.continent_name;

INSERT INTO dwh_raw.continents (
    continent_id, continent_name,
    input_id, update_id, start_date, end_date, source
)
SELECT
    s.continent_id, s.continent_name,
    @load_id, NULL, @now, CAST('9999-12-31' AS DATE), 1
FROM dwh_stg.continents s
LEFT JOIN dwh_raw.continents r
    ON s.continent_id = r.continent_id AND r.end_date = '9999-12-31'
WHERE r.continent_id IS NULL
   OR r.continent_name <> s.continent_name;


----------------------------------------------------------------------------------
-- Incremental load for dwh_raw.climate_zones
----------------------------------------------------------------------------------

UPDATE r
SET 
    end_date = @now,
    update_id = @load_id
FROM dwh_raw.climate_zones r
LEFT JOIN dwh_stg.climate_zones s ON r.climate_zone_id = s.climate_zone_id
WHERE s.climate_zone_id IS NULL
  AND r.end_date = '9999-12-31';

UPDATE r
SET 
    end_date = @now,
    update_id = @load_id
FROM dwh_raw.climate_zones r
JOIN dwh_stg.climate_zones s ON r.climate_zone_id = s.climate_zone_id
WHERE r.end_date = '9999-12-31'
  AND (
      r.climate_zone_name <> s.climate_zone_name OR
      r.zone_description <> s.zone_description
  );

INSERT INTO dwh_raw.climate_zones (
    climate_zone_id, climate_zone_name, zone_description,
    input_id, update_id, start_date, end_date, source
)
SELECT
    s.climate_zone_id, s.climate_zone_name, s.zone_description,
    @load_id, NULL, @now, CAST('9999-12-31' AS DATE), 1
FROM dwh_stg.climate_zones s
LEFT JOIN dwh_raw.climate_zones r
    ON s.climate_zone_id = r.climate_zone_id AND r.end_date = '9999-12-31'
WHERE r.climate_zone_id IS NULL
   OR (
      r.climate_zone_name <> s.climate_zone_name OR
      r.zone_description <> s.zone_description
   );


----------------------------------------------------------------------------------
-- Incremental load for dwh_raw.time_zones
----------------------------------------------------------------------------------

UPDATE r
SET 
    end_date = @now,
    update_id = @load_id
FROM dwh_raw.time_zones r
LEFT JOIN dwh_stg.time_zones s ON r.time_zone_id = s.time_zone_id
WHERE s.time_zone_id IS NULL
  AND r.end_date = '9999-12-31';

UPDATE r
SET 
    end_date = @now,
    update_id = @load_id
FROM dwh_raw.time_zones r
JOIN dwh_stg.time_zones s ON r.time_zone_id = s.time_zone_id
WHERE r.end_date = '9999-12-31'
  AND r.time_zone_name <> s.time_zone_name;

INSERT INTO dwh_raw.time_zones (
    time_zone_id, time_zone_name,
    input_id, update_id, start_date, end_date, source
)
SELECT
    s.time_zone_id, s.time_zone_name,
    @load_id, NULL, @now, CAST('9999-12-31' AS DATE), 1
FROM dwh_stg.time_zones s
LEFT JOIN dwh_raw.time_zones r
    ON s.time_zone_id = r.time_zone_id AND r.end_date = '9999-12-31'
WHERE r.time_zone_id IS NULL
   OR r.time_zone_name <> s.time_zone_name;


----------------------------------------------------------------------------------
-- Incremental load for dwh_raw.air_quality_categories
----------------------------------------------------------------------------------

UPDATE r
SET 
    end_date = @now,
    update_id = @load_id
FROM dwh_raw.air_quality_categories r
LEFT JOIN dwh_stg.air_quality_categories s ON r.category_aq_id = s.category_aq_id
WHERE s.category_aq_id IS NULL
  AND r.end_date = '9999-12-31';

UPDATE r
SET 
    end_date = @now,
    update_id = @load_id
FROM dwh_raw.air_quality_categories r
JOIN dwh_stg.air_quality_categories s ON r.category_aq_id = s.category_aq_id
WHERE r.end_date = '9999-12-31'
  AND (
      r.category_name <> s.category_name OR
      r.min_value <> s.min_value OR
      r.max_value <> s.max_value OR
      r.unit_of_measure <> s.unit_of_measure OR
      r.pollutant <> s.pollutant OR
      r.category_description <> s.category_description
  );

INSERT INTO dwh_raw.air_quality_categories (
    category_aq_id, category_name, min_value, max_value, unit_of_measure, pollutant, category_description,
    input_id, update_id, start_date, end_date, source
)
SELECT
    s.category_aq_id, s.category_name, s.min_value, s.max_value, s.unit_of_measure, s.pollutant, s.category_description,
    @load_id, NULL, @now, CAST('9999-12-31' AS DATE), 1
FROM dwh_stg.air_quality_categories s
LEFT JOIN dwh_raw.air_quality_categories r
    ON s.category_aq_id = r.category_aq_id AND r.end_date = '9999-12-31'
WHERE r.category_aq_id IS NULL
   OR (
      r.category_name <> s.category_name OR
      r.min_value <> s.min_value OR
      r.max_value <> s.max_value OR
      r.unit_of_measure <> s.unit_of_measure OR
      r.pollutant <> s.pollutant OR
      r.category_description <> s.category_description
   );


----------------------------------------------------------------------------------
-- Incremental load for dwh_raw.city_air_quality
----------------------------------------------------------------------------------

UPDATE r
SET 
    end_date = @now,
    update_id = @load_id
FROM dwh_raw.city_air_quality r
LEFT JOIN dwh_stg.city_air_quality s ON r.profile_id = s.profile_id
WHERE s.profile_id IS NULL
  AND r.end_date = '9999-12-31';

UPDATE r
SET 
    end_date = @now,
    update_id = @load_id
FROM dwh_raw.city_air_quality r
JOIN dwh_stg.city_air_quality s ON r.profile_id = s.profile_id
WHERE r.end_date = '9999-12-31'
  AND (
      r.city_id <> s.city_id OR
      r.pollutant <> s.pollutant OR
      r.typical_value <> s.typical_value OR
      r.typical_category_aq_id <> s.typical_category_aq_id OR
      r.description <> s.description
  );

INSERT INTO dwh_raw.city_air_quality (
    profile_id, city_id, pollutant, typical_value, typical_category_aq_id, description,
    input_id, update_id, start_date, end_date, source
)
SELECT
    s.profile_id, s.city_id, s.pollutant, s.typical_value, s.typical_category_aq_id, s.description,
    @load_id, NULL, @now, CAST('9999-12-31' AS DATE), 1
FROM dwh_stg.city_air_quality s
LEFT JOIN dwh_raw.city_air_quality r
    ON s.profile_id = r.profile_id AND r.end_date = '9999-12-31'
WHERE r.profile_id IS NULL
   OR (
      r.city_id <> s.city_id OR
      r.pollutant <> s.pollutant OR
      r.typical_value <> s.typical_value OR
      r.typical_category_aq_id <> s.typical_category_aq_id OR
      r.description <> s.description
   );


----------------------------------------------------------------------------------
-- Incremental load for dwh_raw.GlobalWeatherRepository
----------------------------------------------------------------------------------

UPDATE r
SET 
    end_date = @now,
    update_id = @load_id
FROM dwh_raw.GlobalWeatherRepository r
LEFT JOIN dwh_stg.GlobalWeatherRepository s 
    ON r.country = s.country 
   AND r.location_name = s.location_name 
   AND r.last_updated = s.last_updated
WHERE s.location_name IS NULL
  AND r.end_date = '9999-12-31';

UPDATE r
SET 
    end_date = @now,
    update_id = @load_id
FROM dwh_raw.GlobalWeatherRepository r
JOIN dwh_stg.GlobalWeatherRepository s 
    ON r.country = s.country 
   AND r.location_name = s.location_name 
   AND r.last_updated = s.last_updated
WHERE r.end_date = '9999-12-31'
  AND (
      r.latitude <> s.latitude OR
      r.longitude <> s.longitude OR
      r.timezone <> s.timezone OR
      r.last_updated_epoch <> s.last_updated_epoch OR
      
      r.temperature_celsius <> s.temperature_celsius OR
      r.temperature_fahrenheit <> s.temperature_fahrenheit OR
      r.condition_text <> s.condition_text OR

      r.wind_mph <> s.wind_mph OR
      r.wind_kph <> s.wind_kph OR
      r.wind_degree <> s.wind_degree OR
      r.wind_direction <> s.wind_direction OR

      r.pressure_mb <> s.pressure_mb OR
      r.pressure_in <> s.pressure_in OR
      r.precip_mm <> s.precip_mm OR
      r.precip_in <> s.precip_in OR

      r.humidity <> s.humidity OR
      r.cloud <> s.cloud OR

      r.feels_like_celsius <> s.feels_like_celsius OR
      r.feels_like_fahrenheit <> s.feels_like_fahrenheit OR

      r.visibility_km <> s.visibility_km OR
      r.visibility_miles <> s.visibility_miles OR

      r.uv_index <> s.uv_index OR

      r.gust_mph <> s.gust_mph OR
      r.gust_kph <> s.gust_kph OR

      r.air_quality_Carbon_Monoxide <> s.air_quality_Carbon_Monoxide OR
      r.air_quality_Ozone <> s.air_quality_Ozone OR
      r.air_quality_Nitrogen_dioxide <> s.air_quality_Nitrogen_dioxide OR
      r.air_quality_Sulphur_dioxide <> s.air_quality_Sulphur_dioxide OR
      r.air_quality_PM2_5 <> s.air_quality_PM2_5 OR
      r.air_quality_PM10 <> s.air_quality_PM10 OR
      r.air_quality_us_epa_index <> s.air_quality_us_epa_index OR
      r.air_quality_gb_defra_index <> s.air_quality_gb_defra_index OR

      r.sunrise <> s.sunrise OR
      r.sunset <> s.sunset OR
      r.moonrise <> s.moonrise OR
      r.moonset <> s.moonset OR
      r.moon_phase <> s.moon_phase OR
      r.moon_illumination <> s.moon_illumination
  );

INSERT INTO dwh_raw.GlobalWeatherRepository (
    country, location_name, latitude, longitude, timezone, last_updated_epoch, last_updated,
    temperature_celsius, temperature_fahrenheit, condition_text, wind_mph, wind_kph, wind_degree, wind_direction,
    pressure_mb, pressure_in, precip_mm, precip_in, humidity, cloud, feels_like_celsius, feels_like_fahrenheit,
    visibility_km, visibility_miles, uv_index, gust_mph, gust_kph,
    air_quality_Carbon_Monoxide, air_quality_Ozone, air_quality_Nitrogen_dioxide, air_quality_Sulphur_dioxide,
    air_quality_PM2_5, air_quality_PM10, air_quality_us_epa_index, air_quality_gb_defra_index,
    sunrise, sunset, moonrise, moonset, moon_phase, moon_illumination,
    input_id, update_id, start_date, end_date, source
)
SELECT
    s.country, s.location_name, s.latitude, s.longitude, s.timezone, s.last_updated_epoch, s.last_updated,
    s.temperature_celsius, s.temperature_fahrenheit, s.condition_text, s.wind_mph, s.wind_kph, s.wind_degree, s.wind_direction,
    s.pressure_mb, s.pressure_in, s.precip_mm, s.precip_in, s.humidity, s.cloud, s.feels_like_celsius, s.feels_like_fahrenheit,
    s.visibility_km, s.visibility_miles, s.uv_index, s.gust_mph, s.gust_kph,
    s.air_quality_Carbon_Monoxide, s.air_quality_Ozone, s.air_quality_Nitrogen_dioxide, s.air_quality_Sulphur_dioxide,
    s.air_quality_PM2_5, s.air_quality_PM10, s.air_quality_us_epa_index, s.air_quality_gb_defra_index,
    s.sunrise, s.sunset, s.moonrise, s.moonset, s.moon_phase, s.moon_illumination,
    @load_id, NULL, @now, CAST('9999-12-31' AS DATE), 2
FROM dwh_stg.GlobalWeatherRepository s
LEFT JOIN dwh_raw.GlobalWeatherRepository r
    ON s.country = r.country 
   AND s.location_name = r.location_name 
   AND s.last_updated = r.last_updated
   AND r.end_date = '9999-12-31'
WHERE r.country IS NULL
  AND s.location_name IS NOT NULL
  AND s.temperature_celsius IS NOT NULL
  AND s.last_updated IS NOT NULL;  
