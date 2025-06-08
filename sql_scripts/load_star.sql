DELETE FROM geo_climate_dw.dwh_star.FactAirQuality;
DELETE FROM geo_climate_dw.dwh_star.FactWeatherMetrics;
GO

DELETE FROM geo_climate_dw.dwh_star.DimTime;
DELETE FROM geo_climate_dw.dwh_star.DimDate;
DELETE FROM geo_climate_dw.dwh_star.DimWeatherCondition;
DELETE FROM geo_climate_dw.dwh_star.DimAQCategory;
DELETE FROM geo_climate_dw.dwh_star.DimLocation;
GO

-- ======================================
-- 1. Populating Dimension Tables 
-- ======================================

-- DimLocation
INSERT INTO geo_climate_dw.dwh_star.DimLocation (
    City, Country, Continent, TimeZone, ClimateZone, Latitude, Longitude
)
SELECT DISTINCT
    ci.city_name,
    co.country_name,
    con.continent_name,
    tz.time_zone_id,
    cz.climate_zone_name,
    ci.latitude,
    ci.longitude
FROM geo_climate_dw.dwh_raw.cities ci
JOIN geo_climate_dw.dwh_raw.countries co ON ci.country_id = co.country_id
JOIN geo_climate_dw.dwh_raw.continents con ON ci.continent_id = con.continent_id
JOIN geo_climate_dw.dwh_raw.climate_zones cz ON ci.climate_zone_id = cz.climate_zone_id
JOIN geo_climate_dw.dwh_raw.time_zones tz ON ci.time_zone_id = tz.time_zone_id
WHERE 
    ci.end_date > CAST(GETDATE() AS DATE) AND
    ci.city_name IN (
        SELECT DISTINCT location_name
        FROM geo_climate_dw.dwh_raw.GlobalWeatherRepository
        WHERE end_date > CAST(GETDATE() AS DATE)
    );

-- DimAQCategory
INSERT INTO geo_climate_dw.dwh_star.DimAQCategory (
    Pollutant, Category, MinValue, MaxValue, UnitOfMeasure, CategoryDescription
)
SELECT DISTINCT
    category.pollutant,
    category.category_name,
    category.min_value,
    category.max_value,
    category.unit_of_measure,
    category.category_description
FROM geo_climate_dw.dwh_raw.air_quality_categories category
WHERE category.end_date > CAST(GETDATE() AS DATE);

-- DimWeatherCondition
INSERT INTO geo_climate_dw.dwh_star.DimWeatherCondition (ConditionText, ConditionGroup)
SELECT DISTINCT
    g.condition_text,
    CASE 
        WHEN g.condition_text LIKE '%rain%' THEN 'Rainy'
        WHEN g.condition_text LIKE '%sun%' THEN 'Sunny'
        WHEN g.condition_text LIKE '%cloud%' THEN 'Cloudy'
        WHEN g.condition_text LIKE '%snow%' THEN 'Snowy'
        ELSE 'Other'
    END
FROM geo_climate_dw.dwh_raw.GlobalWeatherRepository g
WHERE g.condition_text IS NOT NULL
AND g.end_date > CAST(GETDATE() AS DATE);

-- DimDate
INSERT INTO dwh_star.DimDate (DimDateKey, FullDate, DayOfWeek, DayOfMonth, Month, MonthName, Year)
SELECT DISTINCT
    CONVERT(INT, FORMAT(last_updated, 'yyyyMMdd')),
    CAST(last_updated AS DATE),
    DATENAME(WEEKDAY, last_updated),
    DAY(last_updated),
    MONTH(last_updated),
    DATENAME(MONTH, last_updated),
    YEAR(last_updated)
FROM dwh_raw.GlobalWeatherRepository
WHERE end_date > CAST(GETDATE() AS DATE);

-- DimTime 
INSERT INTO dwh_star.DimTime (DimTimeKey, FullTime, Hour, Minute)
SELECT DISTINCT
    (DATEPART(HOUR, t.TimeValue) * 100 + DATEPART(MINUTE, t.TimeValue)),
    t.TimeValue,
    DATEPART(HOUR, t.TimeValue),
    DATEPART(MINUTE, t.TimeValue)
FROM (
    SELECT CAST(last_updated AS TIME) AS TimeValue FROM dwh_raw.GlobalWeatherRepository WHERE end_date > CAST(GETDATE() AS DATE)
    UNION
    SELECT TRY_CAST(sunrise AS TIME) FROM dwh_raw.GlobalWeatherRepository WHERE end_date > CAST(GETDATE() AS DATE)
    UNION
    SELECT TRY_CAST(sunset AS TIME) FROM dwh_raw.GlobalWeatherRepository WHERE end_date > CAST(GETDATE() AS DATE)
    UNION
    SELECT TRY_CAST(moonrise AS TIME) FROM dwh_raw.GlobalWeatherRepository WHERE end_date > CAST(GETDATE() AS DATE)
    UNION
    SELECT TRY_CAST(moonset AS TIME) FROM dwh_raw.GlobalWeatherRepository WHERE end_date > CAST(GETDATE() AS DATE)
) t
WHERE t.TimeValue IS NOT NULL
AND NOT EXISTS (
    SELECT 1 FROM dwh_star.DimTime dt
    WHERE dt.DimTimeKey = (DATEPART(HOUR, t.TimeValue) * 100 + DATEPART(MINUTE, t.TimeValue))
);

-- ======================================
-- 2. Populating Fact Tables 
-- ======================================

-- FactWeatherMetrics
INSERT INTO geo_climate_dw.dwh_star.FactWeatherMetrics (
    DimLocationKey, DimLastUpdatedDateKey, DimLastUpdatedTimeKey, DimWeatherConditionKey, DimAQCategoryKey,
    TempCelsius, TempFahrenheit, FeelsLikeCelsius, FeelsLikeFahrenheit,
    WindMph, WindKph, WindDegree, PressureMb, PressureIn, PrecipMm, PrecipIn,
    Humidity, Cloud, VisibilityKm, VisibilityMiles, UVIndex, GustMph, GustKph,
    DimSunriseTimeKey, DimSunsetTimeKey, DimMoonriseTimeKey, DimMoonsetTimeKey, MoonIllumination
)
SELECT
    loc.DimLocationKey,
    dd.DimDateKey,
    dt.DimTimeKey,
    cond.DimWeatherConditionKey,
    aq.DimAQCategoryKey,
    g.temperature_celsius, g.temperature_fahrenheit, g.feels_like_celsius, g.feels_like_fahrenheit,
    g.wind_mph, g.wind_kph, g.wind_degree, g.pressure_mb, g.pressure_in, g.precip_mm, g.precip_in,
    g.humidity, g.cloud, g.visibility_km, g.visibility_miles, g.uv_index, g.gust_mph, g.gust_kph,
    st.DimTimeKey, ss.DimTimeKey, mr.DimTimeKey, ms.DimTimeKey, g.moon_illumination
FROM geo_climate_dw.dwh_raw.GlobalWeatherRepository g
JOIN geo_climate_dw.dwh_star.DimLocation loc ON g.location_name = loc.City
JOIN geo_climate_dw.dwh_star.DimDate dd ON CAST(g.last_updated AS DATE) = dd.FullDate
JOIN geo_climate_dw.dwh_star.DimTime dt ON CAST(g.last_updated AS TIME) = dt.FullTime
JOIN geo_climate_dw.dwh_star.DimWeatherCondition cond ON g.condition_text = cond.ConditionText
LEFT JOIN geo_climate_dw.dwh_star.DimTime st ON g.sunrise = st.FullTime
LEFT JOIN geo_climate_dw.dwh_star.DimTime ss ON g.sunset = ss.FullTime
LEFT JOIN geo_climate_dw.dwh_star.DimTime mr ON g.moonrise = mr.FullTime
LEFT JOIN geo_climate_dw.dwh_star.DimTime ms ON g.moonset = ms.FullTime
LEFT JOIN geo_climate_dw.dwh_star.DimAQCategory aq ON aq.Pollutant = 'PM2.5'
    AND g.air_quality_PM2_5 BETWEEN aq.MinValue AND aq.MaxValue
WHERE g.end_date > CAST(GETDATE() AS DATE)
AND loc.DimLocationKey IS NOT NULL
AND dd.DimDateKey IS NOT NULL
AND dt.DimTimeKey IS NOT NULL
AND cond.DimWeatherConditionKey IS NOT NULL
AND aq.DimAQCategoryKey IS NOT NULL
AND g.temperature_celsius IS NOT NULL
AND g.temperature_fahrenheit IS NOT NULL
AND g.feels_like_celsius IS NOT NULL
AND g.feels_like_fahrenheit IS NOT NULL
AND g.wind_mph IS NOT NULL
AND g.wind_kph IS NOT NULL
AND g.wind_degree IS NOT NULL
AND g.pressure_mb IS NOT NULL
AND g.pressure_in IS NOT NULL
AND g.precip_mm IS NOT NULL
AND g.precip_in IS NOT NULL
AND g.humidity IS NOT NULL
AND g.cloud IS NOT NULL
AND g.visibility_km IS NOT NULL
AND g.visibility_miles IS NOT NULL
AND g.uv_index IS NOT NULL
AND g.gust_mph IS NOT NULL
AND g.gust_kph IS NOT NULL
AND st.DimTimeKey IS NOT NULL
AND ss.DimTimeKey IS NOT NULL
AND mr.DimTimeKey IS NOT NULL
AND ms.DimTimeKey IS NOT NULL
AND g.moon_illumination IS NOT NULL;

-- FactAirQuality
INSERT INTO geo_climate_dw.dwh_star.FactAirQuality (
    DimLocationKey, DimLastUpdatedDateKey, DimLastUpdatedTimeKey,
    DimAQCategoryKey, DimWeatherConditionKey,
    AQ_CarbonMonoxide, AQ_Ozone, AQ_NitrogenDioxide, AQ_SulphurDioxide, AQ_PM25_Raw, AQ_PM10_Raw
)
SELECT
    loc.DimLocationKey,
    dd.DimDateKey,
    dt.DimTimeKey,
    aq.DimAQCategoryKey,
    cond.DimWeatherConditionKey,
    g.air_quality_Carbon_Monoxide,
    g.air_quality_Ozone,
    g.air_quality_Nitrogen_dioxide,
    g.air_quality_Sulphur_dioxide,
    g.air_quality_PM2_5,
    g.air_quality_PM10
FROM geo_climate_dw.dwh_raw.GlobalWeatherRepository g
JOIN geo_climate_dw.dwh_star.DimLocation loc ON g.location_name = loc.City
JOIN geo_climate_dw.dwh_star.DimDate dd ON CAST(g.last_updated AS DATE) = dd.FullDate
JOIN geo_climate_dw.dwh_star.DimTime dt ON CAST(g.last_updated AS TIME) = dt.FullTime
JOIN geo_climate_dw.dwh_star.DimWeatherCondition cond ON g.condition_text = cond.ConditionText
LEFT JOIN geo_climate_dw.dwh_raw.city_air_quality ca
    ON ca.city_id IN (SELECT city_id FROM geo_climate_dw.dwh_raw.cities WHERE city_name = g.location_name AND end_date > CAST(GETDATE() AS DATE))
    AND ca.pollutant = 'PM2.5'
LEFT JOIN geo_climate_dw.dwh_star.DimAQCategory aq
    ON aq.Pollutant = 'PM2.5'
    AND g.air_quality_PM2_5 BETWEEN aq.MinValue AND aq.MaxValue
WHERE g.end_date > CAST(GETDATE() AS DATE)
AND loc.DimLocationKey IS NOT NULL
AND dd.DimDateKey IS NOT NULL
AND dt.DimTimeKey IS NOT NULL
AND aq.DimAQCategoryKey IS NOT NULL
AND cond.DimWeatherConditionKey IS NOT NULL
AND g.air_quality_Carbon_Monoxide IS NOT NULL
AND g.air_quality_Ozone IS NOT NULL
AND g.air_quality_Nitrogen_dioxide IS NOT NULL
AND g.air_quality_Sulphur_dioxide IS NOT NULL
AND g.air_quality_PM2_5 IS NOT NULL
AND g.air_quality_PM10 IS NOT NULL;
GO