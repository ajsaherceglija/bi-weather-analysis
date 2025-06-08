DROP TABLE IF EXISTS dwh_stg.air_quality_categories;
GO
DROP TABLE IF EXISTS dwh_stg.cities;
GO
DROP TABLE IF EXISTS dwh_stg.city_air_quality;
GO
DROP TABLE IF EXISTS dwh_stg.climate_zones;
GO
DROP TABLE IF EXISTS dwh_stg.continents;
GO
DROP TABLE IF EXISTS dwh_stg.countries;
GO
DROP TABLE IF EXISTS dwh_stg.time_zones;
GO
DROP TABLE IF EXISTS dwh_stg.GlobalWeatherRepository;
GO

SELECT * INTO dwh_stg.air_quality_categories FROM geo_climate.dbo.air_quality_categories;
GO
SELECT * INTO dwh_stg.cities FROM geo_climate.dbo.cities;
GO
SELECT * INTO dwh_stg.city_air_quality FROM geo_climate.dbo.city_air_quality;
GO
SELECT * INTO dwh_stg.climate_zones FROM geo_climate.dbo.climate_zones;
GO
SELECT * INTO dwh_stg.continents FROM geo_climate.dbo.continents;
GO
SELECT * INTO dwh_stg.countries FROM geo_climate.dbo.countries;
GO
SELECT * INTO dwh_stg.time_zones FROM geo_climate.dbo.time_zones;
GO
SELECT * INTO dwh_stg.GlobalWeatherRepository FROM global_weather_repository.dbo.GlobalWeatherRepository;
GO
