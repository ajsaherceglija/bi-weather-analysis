USE [master]
GO
/****** Object:  Database [geo_climate_dw]    Script Date: 6/8/2025 5:22:34 PM ******/
CREATE DATABASE [geo_climate_dw]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'geo_climate_dw', FILENAME = N'D:\Program Files\MSSQL16.SQLEXPRESS\MSSQL\DATA\geo_climate_dw.mdf' , SIZE = 139264KB , MAXSIZE = UNLIMITED, FILEGROWTH = 65536KB )
 LOG ON 
( NAME = N'geo_climate_dw_log', FILENAME = N'D:\Program Files\MSSQL16.SQLEXPRESS\MSSQL\DATA\geo_climate_dw_log.ldf' , SIZE = 270336KB , MAXSIZE = 2048GB , FILEGROWTH = 65536KB )
 WITH CATALOG_COLLATION = DATABASE_DEFAULT, LEDGER = OFF
GO
ALTER DATABASE [geo_climate_dw] SET COMPATIBILITY_LEVEL = 160
GO
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [geo_climate_dw].[dbo].[sp_fulltext_database] @action = 'enable'
end
GO
ALTER DATABASE [geo_climate_dw] SET ANSI_NULL_DEFAULT OFF 
GO
ALTER DATABASE [geo_climate_dw] SET ANSI_NULLS OFF 
GO
ALTER DATABASE [geo_climate_dw] SET ANSI_PADDING OFF 
GO
ALTER DATABASE [geo_climate_dw] SET ANSI_WARNINGS OFF 
GO
ALTER DATABASE [geo_climate_dw] SET ARITHABORT OFF 
GO
ALTER DATABASE [geo_climate_dw] SET AUTO_CLOSE ON 
GO
ALTER DATABASE [geo_climate_dw] SET AUTO_SHRINK OFF 
GO
ALTER DATABASE [geo_climate_dw] SET AUTO_UPDATE_STATISTICS ON 
GO
ALTER DATABASE [geo_climate_dw] SET CURSOR_CLOSE_ON_COMMIT OFF 
GO
ALTER DATABASE [geo_climate_dw] SET CURSOR_DEFAULT  GLOBAL 
GO
ALTER DATABASE [geo_climate_dw] SET CONCAT_NULL_YIELDS_NULL OFF 
GO
ALTER DATABASE [geo_climate_dw] SET NUMERIC_ROUNDABORT OFF 
GO
ALTER DATABASE [geo_climate_dw] SET QUOTED_IDENTIFIER OFF 
GO
ALTER DATABASE [geo_climate_dw] SET RECURSIVE_TRIGGERS OFF 
GO
ALTER DATABASE [geo_climate_dw] SET  ENABLE_BROKER 
GO
ALTER DATABASE [geo_climate_dw] SET AUTO_UPDATE_STATISTICS_ASYNC OFF 
GO
ALTER DATABASE [geo_climate_dw] SET DATE_CORRELATION_OPTIMIZATION OFF 
GO
ALTER DATABASE [geo_climate_dw] SET TRUSTWORTHY OFF 
GO
ALTER DATABASE [geo_climate_dw] SET ALLOW_SNAPSHOT_ISOLATION OFF 
GO
ALTER DATABASE [geo_climate_dw] SET PARAMETERIZATION SIMPLE 
GO
ALTER DATABASE [geo_climate_dw] SET READ_COMMITTED_SNAPSHOT OFF 
GO
ALTER DATABASE [geo_climate_dw] SET HONOR_BROKER_PRIORITY OFF 
GO
ALTER DATABASE [geo_climate_dw] SET RECOVERY SIMPLE 
GO
ALTER DATABASE [geo_climate_dw] SET  MULTI_USER 
GO
ALTER DATABASE [geo_climate_dw] SET PAGE_VERIFY CHECKSUM  
GO
ALTER DATABASE [geo_climate_dw] SET DB_CHAINING OFF 
GO
ALTER DATABASE [geo_climate_dw] SET FILESTREAM( NON_TRANSACTED_ACCESS = OFF ) 
GO
ALTER DATABASE [geo_climate_dw] SET TARGET_RECOVERY_TIME = 60 SECONDS 
GO
ALTER DATABASE [geo_climate_dw] SET DELAYED_DURABILITY = DISABLED 
GO
ALTER DATABASE [geo_climate_dw] SET ACCELERATED_DATABASE_RECOVERY = OFF  
GO
ALTER DATABASE [geo_climate_dw] SET QUERY_STORE = ON
GO
ALTER DATABASE [geo_climate_dw] SET QUERY_STORE (OPERATION_MODE = READ_WRITE, CLEANUP_POLICY = (STALE_QUERY_THRESHOLD_DAYS = 30), DATA_FLUSH_INTERVAL_SECONDS = 900, INTERVAL_LENGTH_MINUTES = 60, MAX_STORAGE_SIZE_MB = 1000, QUERY_CAPTURE_MODE = AUTO, SIZE_BASED_CLEANUP_MODE = AUTO, MAX_PLANS_PER_QUERY = 200, WAIT_STATS_CAPTURE_MODE = ON)
GO
USE [geo_climate_dw]
GO
/****** Object:  Schema [dwh_raw]    Script Date: 6/8/2025 5:22:35 PM ******/
CREATE SCHEMA [dwh_raw]
GO
/****** Object:  Schema [dwh_star]    Script Date: 6/8/2025 5:22:35 PM ******/
CREATE SCHEMA [dwh_star]
GO
/****** Object:  Schema [dwh_stg]    Script Date: 6/8/2025 5:22:35 PM ******/
CREATE SCHEMA [dwh_stg]
GO
/****** Object:  Table [dwh_raw].[air_quality_categories]    Script Date: 6/8/2025 5:22:35 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dwh_raw].[air_quality_categories](
	[category_aq_id] [tinyint] NOT NULL,
	[category_name] [nvarchar](50) NOT NULL,
	[min_value] [float] NOT NULL,
	[max_value] [float] NOT NULL,
	[unit_of_measure] [nvarchar](50) NOT NULL,
	[pollutant] [nvarchar](50) NOT NULL,
	[category_description] [nvarchar](100) NOT NULL,
	[input_id] [uniqueidentifier] NULL,
	[update_id] [uniqueidentifier] NULL,
	[start_date] [datetime] NULL,
	[end_date] [date] NULL,
	[source] [int] NOT NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dwh_raw].[cities]    Script Date: 6/8/2025 5:22:35 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dwh_raw].[cities](
	[city_id] [smallint] NOT NULL,
	[city_name] [nvarchar](50) NOT NULL,
	[country_id] [tinyint] NULL,
	[continent_id] [tinyint] NOT NULL,
	[latitude] [float] NOT NULL,
	[longitude] [float] NOT NULL,
	[time_zone_id] [smallint] NULL,
	[climate_zone_id] [tinyint] NULL,
	[input_id] [uniqueidentifier] NULL,
	[update_id] [uniqueidentifier] NULL,
	[start_date] [datetime] NULL,
	[end_date] [date] NULL,
	[source] [int] NOT NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dwh_raw].[city_air_quality]    Script Date: 6/8/2025 5:22:35 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dwh_raw].[city_air_quality](
	[profile_id] [smallint] NOT NULL,
	[city_id] [smallint] NOT NULL,
	[pollutant] [nvarchar](50) NOT NULL,
	[typical_value] [nvarchar](50) NOT NULL,
	[typical_category_aq_id] [tinyint] NOT NULL,
	[description] [nvarchar](50) NOT NULL,
	[input_id] [uniqueidentifier] NULL,
	[update_id] [uniqueidentifier] NULL,
	[start_date] [datetime] NULL,
	[end_date] [date] NULL,
	[source] [int] NOT NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dwh_raw].[climate_zones]    Script Date: 6/8/2025 5:22:35 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dwh_raw].[climate_zones](
	[climate_zone_id] [tinyint] NOT NULL,
	[climate_zone_name] [nvarchar](50) NOT NULL,
	[zone_description] [nvarchar](200) NOT NULL,
	[input_id] [uniqueidentifier] NULL,
	[update_id] [uniqueidentifier] NULL,
	[start_date] [datetime] NULL,
	[end_date] [date] NULL,
	[source] [int] NOT NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dwh_raw].[continents]    Script Date: 6/8/2025 5:22:35 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dwh_raw].[continents](
	[continent_id] [tinyint] NOT NULL,
	[continent_name] [nvarchar](50) NOT NULL,
	[input_id] [uniqueidentifier] NULL,
	[update_id] [uniqueidentifier] NULL,
	[start_date] [datetime] NULL,
	[end_date] [date] NULL,
	[source] [int] NOT NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dwh_raw].[countries]    Script Date: 6/8/2025 5:22:35 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dwh_raw].[countries](
	[country_id] [tinyint] NOT NULL,
	[country_name] [nvarchar](50) NULL,
	[country_code] [nvarchar](50) NULL,
	[currency] [nvarchar](50) NULL,
	[input_id] [uniqueidentifier] NULL,
	[update_id] [uniqueidentifier] NULL,
	[start_date] [datetime] NULL,
	[end_date] [date] NULL,
	[source] [int] NOT NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dwh_raw].[GlobalWeatherRepository]    Script Date: 6/8/2025 5:22:35 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dwh_raw].[GlobalWeatherRepository](
	[country] [nvarchar](50) NOT NULL,
	[location_name] [nvarchar](50) NOT NULL,
	[latitude] [float] NOT NULL,
	[longitude] [float] NOT NULL,
	[timezone] [nvarchar](50) NOT NULL,
	[last_updated_epoch] [int] NOT NULL,
	[last_updated] [datetime2](7) NOT NULL,
	[temperature_celsius] [float] NULL,
	[temperature_fahrenheit] [float] NULL,
	[condition_text] [nvarchar](50) NOT NULL,
	[wind_mph] [float] NOT NULL,
	[wind_kph] [float] NOT NULL,
	[wind_degree] [smallint] NOT NULL,
	[wind_direction] [nvarchar](50) NOT NULL,
	[pressure_mb] [float] NOT NULL,
	[pressure_in] [float] NOT NULL,
	[precip_mm] [float] NOT NULL,
	[precip_in] [float] NOT NULL,
	[humidity] [tinyint] NOT NULL,
	[cloud] [tinyint] NOT NULL,
	[feels_like_celsius] [float] NULL,
	[feels_like_fahrenheit] [float] NULL,
	[visibility_km] [float] NOT NULL,
	[visibility_miles] [float] NOT NULL,
	[uv_index] [float] NOT NULL,
	[gust_mph] [float] NOT NULL,
	[gust_kph] [float] NOT NULL,
	[air_quality_Carbon_Monoxide] [float] NULL,
	[air_quality_Ozone] [float] NULL,
	[air_quality_Nitrogen_dioxide] [float] NULL,
	[air_quality_Sulphur_dioxide] [float] NULL,
	[air_quality_PM2_5] [float] NULL,
	[air_quality_PM10] [float] NULL,
	[air_quality_us_epa_index] [tinyint] NULL,
	[air_quality_gb_defra_index] [tinyint] NULL,
	[sunrise] [time](7) NULL,
	[sunset] [time](7) NULL,
	[moonrise] [time](7) NULL,
	[moonset] [time](7) NULL,
	[moon_phase] [nvarchar](50) NULL,
	[moon_illumination] [tinyint] NULL,
	[input_id] [uniqueidentifier] NULL,
	[update_id] [uniqueidentifier] NULL,
	[start_date] [datetime] NULL,
	[end_date] [date] NULL,
	[source] [int] NOT NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dwh_raw].[time_zones]    Script Date: 6/8/2025 5:22:35 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dwh_raw].[time_zones](
	[time_zone_id] [smallint] NOT NULL,
	[time_zone_name] [nvarchar](50) NOT NULL,
	[utc_offset] [nvarchar](50) NOT NULL,
	[input_id] [uniqueidentifier] NULL,
	[update_id] [uniqueidentifier] NULL,
	[start_date] [datetime] NULL,
	[end_date] [date] NULL,
	[source] [int] NOT NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dwh_star].[DimAQCategory]    Script Date: 6/8/2025 5:22:35 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dwh_star].[DimAQCategory](
	[DimAQCategoryKey] [int] IDENTITY(1,1) NOT NULL,
	[Pollutant] [nvarchar](50) NULL,
	[Category] [nvarchar](100) NULL,
	[MinValue] [decimal](10, 2) NULL,
	[MaxValue] [decimal](10, 2) NULL,
	[UnitOfMeasure] [nvarchar](50) NULL,
	[CategoryDescription] [nvarchar](500) NULL,
PRIMARY KEY CLUSTERED 
(
	[DimAQCategoryKey] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dwh_star].[DimDate]    Script Date: 6/8/2025 5:22:35 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dwh_star].[DimDate](
	[DimDateKey] [int] NOT NULL,
	[FullDate] [date] NOT NULL,
	[DayOfWeek] [nvarchar](10) NULL,
	[DayOfMonth] [int] NULL,
	[Month] [int] NULL,
	[MonthName] [nvarchar](20) NULL,
	[Year] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[DimDateKey] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dwh_star].[DimLocation]    Script Date: 6/8/2025 5:22:35 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dwh_star].[DimLocation](
	[DimLocationKey] [int] IDENTITY(1,1) NOT NULL,
	[City] [nvarchar](255) NULL,
	[Country] [nvarchar](255) NULL,
	[Continent] [nvarchar](100) NULL,
	[TimeZone] [nvarchar](255) NULL,
	[ClimateZone] [nvarchar](255) NULL,
	[Latitude] [decimal](9, 6) NULL,
	[Longitude] [decimal](9, 6) NULL,
PRIMARY KEY CLUSTERED 
(
	[DimLocationKey] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dwh_star].[DimTime]    Script Date: 6/8/2025 5:22:35 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dwh_star].[DimTime](
	[DimTimeKey] [int] NOT NULL,
	[FullTime] [time](7) NOT NULL,
	[Hour] [int] NULL,
	[Minute] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[DimTimeKey] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dwh_star].[DimWeatherCondition]    Script Date: 6/8/2025 5:22:35 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dwh_star].[DimWeatherCondition](
	[DimWeatherConditionKey] [int] IDENTITY(1,1) NOT NULL,
	[ConditionText] [nvarchar](255) NULL,
	[ConditionGroup] [nvarchar](50) NULL,
PRIMARY KEY CLUSTERED 
(
	[DimWeatherConditionKey] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dwh_star].[FactAirQuality]    Script Date: 6/8/2025 5:22:35 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dwh_star].[FactAirQuality](
	[FactAirQualityKey] [int] IDENTITY(1,1) NOT NULL,
	[DimLocationKey] [int] NOT NULL,
	[DimLastUpdatedDateKey] [int] NOT NULL,
	[DimLastUpdatedTimeKey] [int] NOT NULL,
	[DimAQCategoryKey] [int] NOT NULL,
	[DimWeatherConditionKey] [int] NOT NULL,
	[AQ_CarbonMonoxide] [decimal](10, 4) NULL,
	[AQ_Ozone] [decimal](10, 4) NULL,
	[AQ_NitrogenDioxide] [decimal](10, 4) NULL,
	[AQ_SulphurDioxide] [decimal](10, 4) NULL,
	[AQ_PM25_Raw] [decimal](10, 4) NULL,
	[AQ_PM10_Raw] [decimal](10, 4) NULL,
	[AQ_PM25_TypicalValue] [decimal](10, 2) NULL,
PRIMARY KEY CLUSTERED 
(
	[FactAirQualityKey] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dwh_star].[FactWeatherMetrics]    Script Date: 6/8/2025 5:22:35 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dwh_star].[FactWeatherMetrics](
	[FactWeatherMetricsKey] [int] IDENTITY(1,1) NOT NULL,
	[DimLocationKey] [int] NOT NULL,
	[DimLastUpdatedDateKey] [int] NOT NULL,
	[DimLastUpdatedTimeKey] [int] NOT NULL,
	[DimWeatherConditionKey] [int] NOT NULL,
	[DimAQCategoryKey] [int] NOT NULL,
	[TempCelsius] [decimal](5, 2) NULL,
	[TempFahrenheit] [decimal](5, 2) NULL,
	[FeelsLikeCelsius] [decimal](5, 2) NULL,
	[FeelsLikeFahrenheit] [decimal](5, 2) NULL,
	[WindMph] [decimal](6, 2) NULL,
	[WindKph] [decimal](6, 2) NULL,
	[WindDegree] [int] NULL,
	[PressureMb] [decimal](7, 2) NULL,
	[PressureIn] [decimal](7, 2) NULL,
	[PrecipMm] [decimal](6, 2) NULL,
	[PrecipIn] [decimal](6, 2) NULL,
	[Humidity] [int] NULL,
	[Cloud] [int] NULL,
	[VisibilityKm] [decimal](5, 2) NULL,
	[VisibilityMiles] [decimal](5, 2) NULL,
	[UVIndex] [int] NULL,
	[GustMph] [decimal](6, 2) NULL,
	[GustKph] [decimal](6, 2) NULL,
	[DimSunriseTimeKey] [int] NULL,
	[DimSunsetTimeKey] [int] NULL,
	[DimMoonriseTimeKey] [int] NULL,
	[DimMoonsetTimeKey] [int] NULL,
	[MoonIllumination] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[FactWeatherMetricsKey] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dwh_stg].[air_quality_categories]    Script Date: 6/8/2025 5:22:35 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dwh_stg].[air_quality_categories](
	[category_aq_id] [tinyint] NOT NULL,
	[category_name] [nvarchar](50) NOT NULL,
	[min_value] [float] NOT NULL,
	[max_value] [float] NOT NULL,
	[unit_of_measure] [nvarchar](50) NOT NULL,
	[pollutant] [nvarchar](50) NOT NULL,
	[category_description] [nvarchar](100) NOT NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dwh_stg].[cities]    Script Date: 6/8/2025 5:22:35 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dwh_stg].[cities](
	[city_id] [smallint] NOT NULL,
	[city_name] [nvarchar](50) NOT NULL,
	[country_id] [tinyint] NULL,
	[continent_id] [tinyint] NOT NULL,
	[latitude] [float] NOT NULL,
	[longitude] [float] NOT NULL,
	[time_zone_id] [smallint] NULL,
	[climate_zone_id] [tinyint] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dwh_stg].[city_air_quality]    Script Date: 6/8/2025 5:22:35 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dwh_stg].[city_air_quality](
	[profile_id] [smallint] NOT NULL,
	[city_id] [smallint] NOT NULL,
	[pollutant] [nvarchar](50) NOT NULL,
	[typical_value] [nvarchar](50) NOT NULL,
	[typical_category_aq_id] [tinyint] NOT NULL,
	[description] [nvarchar](50) NOT NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dwh_stg].[climate_zones]    Script Date: 6/8/2025 5:22:35 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dwh_stg].[climate_zones](
	[climate_zone_id] [tinyint] NOT NULL,
	[climate_zone_name] [nvarchar](50) NOT NULL,
	[zone_description] [nvarchar](200) NOT NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dwh_stg].[continents]    Script Date: 6/8/2025 5:22:35 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dwh_stg].[continents](
	[continent_id] [tinyint] NOT NULL,
	[continent_name] [nvarchar](50) NOT NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dwh_stg].[countries]    Script Date: 6/8/2025 5:22:35 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dwh_stg].[countries](
	[country_id] [tinyint] NOT NULL,
	[country_name] [nvarchar](50) NULL,
	[country_code] [nvarchar](50) NULL,
	[currency] [nvarchar](50) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dwh_stg].[GlobalWeatherRepository]    Script Date: 6/8/2025 5:22:35 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dwh_stg].[GlobalWeatherRepository](
	[country] [nvarchar](50) NOT NULL,
	[location_name] [nvarchar](50) NOT NULL,
	[latitude] [float] NOT NULL,
	[longitude] [float] NOT NULL,
	[timezone] [nvarchar](50) NOT NULL,
	[last_updated_epoch] [int] NOT NULL,
	[last_updated] [datetime2](7) NOT NULL,
	[temperature_celsius] [float] NULL,
	[temperature_fahrenheit] [float] NULL,
	[condition_text] [nvarchar](50) NOT NULL,
	[wind_mph] [float] NOT NULL,
	[wind_kph] [float] NOT NULL,
	[wind_degree] [smallint] NOT NULL,
	[wind_direction] [nvarchar](50) NOT NULL,
	[pressure_mb] [float] NOT NULL,
	[pressure_in] [float] NOT NULL,
	[precip_mm] [float] NOT NULL,
	[precip_in] [float] NOT NULL,
	[humidity] [tinyint] NOT NULL,
	[cloud] [tinyint] NOT NULL,
	[feels_like_celsius] [float] NULL,
	[feels_like_fahrenheit] [float] NULL,
	[visibility_km] [float] NOT NULL,
	[visibility_miles] [float] NOT NULL,
	[uv_index] [float] NOT NULL,
	[gust_mph] [float] NOT NULL,
	[gust_kph] [float] NOT NULL,
	[air_quality_Carbon_Monoxide] [float] NULL,
	[air_quality_Ozone] [float] NULL,
	[air_quality_Nitrogen_dioxide] [float] NULL,
	[air_quality_Sulphur_dioxide] [float] NULL,
	[air_quality_PM2_5] [float] NULL,
	[air_quality_PM10] [float] NULL,
	[air_quality_us_epa_index] [tinyint] NULL,
	[air_quality_gb_defra_index] [tinyint] NULL,
	[sunrise] [time](7) NULL,
	[sunset] [time](7) NULL,
	[moonrise] [time](7) NULL,
	[moonset] [time](7) NULL,
	[moon_phase] [nvarchar](50) NULL,
	[moon_illumination] [tinyint] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dwh_stg].[time_zones]    Script Date: 6/8/2025 5:22:35 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dwh_stg].[time_zones](
	[time_zone_id] [smallint] NOT NULL,
	[time_zone_name] [nvarchar](50) NOT NULL,
	[utc_offset] [nvarchar](50) NOT NULL
) ON [PRIMARY]
GO
ALTER TABLE [dwh_star].[FactAirQuality]  WITH CHECK ADD  CONSTRAINT [FK_AQ_AQCategory] FOREIGN KEY([DimAQCategoryKey])
REFERENCES [dwh_star].[DimAQCategory] ([DimAQCategoryKey])
GO
ALTER TABLE [dwh_star].[FactAirQuality] CHECK CONSTRAINT [FK_AQ_AQCategory]
GO
ALTER TABLE [dwh_star].[FactAirQuality]  WITH CHECK ADD  CONSTRAINT [FK_AQ_Date] FOREIGN KEY([DimLastUpdatedDateKey])
REFERENCES [dwh_star].[DimDate] ([DimDateKey])
GO
ALTER TABLE [dwh_star].[FactAirQuality] CHECK CONSTRAINT [FK_AQ_Date]
GO
ALTER TABLE [dwh_star].[FactAirQuality]  WITH CHECK ADD  CONSTRAINT [FK_AQ_Location] FOREIGN KEY([DimLocationKey])
REFERENCES [dwh_star].[DimLocation] ([DimLocationKey])
GO
ALTER TABLE [dwh_star].[FactAirQuality] CHECK CONSTRAINT [FK_AQ_Location]
GO
ALTER TABLE [dwh_star].[FactAirQuality]  WITH CHECK ADD  CONSTRAINT [FK_AQ_Time] FOREIGN KEY([DimLastUpdatedTimeKey])
REFERENCES [dwh_star].[DimTime] ([DimTimeKey])
GO
ALTER TABLE [dwh_star].[FactAirQuality] CHECK CONSTRAINT [FK_AQ_Time]
GO
ALTER TABLE [dwh_star].[FactAirQuality]  WITH CHECK ADD  CONSTRAINT [FK_AQ_WeatherCondition] FOREIGN KEY([DimWeatherConditionKey])
REFERENCES [dwh_star].[DimWeatherCondition] ([DimWeatherConditionKey])
GO
ALTER TABLE [dwh_star].[FactAirQuality] CHECK CONSTRAINT [FK_AQ_WeatherCondition]
GO
ALTER TABLE [dwh_star].[FactWeatherMetrics]  WITH CHECK ADD  CONSTRAINT [FK_WeatherMetrics_AQCategory] FOREIGN KEY([DimAQCategoryKey])
REFERENCES [dwh_star].[DimAQCategory] ([DimAQCategoryKey])
GO
ALTER TABLE [dwh_star].[FactWeatherMetrics] CHECK CONSTRAINT [FK_WeatherMetrics_AQCategory]
GO
ALTER TABLE [dwh_star].[FactWeatherMetrics]  WITH CHECK ADD  CONSTRAINT [FK_WeatherMetrics_Date] FOREIGN KEY([DimLastUpdatedDateKey])
REFERENCES [dwh_star].[DimDate] ([DimDateKey])
GO
ALTER TABLE [dwh_star].[FactWeatherMetrics] CHECK CONSTRAINT [FK_WeatherMetrics_Date]
GO
ALTER TABLE [dwh_star].[FactWeatherMetrics]  WITH CHECK ADD  CONSTRAINT [FK_WeatherMetrics_Location] FOREIGN KEY([DimLocationKey])
REFERENCES [dwh_star].[DimLocation] ([DimLocationKey])
GO
ALTER TABLE [dwh_star].[FactWeatherMetrics] CHECK CONSTRAINT [FK_WeatherMetrics_Location]
GO
ALTER TABLE [dwh_star].[FactWeatherMetrics]  WITH CHECK ADD  CONSTRAINT [FK_WeatherMetrics_MoonriseTime] FOREIGN KEY([DimMoonriseTimeKey])
REFERENCES [dwh_star].[DimTime] ([DimTimeKey])
GO
ALTER TABLE [dwh_star].[FactWeatherMetrics] CHECK CONSTRAINT [FK_WeatherMetrics_MoonriseTime]
GO
ALTER TABLE [dwh_star].[FactWeatherMetrics]  WITH CHECK ADD  CONSTRAINT [FK_WeatherMetrics_MoonsetTime] FOREIGN KEY([DimMoonsetTimeKey])
REFERENCES [dwh_star].[DimTime] ([DimTimeKey])
GO
ALTER TABLE [dwh_star].[FactWeatherMetrics] CHECK CONSTRAINT [FK_WeatherMetrics_MoonsetTime]
GO
ALTER TABLE [dwh_star].[FactWeatherMetrics]  WITH CHECK ADD  CONSTRAINT [FK_WeatherMetrics_SunriseTime] FOREIGN KEY([DimSunriseTimeKey])
REFERENCES [dwh_star].[DimTime] ([DimTimeKey])
GO
ALTER TABLE [dwh_star].[FactWeatherMetrics] CHECK CONSTRAINT [FK_WeatherMetrics_SunriseTime]
GO
ALTER TABLE [dwh_star].[FactWeatherMetrics]  WITH CHECK ADD  CONSTRAINT [FK_WeatherMetrics_SunsetTime] FOREIGN KEY([DimSunsetTimeKey])
REFERENCES [dwh_star].[DimTime] ([DimTimeKey])
GO
ALTER TABLE [dwh_star].[FactWeatherMetrics] CHECK CONSTRAINT [FK_WeatherMetrics_SunsetTime]
GO
ALTER TABLE [dwh_star].[FactWeatherMetrics]  WITH CHECK ADD  CONSTRAINT [FK_WeatherMetrics_Time] FOREIGN KEY([DimLastUpdatedTimeKey])
REFERENCES [dwh_star].[DimTime] ([DimTimeKey])
GO
ALTER TABLE [dwh_star].[FactWeatherMetrics] CHECK CONSTRAINT [FK_WeatherMetrics_Time]
GO
ALTER TABLE [dwh_star].[FactWeatherMetrics]  WITH CHECK ADD  CONSTRAINT [FK_WeatherMetrics_WeatherCondition] FOREIGN KEY([DimWeatherConditionKey])
REFERENCES [dwh_star].[DimWeatherCondition] ([DimWeatherConditionKey])
GO
ALTER TABLE [dwh_star].[FactWeatherMetrics] CHECK CONSTRAINT [FK_WeatherMetrics_WeatherCondition]
GO
USE [master]
GO
ALTER DATABASE [geo_climate_dw] SET  READ_WRITE 
GO
