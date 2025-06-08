# Weather & Air Quality Data Analytics Platform

## Overview

This project implements an end-to-end analytics platform for monitoring and analyzing historical weather and air quality data across global locations. It provides a structured data warehousing solution, automated data processing, and a published Power BI dashboard with daily refresh.

The platform is designed for potential use in environmental research, public health advisory systems, or general climate and pollution analysis.

## Technologies Used

- **Microsoft SQL Server Management Studio (SSMS)**  
  Used for all database development tasks: creating schemas, defining tables, writing SQL scripts, and executing data transformations.

- **Python (`pyodbc`)**  
  Orchestrates the execution of SQL scripts (initial and incremental loads). Integrated with **Windows Task Scheduler** for automation and periodic execution.

- **Power BI**  
  Used to build and publish the final visualizations on top of the star schema. Configured to auto-refresh data daily from the star schema.

## Data Sources

1. **Structured Data in SQL Server**  
   AI-generated data simulating real-world weather and air quality metrics.

2. **Public Dataset**  
   [Kaggle: Global Weather Repository](https://www.kaggle.com/datasets/nelgiriyewithana/global-weather-repository)

## Data Warehouse Architecture

The solution is organized into three main schemas within SQL Server:

### `dwh_stg` – Staging Layer
- Temporary landing zone for initial data load.
- Data is inserted as-is from source.

### `dwh_raw` – Raw Layer
- Data is cleansed, standardized, and enriched.
- Includes metadata fields: `input_id`, `update_id`, `start_date`, `end_date`.
- Supports tracking changes over time using validity periods.

### `dwh_star` – Star Schema
- Structured for analytics and reporting.
- Includes:

  **Fact Tables**
  - `FactWeatherMetrics`
  - `FactAirQuality`

  **Dimension Tables**
  - `DimDate`
  - `DimTime`
  - `DimLocation`
  - `DimAQCategory`
  - `DimWeatherCondition`

## ETL & Orchestration

This project uses a Python script to orchestrate SQL Server ETL logic. Transformations are done via SQL scripts executed by Python.

### Initial Load
- Three SQL scripts:
  - `load_staging.sql`
  - `load_raw.sql`
  - `load_star.sql`
- All scripts are executed sequentially by `orchestration.py`.

### Incremental Load

The incremental load logic applies only to the `dwh_raw` schema, where historical changes are tracked.

- The Python orchestrator checks if the raw layer (`dwh_raw`) is empty:
  - If **empty**, it runs a full load for raw schema.
  - If **not empty**, it runs `incremental_load.sql` to process changes into the raw layer.
  
- The raw schema uses metadata columns (`start_date`, `end_date`, `input_id`, `update_id`) to track changes over time.
- The star schema is not incrementally updated; it is fully refreshed after each load for simplicity.


### Automation
- **Windows Task Scheduler** is used to run the Python orchestrator script periodically.

## Power BI Dashboard

The published Power BI report is titled **"Historical Weather & Air Quality"**, connected directly to the `dwh_star` schema and set to refresh automatically once per day.

### Visualizations Include:

- **Average Temperature**  
  Average temperature over a period of time.

- **Average Humidity**  
  Average humidity over a period of time.

- **Air Quality Index (PM2.5)**  
  Gauge chart showing pollutant level within 0–500 µg/m³.

- **Temperature Range**  
  Line chart comparing minimum and maximum temperature by month.

- **Wind Speed & Precipitation**  
  Combined column and line chart showing monthly averages.


> ![Image](https://github.com/user-attachments/assets/0eb9f594-99e7-433d-ac93-c627e435b1ee)
