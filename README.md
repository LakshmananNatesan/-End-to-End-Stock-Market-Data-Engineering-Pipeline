# Navigating Financial Markets: A Cloud-Based Stock Price Analytics Pipeline
End-to-end data engineering pipeline that ingests stock market data from Yahoo Finance API, processes it using AWS Glue, stores it in S3, ingests into Snowflake using Snowpipe, and visualizes insights in Tableau
# Stock Market Data Engineering Pipeline

## Overview
End-to-end data engineering pipeline that ingests stock market data from Yahoo Finance API,
processes it using AWS Glue, stores it in S3, ingests into Snowflake using Snowpipe,
and visualizes insights in Tableau.

## Tech Stack
- Apache Airflow (Orchestration)
- AWS S3 (Data Lake)
- AWS Glue (ETL)
- Snowflake (Data Warehouse)
- Tableau (Analytics & Visualization)

## Architecture
![Architecture](architecture/architecture_diagram.png)

## Pipeline Flow
1. Airflow triggers Yahoo Finance API
2. Raw JSON stored in S3
3. Glue ETL transforms to cleaned Parquet
4. Snowpipe auto-ingests into Snowflake
5. Tableau dashboard for analysis

## Key Analytics
- Price vs Volume trends
- Moving Average crossover analysis
- Volatility detection
- Trading activity by weekday

## Dashboard
[Tableau Public Link](link_here)
