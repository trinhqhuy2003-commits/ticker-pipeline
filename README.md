# Real-Time Crypto Ticker Pipeline

A robust data engineering pipeline that ingests real-time crypto prices, streams them via Kafka, processes them with Spark Structured Streaming, and stores aggregated metrics in PostgreSQL for visualization in Grafana.

## Architecture

1.  **Ingestion**: Python script fetching data from Yahoo Finance and producing to Kafka.
2.  **Streaming**: Apache Kafka & Zookeeper.
3.  **Processing**: Java Spark Structured Streaming app (aggregates 5-min windows).
4.  **Storage**: PostgreSQL.
5.  **Visualization**: Grafana.

## Prerequisites

-   Docker & Docker Compose

## Quick Start

1.  Start the pipeline:
    ```bash
    docker-compose up --build
    ```

2.  Access Grafana:
    -   URL: http://localhost:3000 (or http://127.0.0.1:3000)
    -   Login: admin / admin

3.  Verify Data in Postgres:
    ```bash
    docker exec -it postgres psql -U user -d ticker_db -c "SELECT * FROM price_aggregates ORDER BY window_start DESC LIMIT 10;"
    ```
