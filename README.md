# 📈 Market Sentiment ETL Pipeline (Dockerized)

![Python](https://img.shields.io/badge/Python-3.11-blue.svg?logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.5-E25A1C.svg?logo=apachespark&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Enabled-2496ED.svg?logo=docker&logoColor=white)
![Architecture](https://img.shields.io/badge/Architecture-Medallion-success.svg)

An End-to-End Data Engineering pipeline designed to extract, process, and analyze the correlation between cryptocurrency price action and the emotional polarity (NLP) of real-time financial news. 

The project is 100% containerized to guarantee reproducibility and an agnostic deployment environment.

## Data Architecture (Medallion)

The system implements the industry-standard Medallion Architecture, ensuring data traceability, quality, and strict idempotency:

* **Bronze Layer (Raw Data):** Immutable ingestion.
  * Time-series price extraction via REST API (CoinGecko).
  * Dynamic Web Scraping of financial headlines (Yahoo Finance), bypassing bot-protection through network header spoofing.
* **Silver Layer (Distributed Processing & AI):** The transformation engine.
  * Cleansing, flattening of complex JSONs, and strict type casting using **Apache Spark (PySpark)**.
  * Implementation of Natural Language Processing (TextBlob) distributed via Spark UDFs (User Defined Functions) to calculate sentiment polarity.
  * Strict deduplication logic and format transition to columnar **Parquet**.
* **Gold Layer (Business Intelligence):** Analytical modeling.
  * Temporal aggregation and dimensional joins (Market vs. News) using in-memory **SQL**.
  * Safe null-handling (`COALESCE`) to protect the integrity of downstream visualization tools.

## Tech Stack

* **Big Data Processing:** PySpark, SQL.
* **Software Engineering:** Python 3.10, Defensive Programming, Log Rotation (`RotatingFileHandler`).
* **Artificial Intelligence (NLP):** TextBlob (Polarity scoring from -1.0 to 1.0).
* **Infrastructure & DevOps:** Docker, Docker Compose.
* **Visualization:** Pandas, Matplotlib (Dynamic dashboard generation iterating over discovered dimensions).

## Quick Start (Reproducibility)

The project is configured with `docker-compose` to spin up the entire environment (including the required Java Virtual Machine for Spark) without local dependency conflicts.

**1. Clone the repository:**
```bash
git clone https://github.com/rodrigoroson/market-sentiment-pipeline.git
cd market-sentiment-pipeline
```

**2. Build the infrastructure image:**
```bash
docker compose build
```

**3. Run the ETL Orchestrator (Full Pipeline):**
*Volumes are mapped. Processed data will be automatically saved to your local `./data` directory.*
```bash
docker compose up
```

**4. Generate Business Intelligence (Dashboards):**
Once the pipeline finishes, run this command to render the analytical charts:
```bash
docker compose run pipeline python -m src.visualization.plot_metrics
```

## Visual Results

The system automatically generates analytical dashboards for every asset discovered in the database, crossing the historical price with the market sentiment index.

> **Note on Data Visualization:** The visualization module is fully functional and ready to use. However, to display a meaningful and interesting trend chart here, the ETL pipeline needs to run continuously over several days to accumulate sufficient historical data. Once enough data points are collected to show a clear correlation, a sample dashboard will be uploaded to this section.