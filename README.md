# NYC Yellow Taxi Analytics Pipeline

Production-grade PySpark data pipeline for analyzing NYC Yellow Taxi trips.
Demonstrates end-to-end data engineering workflow from raw data to business insights.

![Python](https://img.shields.io/badge/python-3.10+-blue.svg)
![PySpark](https://img.shields.io/badge/PySpark-3.5.1-orange.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

## Overview

This project processes ~3M+ taxi trips from the NYC Taxi & Limousine Commission
and generates business insights using PySpark. The pipeline includes data ingestion,
quality checks, cleaning, feature engineering, and 9 different analytical queries.

## Architecture

\`\`\`
Raw Parquet → Quality Check → Cleaning → Feature Engineering → Analytics
    ↓              ↓             ↓              ↓                 ↓
 data/raw/    reports/JSON  data/processed  data/processed   data/output/
                              yellow_taxi_    yellow_taxi_     *_csv/
                              clean/          features/
\`\`\`

## Features

- **Data Quality Framework** — 9 categories of checks, JSON reports
- **Configurable Cleaning** — all rules externalized in \`config.yaml\`
- **19 Derived Features** — time buckets, speed, tip %, airport flags, categorical labels
- **9 Analytics Queries** — including window functions
- **Full Pipeline Orchestration** — single runner with CLI options
- **Unit Tests** — 20+ tests with pytest
- **Explicit Schema** — no schema inference overhead

## Tech Stack

- **PySpark 3.5.1** — distributed data processing
- **Python 3.10+**
- **Parquet** — columnar storage
- **YAML** — configuration
- **pytest** — testing framework
- **Jupyter** — interactive exploration

## Project Structure

```
nyc-taxi-analytics/
├── config/
│   └── config.yaml              # Cleaning rules & Spark settings
├── data/
│   ├── raw/                     # Original Parquet files
│   ├── processed/               # Cleaned & enriched data
│   ├── output/                  # Analytics results (CSV)
│   └── reports/                 # Quality reports (JSON)
├── notebooks/
│   ├── 01_data_exploration.ipynb
│   ├── 02_test_cleaning.ipynb
│   └── 03_features_exploration.ipynb
├── src/
│   ├── spark_session.py         # Centralized Spark factory
│   ├── schema.py                # Explicit schemas
│   ├── loader.py                # Data loading
│   ├── quality.py               # Quality check framework
│   ├── cleaner.py               # Cleaning pipeline
│   ├── transformations.py       # Feature engineering
│   ├── analytics.py             # Business analyses
│   ├── download_data.py         # NYC TLC data fetching
│   ├── run_pipeline.py          # Full orchestration
│   └── reports/
│       └── quality_report.py
├── tests/
│   ├── conftest.py              # Pytest fixtures
│   └── test_transformations.py
└── requirements.txt
```

## Setup

### Prerequisites

- Python 3.10+
- Java 17 (required for Spark)
- 8GB+ RAM recommended

### Installation

\`\`\`bash
# Clone the repository
git clone https://github.com/YOUR-USERNAME/nyc-taxi-analytics.git
cd nyc-taxi-analytics

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt
\`\`\`

## Usage

### Full pipeline

\`\`\`bash
python -m src.run_pipeline
\`\`\`

### With options

\`\`\`bash
# Skip download if data already exists
python -m src.run_pipeline --skip-download

# Skip analytics step (cleaning + features only)
python -m src.run_pipeline --skip-download --skip-analytics
\`\`\`

### Individual steps

\`\`\`bash
python -m src.download_data           # 1. Download raw data
python -m src.run_quality_check       # 2. Quality check
python -m src.run_cleaning            # 3. Clean data
python -m src.run_feature_engineering # 4. Feature engineering
python -m src.run_analytics           # 5. Run analytics
\`\`\`

### Tests

\`\`\`bash
pytest tests/ -v
\`\`\`

## Key Findings

Analysis of January 2024 (~2.7M trips):

- **Peak activity:** 17:00-20:00 (afternoon rush)
- **Lowest activity:** 4-5 AM (~20K trips vs 120K+ at peak)
- **Airport trips:** 11% of all rides
- **Top routes:** Hyper-local Midtown Manhattan (Zones 236↔237)
- **Payment behavior:** Credit card users tip more (~22%) vs Cash (~0-1% recorded)
- **Traffic patterns:** Median speed 18 mph @ 6 AM (free flow) vs 13 mph @ 9 AM (rush)

## Data Quality Approach

The cleaning pipeline removes:
- Null values in critical columns
- Dates outside the target period
- Negative or extreme values
- Impossible durations and speeds
- Invalid categorical values

Result: ~88% of raw data retained after cleaning — realistic for real-world data.

## Optimizations

- **Explicit schema** — avoids schema inference overhead
- **Adaptive Query Execution** — enabled
- **Kryo Serializer** — 10x faster than Java default
- **Partitioning** — data partitioned by \`pickup_day\`
- **Cache** — used for multi-pass operations
- **Coalesce(1)** — for small aggregated outputs

## Testing

- 20+ unit tests
- Fixtures-based test data
- Class-organized test suites
- Coverage: critical feature engineering functions

## Data Source

NYC Taxi & Limousine Commission (TLC) Trip Record Data:
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## License

MIT

## Author

Mario
