# Financial Analytics Pipeline

## Overview

This project demonstrates an **end-to-end data pipeline** built using modern data engineering tools. It ingests raw financial data (from CSV files found here: https://www.kaggle.com/datasets/computingvictor/transactions-fraud-datasets/data?select=cards_data.csv), transforms it into analytics-ready datasets, performs advanced analysis in Python, and prepares outputs for visualization in Power BI.

The project follows industry best practices including:

* layered data modelling (RAW → STAGING → MARTS)
* modular Python code
* environment-based configuration
* reusable utilities

---

## Tech Stack

* **Snowflake** – Cloud data warehouse
* **dbt (Data Build Tool)** – Data transformation & modelling
* **Python** – Data ingestion and analysis
* **pandas** – Data manipulation

---

## Architecture

```
CSV (Kaggle dataset)
    ↓
Python (ingestion)
    ↓
Snowflake RAW layer
    ↓
dbt staging models
    ↓
dbt marts layer
    ↓
Python analytics
    ↓
Snowflake analytics tables
```

---

## Project Structure

```
finance_project/
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   └── dbt_project.yml
├── python/
│   ├── analysis/
│   └── utils/
├── data/
├── requirements.txt
├── env.example
└── README.md
```

---

## Data Layers

### RAW (Snowflake)

Raw CSV data is loaded into Snowflake with minimal transformation.

Tables:

* `RAW.TRANSACTIONS`
* `RAW.USERS`
* `RAW.CARDS`

All columns are stored as `VARCHAR` as I wanted to clean later on.

---

### STAGING (dbt)

Data is cleaned and standardised.

Examples:

* data type casting
* date parsing
* currency cleaning
* column renaming

Models:

* `stg_transactions`
* `stg_users`
* `stg_cards`

---

### MARTS (dbt)

Create datasets that are ready for analysis.

Models:

* `fct_transactions`
* `dim_users`
* `dim_cards`
* `fct_transactions_enriched`
* `fct_daily_spend`

---

## Python Analysis

I normally use SQL for data manipulation so wanted to improve my knowledge and use Python for my transformations.

The Outputs are then written back to Snowflake as these are easier to feed into visualisation tools such as PowerBI:

* `CUSTOMER_SEGMENTS`
* `MONTHLY_SPEND_TRENDS`
* `CARD_SPEND_SUMMARY`
* `CUSTOMER_VALUE_SUMMARY`
* `TRANSACTION_ANOMALIES`
* `AGE_BAND_SUMMARY`
* `COHORT_SPEND_SUMMARY`

---

## Setup Instructions

### 1. Clone the repository

```
git clone <your-repo-url>
cd finance_project
```

---

### 2. Create and activate virtual environment

```
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

---

### 3. Install dependencies

```
pip install -r requirements.txt
```

---

### 4. Configure environment variables

Create a `.env` file in the project root:

```
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=FINANCE_DB
SNOWFLAKE_SCHEMA=ANALYTICS
```

This file is ignored by Git.

---

### 5. Run dbt models

```
cd dbt
dbt run
```

---

### 6. Run Python analysis scripts

From project root:

```
python -m python.analysis.customer_segmentation
python -m python.analysis.monthly_spend_trends
```

---

## Power BI

Power BI connects to Snowflake `ANALYTICS` schema.

Recommended tables:

* `CUSTOMER_SEGMENTS`
* `MONTHLY_SPEND_TRENDS`
* `CARD_SPEND_SUMMARY`
* `CUSTOMER_VALUE_SUMMARY`

---

## Security

* Credentials are stored in `.env`
* `.env` is excluded via `.gitignore`
* No sensitive data is committed

---

## Key Learnings

* Building scalable data pipelines
* dbt data modelling (staging vs marts)
* Snowflake data architecture
* Python-based analytics

---