# COVID-19 Big Data Analytics (BDA) Dashboard & MongoDB Atlas Platform

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.13-blue?logo=python&logoColor=white" alt="Python">
  <img src="https://img.shields.io/badge/Database-MongoDB%20Atlas-green?logo=mongodb&logoColor=white" alt="MongoDB Atlas">
  <img src="https://img.shields.io/badge/Backend-FastAPI-teal?logo=fastapi&logoColor=white" alt="FastAPI">
  <img src="https://img.shields.io/badge/Frontend-Tailwind%20%2B%20Chart.js-purple" alt="Frontend">
  <img src="https://img.shields.io/badge/Status-Production%20Ready-brightgreen" alt="Status">
</p>

<p align="center">
  <strong>An end-to-end Big Data Analytics (BDA) web application powered by MongoDB Atlas</strong><br>
  Transforms raw and processed COVID-19 epidemiological data (JHU CSSE) and World Bank socioeconomic indicators into an interactive, high-performance analytical dashboard using distributed database aggregation pipelines.
</p>

---

## 📑 Table of Contents

1. [Project Overview & Objective](#project-overview--objective)
2. [Why MongoDB Atlas for Big Data Analytics?](#why-mongodb-atlas-for-big-data-analytics)
3. [System Architecture & Data Flow](#system-architecture--data-flow)
4. [Database Collections & Document Schema](#database-collections--document-schema)
5. [Data Ingestion & Duplicate Prevention](#data-ingestion--duplicate-prevention)
6. [MongoDB Aggregation Pipelines](#mongodb-aggregation-pipelines)
7. [Web Dashboard Features](#web-dashboard-features)
8. [Technology Stack](#technology-stack)
9. [Quick Start & Run Instructions (VS Code & PowerShell)](#quick-start--run-instructions-vs-code--powershell)
10. [REST API Documentation](#rest-api-documentation)
11. [Testing & Verification](#testing--verification)
12. [Troubleshooting Guide](#troubleshooting-guide)
13. [Original Exploratory Notebooks & Data Sources](#original-exploratory-notebooks--data-sources)

---

## Project Overview & Objective

This project fulfills the requirements of a **Final-Year Engineering Big Data Analytics (BDA)** course. It upgrades a traditional offline Python data analysis project into a cloud-native, database-backed analytical web platform.

The system ingests over 34,000 historical epidemiological records spanning January 2020 through March 2023 across 198 countries, stores them in MongoDB Atlas, and exposes them through real-time aggregation queries and a responsive web dashboard.

---

## Why MongoDB Atlas for Big Data Analytics?

Traditional relational databases (RDBMS) struggle with evolving time-series records, semi-structured socioeconomic indicators, and multi-granularity aggregations. MongoDB Atlas was selected for the following BDA advantages:

1. **Document-Oriented Flexibility**: Stores diverse data types (daily case time series, spatial coordinates, World Bank indicators) in clean JSON-like BSON documents without rigid schema migrations.
2. **Powerful Distributed Aggregation Framework**: Computes worldwide totals, weighted mortality rates, and continental rollups natively in the database engine using `$group`, `$project`, `$round`, and `$cond`, offloading CPU-intensive processing from the web application server.
3. **Compound Indexing & Fast Retrieval**: Employs unique compound indexes (`{country: 1, date: 1}`) for sub-millisecond chronological queries and guaranteed idempotency.
4. **Cloud Scalability**: Hosted on MongoDB Atlas M0 free tier with zero local server maintenance.

---

## System Architecture & Data Flow

```
┌─────────────────────────────────┐       ┌─────────────────────────────────┐
│   Processed CSV Datasets        │       │    World Bank Socioeconomic     │
│   (JHU CSSE 2020-2023)          │       │    Indicators (188 Countries)   │
└────────────────┬────────────────┘       └────────────────┬────────────────┘
                 │                                         │
                 ▼                                         ▼
   ┌─────────────────────────────────────────────────────────────┐
   │        backend/import_data.py (Ingestion Pipeline)          │
   │  • Validates required columns & cleans null/NaN values      │
   │  • Filters cruise ships (MS Zaandam, Diamond Princess)      │
   │  • Idempotent Bulk Upsert with Compound Unique Keys         │
   └─────────────────────────────┬───────────────────────────────┘
                                 │
                                 ▼
   ┌─────────────────────────────────────────────────────────────┐
   │            MongoDB Atlas Cloud Database (covid19_db)        │
   │  ├── country_statistics  (198 documents, unique country)    │
   │  ├── daily_cases         (33,813 docs, {country, date} idx) │
   │  └── country_metadata    (188 docs, World Bank indicators)  │
   └─────────────────────────────┬───────────────────────────────┘
                                 │
                                 ▼
   ┌─────────────────────────────────────────────────────────────┐
   │        FastAPI REST API & PyMongo Aggregation Service       │
   │  ├── /api/summary, /api/continents, /api/countries/...      │
   │  └── Automatic SRV Public DNS Fallback (8.8.8.8, 1.1.1.1)   │
   └─────────────────────────────┬───────────────────────────────┘
                                 │
                                 ▼
   ┌─────────────────────────────────────────────────────────────┐
   │        Modern Web Dashboard (Tailwind CSS + Chart.js)       │
   │  ├── Live Atlas Telemetry & KPI Cards                       │
   │  ├── Interactive Time-Series Pandemic Trajectory            │
   │  ├── Side-by-Side Top 10 Case & Death Bar Charts            │
   │  ├── Multi-Country Comparison & World Bank Matrix           │
   │  └── Live Aggregation Pipeline Code Showcase                │
   └─────────────────────────────────────────────────────────────┘
```

---

## Database Collections & Document Schema

The database `covid19_db` contains three collections:

### 1. `country_statistics` (198 documents)
Stores current summary metrics for all 198 countries:
```json
{
  "_id": "India",
  "country": "India",
  "confirmed": 44690738,
  "deaths": 530779,
  "recovered": 0,
  "active": 44159959,
  "mortality_rate": 1.19,
  "continent": "Asia",
  "latitude": 20.5937,
  "longitude": 78.9629,
  "last_updated": "2023-03-09"
}
```
**Indexes**:
- `_id`: Unique Country Name (Ascending)
- `confirmed`: Descending (for Top-N queries)
- `deaths`: Descending
- `mortality_rate`: Descending
- `continent`: Ascending

### 2. `daily_cases` (33,813 documents)
Stores chronological time-series checkpoints from January 2020 to March 2023:
```json
{
  "_id": "India_2021-05-09",
  "country": "India",
  "date": "2021-05-09",
  "confirmed": 22662575,
  "deaths": 246116,
  "recovered": 0,
  "daily_new_cases": 366161,
  "daily_new_deaths": 3754
}
```
**Indexes**:
- `_id`: `{country}_{date}` (Unique)
- Compound: `{ country: 1, date: 1 }` (Unique)
- Query: `{ date: 1 }`, `{ country: 1 }`

### 3. `country_metadata` (188 documents)
Stores World Bank developmental indicators:
```json
{
  "_id": "India",
  "country": "India",
  "continent": "Asia",
  "gdp_per_capita": 6997.86,
  "life_expectancy": 69.66,
  "gdp_healthcare_pct": 3.01,
  "rural_population_pct": 65.53,
  "cases_per_million": 32384.52,
  "deaths_per_million": 384.62
}
```

---

## Data Ingestion & Duplicate Prevention

Data ingestion is implemented in `backend/import_data.py`.

### Key Features:
- **Zero-Duplicate Guarantee**: Uses MongoDB `UpdateOne({"_id": ...}, {"$set": ...}, upsert=True)`. Re-running the import script at any time produces 0 duplicates.
- **Data Quality**: Filters cruise ships (`MS Zaandam`, `Diamond Princess`) from country aggregations, converts missing values safely, and checks data types.
- **Network Optimization**: Batches bulk operations in groups of 2,000 for fast upload over internet connections.
- **Dry-Run Validation**: Supports `--dry-run` to preview data mapping without making database changes.

---

## MongoDB Aggregation Pipelines

All statistical computations are executed inside MongoDB Atlas:

### Pipeline 1: Worldwide KPI Summary
```javascript
db.country_statistics.aggregate([
  {
    $group: {
      _id: null,
      total_confirmed: { $sum: "$confirmed" },
      total_deaths: { $sum: "$deaths" },
      total_active: { $sum: "$active" },
      total_countries: { $sum: 1 }
    }
  },
  {
    $project: {
      _id: 0,
      total_confirmed: 1,
      total_deaths: 1,
      total_active: 1,
      total_countries: 1,
      mortality_rate: {
        $round: [
          { $multiply: [{ $divide: ["$total_deaths", "$total_confirmed"] }, 100] },
          2
        ]
      }
    }
  }
])
```

### Pipeline 2: Continental Rollup
```javascript
db.country_statistics.aggregate([
  { $match: { continent: { $nin: ["Unknown", null] } } },
  {
    $group: {
      _id: "$continent",
      total_confirmed: { $sum: "$confirmed" },
      total_deaths: { $sum: "$deaths" },
      country_count: { $sum: 1 }
    }
  },
  {
    $project: {
      _id: 0,
      continent: "$_id",
      total_confirmed: 1,
      total_deaths: 1,
      country_count: 1,
      mortality_rate: {
        $round: [
          { $multiply: [{ $divide: ["$total_deaths", "$total_confirmed"] }, 100] },
          2
        ]
      }
    }
  },
  { $sort: { total_confirmed: -1 } }
])
```

---

## Web Dashboard Features

The dashboard runs locally and communicates with MongoDB Atlas:

1. **Live Atlas Telemetry**: Live ping status, cluster name, database name, and live document counts.
2. **Executive KPI Cards**: Worldwide Confirmed Cases, Total Deaths, Case Fatality Rate (Mortality %), and Country Count.
3. **Interactive Time-Series Chart**: Switch between Cumulative Cases, Cumulative Deaths, and Daily New Cases.
4. **Top 10 Horizontal Bar Charts**: Comparing highest-load countries by cases and deaths.
5. **Continental Doughnut & Table**: Case and death distribution across Europe, Asia, Americas, Africa, and Oceania.
6. **Country Explorer & Search**: Instant autocomplete search, socioeconomic World Bank indicators, and country time series.
7. **Multi-Country Comparison Matrix**: Select and compare metrics for multiple countries simultaneously.
8. **Live Pipeline Viewer**: Displays the exact MongoDB aggregation syntax used to power the dashboard.

---

## Technology Stack

- **Database**: MongoDB Atlas (Free M0 Cluster `COVID19Cluster`)
- **Database Driver**: PyMongo 4.19
- **Backend API**: FastAPI 0.143 + Uvicorn
- **Environment Management**: python-dotenv
- **Frontend UI**: HTML5, Tailwind CSS, Chart.js, Lucide Icons
- **Data Wrangling**: Pandas, NumPy

---

## Quick Start & Run Instructions (VS Code & PowerShell)

### Step 1: Open Terminal in Project Root
```powershell
cd c:\Users\Shabana\Desktop\covid19_analysis
```

### Step 2: Ensure Dependencies Are Installed
```powershell
pip install -r requirements.txt
```

### Step 3: Verify MongoDB Atlas Connection
Run the safe connection diagnostic script:
```powershell
python test_mongodb.py
```
*Expected output: `[SUCCESS] MongoDB Atlas ping successful!`*

### Step 4: Import Data into MongoDB Atlas
*(Only needed once or to refresh data)*
```powershell
# Optional dry-run check:
python backend/import_data.py --dry-run

# Run the real ingestion:
python backend/import_data.py
```
*Expected output: `SUMMARY: 34,199 total records processed (34,199 inserted, 0 updated).`*

### Step 5: Start the Analytics Web Application
```powershell
uvicorn backend.main:app --reload --port 8000
```

### Step 6: Open the Dashboard
Open your web browser and navigate to:
```
http://127.0.0.1:8000
```
Interactive API documentation is also available at:
```
http://127.0.0.1:8000/docs
```

---

## REST API Documentation

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/health` | Safe Atlas connectivity and service health check |
| `GET` | `/api/summary` | Global summary computed via MongoDB aggregation pipeline |
| `GET` | `/api/countries/top-cases?limit=10` | Top N countries by confirmed cases |
| `GET` | `/api/countries/top-deaths?limit=10` | Top N countries by total deaths |
| `GET` | `/api/countries/top-mortality` | Top N countries by mortality rate |
| `GET` | `/api/continents` | Continental aggregation breakdown |
| `GET` | `/api/countries/search?q=...` | Case-insensitive regex country search |
| `GET` | `/api/countries/{country_name}` | Country statistics + World Bank socioeconomic metadata |
| `GET` | `/api/countries/compare?countries=...` | Compare multiple countries via MongoDB `$in` operator |
| `GET` | `/api/trends/{country_name}` | Chronological daily case time series |
| `GET` | `/api/database-status` | Database storage size, index size, and document counts |

---

## Testing & Verification

Run the automated integration and regression test suites:

```powershell
# Run the complete MongoDB Atlas & FastAPI integration test suite:
python -m unittest tests/test_mongodb_app.py

# Run original data-cleaning unit tests:
python -m unittest tests/test.py
```

Both test suites pass with **100% success rate (20/20 tests passed)**.

---

## Troubleshooting Guide

### 1. Connection Timeout or SRV Resolution Error
- **Symptom**: `Server Do53:192.168.0.1@53 answered The DNS operation timed out`
- **Solution**: The application already includes an automatic fallback resolver (`8.8.8.8`, `1.1.1.1`) inside `backend/database.py` and `test_mongodb.py`. If testing outside Python, check your internet connection or use a public DNS.

### 2. IP Not Whitelisted in MongoDB Atlas
- **Symptom**: `ServerSelectionTimeoutError: No replica set members match selector`
- **Solution**:
  1. Log into [MongoDB Atlas](https://cloud.mongodb.com/).
  2. Go to **Security** -> **Network Access**.
  3. Ensure your current IP address is added, or add `0.0.0.0/0` (Allow Access from Anywhere) for testing.

### 3. Missing or Malformed `.env` File
- **Symptom**: `[ERROR] MONGODB_URI is not set in your .env file`
- **Solution**: Ensure your `.env` contains:
  ```env
  MONGODB_URI=mongodb+srv://<username>:<password>@covid19cluster.djyps0w.mongodb.net/?retryWrites=true&w=majority&appName=COVID19Cluster
  MONGODB_DB_NAME=covid19_db
  ```
  *(Never wrap passwords in `<` and `>` angle brackets).*

---

## Original Exploratory Notebooks & Data Sources

The project preserves all original exploratory data analysis and Jupyter notebooks:

- `notebooks/Data-wrangling.ipynb`: Data pipeline documentation and cleaning.
- `notebooks/Exploratory-analysis-globally.ipynb`: Global and continental trends.
- `notebooks/Exploratory_analysis_fancy_plot.ipynb`: Publication-grade visualization.
- `notebooks/Exploratory-analysis-mortality.ipynb`: Country-level mortality rate investigations.
- `notebooks/Exploratory_analysis_socioeconomic.ipynb`: World Bank correlation studies.
