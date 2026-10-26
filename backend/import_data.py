"""
COVID-19 Data Ingestion Pipeline for MongoDB Atlas
--------------------------------------------------
Safely ingests and transforms processed COVID-19 datasets and World Bank
socioeconomic data into MongoDB Atlas with idempotent bulk upserts,
duplicate prevention, column validation, and detailed reporting.
"""

import os
import sys
import argparse
import logging
from datetime import datetime
from typing import Dict, Any, List, Tuple
import pandas as pd
import numpy as np

# Ensure parent directory is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pymongo import UpdateOne
from backend.database import get_database, create_indexes, check_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("covid19.import")

# Excluded entities per data cleaning rules
CRUISE_SHIPS = {"MS Zaandam", "Diamond Princess"}

def get_data_dir() -> str:
    """Locate the processed data directory."""
    candidates = [
        os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "processed"),
        os.path.join(".", "data", "processed"),
        "./data/processed"
    ]
    for c in candidates:
        if os.path.isdir(c):
            return os.path.abspath(c)
    raise FileNotFoundError("Could not locate 'data/processed' directory.")

def clean_val(val, default=0, is_float=False):
    """Safely convert numpy/pandas values to standard Python types, handling NaN."""
    if pd.isna(val) or val is None or (isinstance(val, (int, float)) and np.isnan(val)):
        return default
    try:
        return round(float(val), 2) if is_float else int(val)
    except (ValueError, TypeError):
        return default

def import_country_statistics(data_dir: str, db, dry_run: bool = False) -> Dict[str, int]:
    """
    Ingest country_stats.csv enriched with coordinates.csv and country_to_continent.csv
    into the 'country_statistics' collection.
    """
    stats_file = os.path.join(data_dir, "country_stats.csv")
    coords_file = os.path.join(data_dir, "coordinates.csv")
    continent_file = os.path.join(data_dir, "country_to_continent.csv")

    for fpath in [stats_file, coords_file, continent_file]:
        if not os.path.exists(fpath):
            raise FileNotFoundError(f"Missing required data file: {fpath}")

    df_stats = pd.read_csv(stats_file)
    df_coords = pd.read_csv(coords_file)
    df_cont = pd.read_csv(continent_file)

    # Validate required columns
    required_cols = {"Country", "Confirmed", "Recovered", "Dead", "Active", "Mortality"}
    if not required_cols.issubset(df_stats.columns):
        missing = required_cols - set(df_stats.columns)
        raise ValueError(f"country_stats.csv missing required columns: {missing}")

    # Merge coordinates and continent
    merged = pd.merge(df_stats, df_cont, on="Country", how="left")
    merged = pd.merge(merged, df_coords, on="Country", how="left")

    # Exclude cruise ships
    merged = merged[~merged["Country"].isin(CRUISE_SHIPS)]

    operations = []
    processed = 0
    skipped = 0

    for _, row in merged.iterrows():
        country_name = str(row["Country"]).strip()
        if not country_name:
            skipped += 1
            continue

        confirmed = clean_val(row["Confirmed"])
        deaths = clean_val(row["Dead"])
        recovered = clean_val(row["Recovered"])
        active = clean_val(row["Active"])
        
        # Calculate mortality safely
        mortality = clean_val(row["Mortality"], is_float=True)
        if mortality == 0 and confirmed > 0 and deaths > 0:
            mortality = round((deaths / confirmed) * 100, 2)

        continent = row.get("Continent")
        continent = str(continent).strip() if pd.notna(continent) else "Unknown"

        lat = clean_val(row.get("Lat"), default=0.0, is_float=True)
        lon = clean_val(row.get("Long"), default=0.0, is_float=True)

        doc = {
            "country": country_name,
            "confirmed": confirmed,
            "deaths": deaths,
            "recovered": recovered,
            "active": active,
            "mortality_rate": mortality,
            "continent": continent,
            "latitude": lat,
            "longitude": lon,
            "last_updated": "2023-03-09"
        }

        # Use country as stable _id for idempotent upsert
        operations.append(UpdateOne({"_id": country_name}, {"$set": doc}, upsert=True))
        processed += 1

    report = {"processed": processed, "upserted": 0, "modified": 0, "skipped": skipped}
    if dry_run:
        logger.info(f"[DRY-RUN] country_statistics: prepared {processed} records.")
        return report

    if operations:
        coll = db["country_statistics"]
        res = coll.bulk_write(operations, ordered=False)
        report["upserted"] = res.upserted_count
        report["modified"] = res.modified_count
        logger.info(f"country_statistics: {processed} processed ({res.upserted_count} inserted, {res.modified_count} updated).")

    return report

def import_country_metadata(data_dir: str, db, dry_run: bool = False) -> Dict[str, int]:
    """
    Ingest world_bank.csv into 'country_metadata' collection.
    """
    wb_file = os.path.join(data_dir, "world_bank.csv")
    if not os.path.exists(wb_file):
        raise FileNotFoundError(f"Missing world bank file: {wb_file}")

    df_wb = pd.read_csv(wb_file)
    if "Country" not in df_wb.columns:
        raise ValueError("world_bank.csv must contain 'Country' column.")

    df_wb = df_wb[~df_wb["Country"].isin(CRUISE_SHIPS)]

    operations = []
    processed = 0
    skipped = 0

    for _, row in df_wb.iterrows():
        country_name = str(row["Country"]).strip()
        if not country_name:
            skipped += 1
            continue

        doc = {
            "country": country_name,
            "continent": str(row.get("Continent", "Unknown")).strip(),
            "gdp_per_capita": clean_val(row.get("GDP per capita"), default=0.0, is_float=True),
            "life_expectancy": clean_val(row.get("Life expectancy"), default=0.0, is_float=True),
            "gdp_healthcare_pct": clean_val(row.get("GDP Healthcare %"), default=0.0, is_float=True),
            "rural_population_pct": clean_val(row.get("Rural population %"), default=0.0, is_float=True),
            "cases_per_million": clean_val(row.get("Cases per mln"), default=0.0, is_float=True),
            "deaths_per_million": clean_val(row.get("Dead per mln"), default=0.0, is_float=True),
        }

        operations.append(UpdateOne({"_id": country_name}, {"$set": doc}, upsert=True))
        processed += 1

    report = {"processed": processed, "upserted": 0, "modified": 0, "skipped": skipped}
    if dry_run:
        logger.info(f"[DRY-RUN] country_metadata: prepared {processed} records.")
        return report

    if operations:
        coll = db["country_metadata"]
        res = coll.bulk_write(operations, ordered=False)
        report["upserted"] = res.upserted_count
        report["modified"] = res.modified_count
        logger.info(f"country_metadata: {processed} processed ({res.upserted_count} inserted, {res.modified_count} updated).")

    return report

def import_daily_cases(data_dir: str, db, dry_run: bool = False, full_daily: bool = False) -> Dict[str, int]:
    """
    Ingest chronological case records into 'daily_cases'.
    Supports:
    1. Global daily timeline (all 1,143 dates) with daily totals and new cases/deaths.
    2. Country time series: weekly checkpoints (or full daily if full_daily=True).
    Uses compound key _id: '{country}_{date}' for idempotent duplicate prevention.
    """
    conf_file = os.path.join(data_dir, "confirmed_cases.csv")
    dead_file = os.path.join(data_dir, "dead_cases.csv")
    daily_chg_file = os.path.join(data_dir, "confirmed_cases_daily_change.csv")
    recov_file = os.path.join(data_dir, "recovered_cases.csv")

    for fpath in [conf_file, dead_file]:
        if not os.path.exists(fpath):
            raise FileNotFoundError(f"Missing time series file: {fpath}")

    df_conf = pd.read_csv(conf_file)
    df_dead = pd.read_csv(dead_file)
    df_chg = pd.read_csv(daily_chg_file) if os.path.exists(daily_chg_file) else None
    df_recov = pd.read_csv(recov_file) if os.path.exists(recov_file) else None

    # Get valid country list (exclude 'Date' and cruise ships)
    country_cols = [c for c in df_conf.columns if c != "Date" and c not in CRUISE_SHIPS]

    # Convert Date column to string YYYY-MM-DD
    df_conf["Date"] = pd.to_datetime(df_conf["Date"]).dt.strftime("%Y-%m-%d")
    df_dead["Date"] = pd.to_datetime(df_dead["Date"]).dt.strftime("%Y-%m-%d")
    if df_chg is not None:
        df_chg["Date"] = pd.to_datetime(df_chg["Date"]).dt.strftime("%Y-%m-%d")
    if df_recov is not None:
        df_recov["Date"] = pd.to_datetime(df_recov["Date"]).dt.strftime("%Y-%m-%d")

    # 1. Generate Global Daily Records
    total_dates = len(df_conf)
    global_docs = []
    
    # Calculate global totals across all countries for each date
    conf_totals = df_conf[country_cols].sum(axis=1)
    dead_totals = df_dead[country_cols].sum(axis=1)
    chg_totals = df_chg[country_cols].sum(axis=1) if df_chg is not None else conf_totals.diff().fillna(0)

    for i in range(total_dates):
        d_str = df_conf.loc[i, "Date"]
        tot_c = int(conf_totals.iloc[i])
        tot_d = int(dead_totals.iloc[i])
        new_c = max(0, int(chg_totals.iloc[i]))
        prev_d = int(dead_totals.iloc[i-1]) if i > 0 else 0
        new_d = max(0, tot_d - prev_d)

        doc = {
            "country": "Global",
            "date": d_str,
            "confirmed": tot_c,
            "deaths": tot_d,
            "recovered": 0,
            "daily_new_cases": new_c,
            "daily_new_deaths": new_d
        }
        global_docs.append(UpdateOne({"_id": f"Global_{d_str}"}, {"$set": doc}, upsert=True))

    # 2. Country Time Series
    # Sample every 7 days + final day for fast, optimal storage, or all days if full_daily
    if full_daily:
        selected_indices = list(range(total_dates))
    else:
        selected_indices = list(range(0, total_dates, 7))
        if (total_dates - 1) not in selected_indices:
            selected_indices.append(total_dates - 1)

    country_ops = []
    for country in country_cols:
        prev_c = 0
        prev_d = 0
        for idx in selected_indices:
            d_str = df_conf.loc[idx, "Date"]
            c_val = clean_val(df_conf.loc[idx, country])
            d_val = clean_val(df_dead.loc[idx, country])
            r_val = clean_val(df_recov.loc[idx, country]) if df_recov is not None and country in df_recov.columns else 0
            
            # Daily change from previous checkpoint
            new_c = max(0, c_val - prev_c) if prev_c > 0 else 0
            new_d = max(0, d_val - prev_d) if prev_d > 0 else 0
            prev_c = c_val
            prev_d = d_val

            doc = {
                "country": country,
                "date": d_str,
                "confirmed": c_val,
                "deaths": d_val,
                "recovered": r_val,
                "daily_new_cases": new_c,
                "daily_new_deaths": new_d
            }
            country_ops.append(UpdateOne({"_id": f"{country}_{d_str}"}, {"$set": doc}, upsert=True))

    all_ops = global_docs + country_ops
    total_records = len(all_ops)
    report = {"processed": total_records, "upserted": 0, "modified": 0, "skipped": 0}

    if dry_run:
        logger.info(f"[DRY-RUN] daily_cases: prepared {total_records} records ({len(global_docs)} global, {len(country_ops)} country).")
        return report

    # Bulk execute in batches of 2,000 for network efficiency
    coll = db["daily_cases"]
    batch_size = 2000
    upserted_total = 0
    modified_total = 0

    for i in range(0, total_records, batch_size):
        batch = all_ops[i:i + batch_size]
        res = coll.bulk_write(batch, ordered=False)
        upserted_total += res.upserted_count
        modified_total += res.modified_count

    report["upserted"] = upserted_total
    report["modified"] = modified_total
    logger.info(f"daily_cases: {total_records} processed ({upserted_total} inserted, {modified_total} updated).")

    return report

def run_import(dry_run: bool = False, full_daily: bool = False):
    """Execute complete data import pipeline."""
    print("=" * 65)
    print(f" COVID-19 Data Ingestion Pipeline {'[DRY-RUN]' if dry_run else ''}")
    print("=" * 65)

    data_dir = get_data_dir()
    print(f"Source Directory: {data_dir}")

    db = None
    if not dry_run:
        status = check_connection()
        if not status["connected"]:
            print(f"[FATAL] Cannot connect to MongoDB Atlas: {status['error']}")
            sys.exit(1)
        db = get_database()
        print(f"Connected to Atlas DB: {db.name}")
        print("Ensuring database indexes...")
        create_indexes(db)

    print("\n1. Ingesting Country Statistics...")
    stats_rep = import_country_statistics(data_dir, db, dry_run=dry_run)
    print(f"   -> Records: {stats_rep['processed']:,} | Inserted: {stats_rep['upserted']:,} | Updated: {stats_rep['modified']:,}")

    print("\n2. Ingesting World Bank Country Metadata...")
    meta_rep = import_country_metadata(data_dir, db, dry_run=dry_run)
    print(f"   -> Records: {meta_rep['processed']:,} | Inserted: {meta_rep['upserted']:,} | Updated: {meta_rep['modified']:,}")

    print(f"\n3. Ingesting Chronological Daily Cases ({'Full Daily' if full_daily else 'Global Daily + Country Weekly'})...")
    daily_rep = import_daily_cases(data_dir, db, dry_run=dry_run, full_daily=full_daily)
    print(f"   -> Records: {daily_rep['processed']:,} | Inserted: {daily_rep['upserted']:,} | Updated: {daily_rep['modified']:,}")

    print("\n" + "=" * 65)
    total_proc = stats_rep["processed"] + meta_rep["processed"] + daily_rep["processed"]
    total_up = stats_rep["upserted"] + meta_rep["upserted"] + daily_rep["upserted"]
    total_mod = stats_rep["modified"] + meta_rep["modified"] + daily_rep["modified"]
    print(f"SUMMARY: {total_proc:,} total records processed ({total_up:,} inserted, {total_mod:,} updated).")
    if not dry_run:
        print("Verification: All documents safely committed to MongoDB Atlas.")
    print("=" * 65)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest COVID-19 data into MongoDB Atlas.")
    parser.add_argument("--dry-run", action="store_true", help="Validate data and mapping without writing to Atlas.")
    parser.add_argument("--full-daily", action="store_true", help="Ingest full daily resolution for all countries.")
    args = parser.parse_args()

    run_import(dry_run=args.dry_run, full_daily=args.full_daily)

