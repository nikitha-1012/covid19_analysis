"""
MongoDB Aggregation Pipelines and Analytical Query Services
------------------------------------------------------------
Implements optimized MongoDB aggregation pipelines and queries for Big Data Analytics:
- Global KPI aggregation ($group, $project, $round)
- Top-N rankings by confirmed cases, deaths, and mortality rate
- Continent-level aggregations
- Country search and multi-country comparison ($in, regex)
- Chronological time-series queries for charting
- Safe database metadata and diagnostics
"""

import re
import logging
from typing import Dict, Any, List, Optional
from backend.database import get_database

logger = logging.getLogger("covid19.queries")

def get_global_summary() -> Dict[str, Any]:
    """
    MongoDB Aggregation Pipeline:
    Calculates worldwide totals and global case-fatality rate across country_statistics.
    """
    db = get_database()
    coll = db["country_statistics"]

    pipeline = [
        {
            "$group": {
                "_id": None,
                "total_confirmed": {"$sum": "$confirmed"},
                "total_deaths": {"$sum": "$deaths"},
                "total_recovered": {"$sum": "$recovered"},
                "total_active": {"$sum": "$active"},
                "total_countries": {"$sum": 1}
            }
        },
        {
            "$project": {
                "_id": 0,
                "total_confirmed": 1,
                "total_deaths": 1,
                "total_recovered": 1,
                "total_active": 1,
                "total_countries": 1,
                "mortality_rate": {
                    "$cond": [
                        {"$gt": ["$total_confirmed", 0]},
                        {
                            "$round": [
                                {
                                    "$multiply": [
                                        {"$divide": ["$total_deaths", "$total_confirmed"]},
                                        100
                                    ]
                                },
                                2
                            ]
                        },
                        0.0
                    ]
                }
            }
        }
    ]

    results = list(coll.aggregate(pipeline))
    if not results:
        return {
            "total_confirmed": 0,
            "total_deaths": 0,
            "total_recovered": 0,
            "total_active": 0,
            "total_countries": 0,
            "mortality_rate": 0.0,
            "last_updated": "2023-03-09",
            "recovery_data_note": "Reliable global recovery reporting ceased in mid-2021 by JHU CSSE."
        }

    summary = results[0]
    summary["last_updated"] = "2023-03-09"
    summary["recovery_data_note"] = "Reliable global recovery reporting ceased in mid-2021 by JHU CSSE."
    return summary

def get_top_countries_by_cases(limit: int = 10) -> List[Dict[str, Any]]:
    """Retrieve top N countries with highest confirmed cases."""
    db = get_database()
    coll = db["country_statistics"]
    cursor = coll.find(
        {},
        {"_id": 0, "country": 1, "confirmed": 1, "deaths": 1, "mortality_rate": 1, "continent": 1}
    ).sort("confirmed", -1).limit(limit)
    return list(cursor)

def get_top_countries_by_deaths(limit: int = 10) -> List[Dict[str, Any]]:
    """Retrieve top N countries with highest total deaths."""
    db = get_database()
    coll = db["country_statistics"]
    cursor = coll.find(
        {},
        {"_id": 0, "country": 1, "deaths": 1, "confirmed": 1, "mortality_rate": 1, "continent": 1}
    ).sort("deaths", -1).limit(limit)
    return list(cursor)

def get_top_countries_by_mortality(min_confirmed: int = 10000, limit: int = 10) -> List[Dict[str, Any]]:
    """Retrieve top countries by mortality rate among those with significant confirmed cases."""
    db = get_database()
    coll = db["country_statistics"]
    cursor = coll.find(
        {"confirmed": {"$gte": min_confirmed}},
        {"_id": 0, "country": 1, "mortality_rate": 1, "confirmed": 1, "deaths": 1, "continent": 1}
    ).sort("mortality_rate", -1).limit(limit)
    return list(cursor)

def get_continent_breakdown() -> List[Dict[str, Any]]:
    """
    MongoDB Aggregation Pipeline:
    Aggregates metrics by continent: total cases, total deaths, country counts,
    and aggregate continent-level mortality rate.
    """
    db = get_database()
    coll = db["country_statistics"]

    pipeline = [
        {"$match": {"continent": {"$nin": ["Unknown", None, ""]}}},
        {
            "$group": {
                "_id": "$continent",
                "total_confirmed": {"$sum": "$confirmed"},
                "total_deaths": {"$sum": "$deaths"},
                "total_active": {"$sum": "$active"},
                "country_count": {"$sum": 1}
            }
        },
        {
            "$project": {
                "_id": 0,
                "continent": "$_id",
                "total_confirmed": 1,
                "total_deaths": 1,
                "total_active": 1,
                "country_count": 1,
                "mortality_rate": {
                    "$cond": [
                        {"$gt": ["$total_confirmed", 0]},
                        {
                            "$round": [
                                {
                                    "$multiply": [
                                        {"$divide": ["$total_deaths", "$total_confirmed"]},
                                        100
                                    ]
                                },
                                2
                            ]
                        },
                        0.0
                    ]
                }
            }
        },
        {"$sort": {"total_confirmed": -1}}
    ]

    return list(coll.aggregate(pipeline))

def search_countries(query: str, limit: int = 15) -> List[Dict[str, Any]]:
    """Search for countries using case-insensitive regex prefix or substring match."""
    if not query or not query.strip():
        return []
    
    db = get_database()
    coll = db["country_statistics"]
    escaped = re.escape(query.strip())
    pattern = f".*{escaped}.*"
    
    cursor = coll.find(
        {"country": {"$regex": pattern, "$options": "i"}},
        {"_id": 0, "country": 1, "confirmed": 1, "deaths": 1, "mortality_rate": 1, "continent": 1}
    ).sort("confirmed", -1).limit(limit)
    
    return list(cursor)

def get_country_details(country_name: str) -> Optional[Dict[str, Any]]:
    """Retrieve detailed country statistics along with World Bank socioeconomic metadata."""
    db = get_database()
    stats_coll = db["country_statistics"]
    meta_coll = db["country_metadata"]

    # Match country by exact name or case-insensitive fallback
    country_doc = stats_coll.find_one({"country": country_name}, {"_id": 0})
    if not country_doc:
        country_doc = stats_coll.find_one(
            {"country": {"$regex": f"^{re.escape(country_name)}$", "$options": "i"}},
            {"_id": 0}
        )
    if not country_doc:
        return None

    # Fetch socioeconomic metadata
    canonical_name = country_doc["country"]
    meta_doc = meta_coll.find_one({"country": canonical_name}, {"_id": 0})
    country_doc["metadata"] = meta_doc or {}

    return country_doc

def compare_countries(country_names: List[str]) -> List[Dict[str, Any]]:
    """Compare multiple countries using MongoDB $in query operator."""
    if not country_names:
        return []

    db = get_database()
    stats_coll = db["country_statistics"]
    meta_coll = db["country_metadata"]

    stats_list = list(stats_coll.find({"country": {"$in": country_names}}, {"_id": 0}))
    meta_map = {
        m["country"]: m
        for m in meta_coll.find({"country": {"$in": country_names}}, {"_id": 0})
    }

    for item in stats_list:
        cname = item["country"]
        item["metadata"] = meta_map.get(cname, {})

    return stats_list

def get_trend(country_name: str = "Global", limit: int = 500) -> List[Dict[str, Any]]:
    """
    Retrieve chronological time-series points for Global or individual countries.
    Preserves date ordering.
    """
    db = get_database()
    coll = db["daily_cases"]

    # Match exact or case-insensitive
    query: Dict[str, Any] = {"country": country_name}
    if country_name.lower() != "global":
        existing = coll.find_one({"country": {"$regex": f"^{re.escape(country_name)}$", "$options": "i"}})
        if existing:
            query = {"country": existing["country"]}

    cursor = coll.find(
        query,
        {
            "_id": 0,
            "date": 1,
            "confirmed": 1,
            "deaths": 1,
            "recovered": 1,
            "daily_new_cases": 1,
            "daily_new_deaths": 1
        }
    ).sort("date", 1).limit(limit)

    return list(cursor)

def get_database_status() -> Dict[str, Any]:
    """Retrieve live MongoDB Atlas database telemetry, collection counts, and storage stats."""
    db = get_database()
    client = db.client

    try:
        ping_res = client.admin.command("ping")
        stats = db.command("dbStats")
        coll_names = db.list_collection_names()
        
        counts = {}
        for c in coll_names:
            counts[c] = db[c].estimated_document_count()

        return {
            "connected": True,
            "database_name": db.name,
            "ping_ok": ping_res.get("ok", 1) == 1,
            "collections": sorted(coll_names),
            "document_counts": counts,
            "data_size_kb": round(stats.get("dataSize", 0) / 1024, 2),
            "storage_size_kb": round(stats.get("storageSize", 0) / 1024, 2),
            "index_size_kb": round(stats.get("indexSize", 0) / 1024, 2),
            "last_dataset_date": "2023-03-09"
        }
    except Exception as e:
        logger.error(f"Failed to query database status: {e}")
        return {
            "connected": False,
            "database_name": db.name,
            "ping_ok": False,
            "collections": [],
            "document_counts": {},
            "error": str(e)
        }

