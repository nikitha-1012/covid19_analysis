"""
FastAPI COVID-19 Analytics Backend
----------------------------------
Serves REST API endpoints backed by MongoDB Atlas aggregation pipelines,
along with static dashboard web assets.
"""

import os
import sys
import logging
from typing import List, Optional
from fastapi import FastAPI, Query, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse

# Ensure root directory is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from backend.database import check_connection, close_connection
from backend.queries import (
    get_global_summary,
    get_top_countries_by_cases,
    get_top_countries_by_deaths,
    get_top_countries_by_mortality,
    get_continent_breakdown,
    search_countries,
    get_country_details,
    compare_countries,
    get_trend,
    get_database_status
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("covid19.api")

app = FastAPI(
    title="COVID-19 Big Data Analytics API",
    description="MongoDB Atlas backed analytics and aggregation service for COVID-19 pandemic data.",
    version="1.0.0"
)

# Enable CORS for local dev servers (e.g., Vite/React dev server on 5173 or others)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("shutdown")
def shutdown_event():
    close_connection()

# -------------------------------------------------------------------------
# API Endpoints
# -------------------------------------------------------------------------

@app.get("/api/health", tags=["System"])
def health_check():
    """Report application and database connection status without exposing secrets."""
    db_status = check_connection()
    is_healthy = db_status.get("connected", False) and db_status.get("ping", False)
    
    return {
        "status": "healthy" if is_healthy else "degraded",
        "database": {
            "connected": db_status.get("connected", False),
            "ping": db_status.get("ping", False),
            "target_db": db_status.get("database", "covid19_db")
        },
        "service": "COVID-19 BDA Analytics API"
    }

@app.get("/api/database-status", tags=["System"])
def database_telemetry():
    """Return live MongoDB Atlas statistics, collection counts, and dataset dates."""
    return get_database_status()

@app.get("/api/summary", tags=["Analytics"])
def global_summary():
    """Retrieve global statistics calculated via MongoDB aggregation pipeline."""
    try:
        return get_global_summary()
    except Exception as e:
        logger.error(f"Error calculating global summary: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve global summary from database.")

@app.get("/api/countries/top-cases", tags=["Countries"])
def top_countries_cases(limit: int = Query(default=10, ge=1, le=50)):
    """Retrieve countries with highest confirmed cases."""
    try:
        return get_top_countries_by_cases(limit=limit)
    except Exception as e:
        logger.error(f"Error fetching top cases: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch top countries by cases.")

@app.get("/api/countries/top-deaths", tags=["Countries"])
def top_countries_deaths(limit: int = Query(default=10, ge=1, le=50)):
    """Retrieve countries with highest death counts."""
    try:
        return get_top_countries_by_deaths(limit=limit)
    except Exception as e:
        logger.error(f"Error fetching top deaths: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch top countries by deaths.")

@app.get("/api/countries/top-mortality", tags=["Countries"])
def top_countries_mortality(
    min_confirmed: int = Query(default=10000, ge=0),
    limit: int = Query(default=10, ge=1, le=50)
):
    """Retrieve countries with highest mortality rate among countries with confirmed >= min_confirmed."""
    try:
        return get_top_countries_by_mortality(min_confirmed=min_confirmed, limit=limit)
    except Exception as e:
        logger.error(f"Error fetching top mortality: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch top countries by mortality.")

@app.get("/api/continents", tags=["Analytics"])
def continent_analytics():
    """Retrieve continent-level aggregates computed via MongoDB aggregation pipeline."""
    try:
        return get_continent_breakdown()
    except Exception as e:
        logger.error(f"Error fetching continent breakdown: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch continent analytics.")

@app.get("/api/countries/search", tags=["Countries"])
def search_country_list(q: str = Query(default="", min_length=1, max_length=100)):
    """Search for countries by name."""
    try:
        return search_countries(query=q)
    except Exception as e:
        logger.error(f"Error searching countries: {e}")
        raise HTTPException(status_code=500, detail="Search failed.")

@app.get("/api/countries/compare", tags=["Countries"])
def compare_country_list(countries: str = Query(..., description="Comma-separated country names")):
    """Compare selected countries side by side."""
    country_list = [c.strip() for c in countries.split(",") if c.strip()]
    if not country_list:
        raise HTTPException(status_code=400, detail="Please provide at least one country name.")
    if len(country_list) > 10:
        raise HTTPException(status_code=400, detail="Cannot compare more than 10 countries simultaneously.")
    try:
        return compare_countries(country_list)
    except Exception as e:
        logger.error(f"Error comparing countries: {e}")
        raise HTTPException(status_code=500, detail="Country comparison query failed.")

@app.get("/api/countries/{country_name}", tags=["Countries"])
def country_details(country_name: str):
    """Retrieve detailed statistics and socioeconomic metadata for a specific country."""
    try:
        doc = get_country_details(country_name)
        if not doc:
            raise HTTPException(status_code=404, detail=f"Country '{country_name}' not found.")
        return doc
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving country details for {country_name}: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve country data.")

@app.get("/api/trends/{country_name}", tags=["Time Series"])
def time_series_trend(country_name: str, limit: int = Query(default=500, ge=10, le=2000)):
    """Retrieve chronological case & death trend points for Global or individual countries."""
    try:
        trend = get_trend(country_name=country_name, limit=limit)
        if not trend and country_name.lower() != "global":
            # Check if country exists
            existing = get_country_details(country_name)
            if not existing:
                raise HTTPException(status_code=404, detail=f"Country '{country_name}' not found.")
        return trend
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving trend for {country_name}: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve trend data.")

# -------------------------------------------------------------------------
# Frontend Static Asset Serving
# -------------------------------------------------------------------------

static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.isdir(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

@app.get("/", include_in_schema=False)
def serve_dashboard():
    """Serve the single-page dashboard application."""
    index_path = os.path.join(os.path.dirname(__file__), "static", "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return JSONResponse(
        content={
            "message": "COVID-19 Big Data Analytics API is running.",
            "docs": "/docs",
            "dashboard": "Frontend build pending in backend/static."
        }
    )

