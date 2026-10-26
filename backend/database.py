"""
MongoDB Connection and Database Management Module
-------------------------------------------------
Provides singleton access to MongoDB Atlas, connection pooling,
error handling, credential masking, and automatic collection indexing.
"""

import os
import re
import logging
from typing import Optional, Dict, Any
from dotenv import load_dotenv

# Ensure robust DNS resolution for SRV records
try:
    import dns.resolver
    dns.resolver.default_resolver = dns.resolver.Resolver(configure=False)
    dns.resolver.default_resolver.nameservers = ['8.8.8.8', '1.1.1.1']
except Exception:
    pass

from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.database import Database
from pymongo.errors import PyMongoError, ConnectionFailure, ServerSelectionTimeoutError

logger = logging.getLogger("covid19.database")

# Global singleton client instance
_mongo_client: Optional[MongoClient] = None

def mask_uri(uri: str) -> str:
    """Mask credentials in connection URI to prevent accidental leakage in logs."""
    if not uri:
        return "[NOT SET]"
    return re.sub(r"://([^:]+):([^@]+)@", r"://\1:****@", uri)

def sanitize_uri(uri: str) -> str:
    """Strip inadvertent template angle brackets around passwords if present."""
    if not uri:
        return ""
    return re.sub(r":<([^>]+)>@", r":\1@", uri.strip())

def get_connection_uri() -> str:
    """Read MONGODB_URI from environment with fallback safety."""
    load_dotenv(override=True)
    uri = os.getenv("MONGODB_URI")
    if not uri:
        # Fallback to inspect .env directly if variable wasn't exported
        env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
        if os.path.exists(env_path):
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("MONGODB_URI="):
                        uri = line.split("=", 1)[1].strip()
                        break
                    elif line.startswith("mongodb"):
                        uri = line
                        break
    if not uri:
        raise ValueError("MONGODB_URI not found in environment or .env file.")
    return sanitize_uri(uri)

def get_database_name() -> str:
    """Get the target database name from environment, default 'covid19_db'."""
    load_dotenv()
    return os.getenv("MONGODB_DB_NAME", "covid19_db")

def get_client() -> MongoClient:
    """Get or initialize the singleton MongoClient."""
    global _mongo_client
    if _mongo_client is None:
        uri = get_connection_uri()
        logger.info(f"Connecting to MongoDB Atlas: {mask_uri(uri)}")
        _mongo_client = MongoClient(
            uri,
            serverSelectionTimeoutMS=7000,
            connectTimeoutMS=7000,
            socketTimeoutMS=10000,
            maxPoolSize=50,
            minPoolSize=5,
            retryWrites=True,
            appName="COVID19AnalyticsApp"
        )
    return _mongo_client

def get_database(db_name: Optional[str] = None) -> Database:
    """Get the MongoDB database instance."""
    client = get_client()
    target_db = db_name or get_database_name()
    return client[target_db]

def check_connection() -> Dict[str, Any]:
    """
    Safely ping MongoDB Atlas and return status dictionary.
    Excludes passwords and sensitive connection parameters.
    """
    try:
        client = get_client()
        ping_res = client.admin.command("ping")
        db_name = get_database_name()
        db = client[db_name]
        collections = db.list_collection_names()
        
        counts = {}
        for coll in collections:
            counts[coll] = db[coll].estimated_document_count()
            
        return {
            "connected": True,
            "database": db_name,
            "ping": ping_res.get("ok", 1) == 1,
            "collections": collections,
            "document_counts": counts,
            "error": None
        }
    except Exception as e:
        logger.error(f"MongoDB connection check failed: {type(e).__name__}: {e}")
        return {
            "connected": False,
            "database": get_database_name(),
            "ping": False,
            "collections": [],
            "document_counts": {},
            "error": str(e)
        }

def create_indexes(db: Optional[Database] = None) -> Dict[str, list]:
    """
    Create useful indexes on collections to ensure query performance
    and prevent duplicate records.
    """
    if db is None:
        db = get_database()
        
    created = {}
    
    # 1. country_statistics collection
    stats_coll = db["country_statistics"]
    idx_country = stats_coll.create_index([("country", ASCENDING)], unique=True, name="idx_country_unique")
    idx_confirmed = stats_coll.create_index([("confirmed", DESCENDING)], name="idx_confirmed_desc")
    idx_deaths = stats_coll.create_index([("deaths", DESCENDING)], name="idx_deaths_desc")
    idx_mortality = stats_coll.create_index([("mortality_rate", DESCENDING)], name="idx_mortality_desc")
    idx_continent = stats_coll.create_index([("continent", ASCENDING)], name="idx_continent")
    created["country_statistics"] = [idx_country, idx_confirmed, idx_deaths, idx_mortality, idx_continent]

    # 2. daily_cases collection (time-series)
    daily_coll = db["daily_cases"]
    idx_compound = daily_coll.create_index(
        [("country", ASCENDING), ("date", ASCENDING)],
        unique=True,
        name="idx_country_date_unique"
    )
    idx_date = daily_coll.create_index([("date", ASCENDING)], name="idx_date_asc")
    idx_country_only = daily_coll.create_index([("country", ASCENDING)], name="idx_country_asc")
    created["daily_cases"] = [idx_compound, idx_date, idx_country_only]

    # 3. country_metadata collection (World Bank socioeconomic data)
    meta_coll = db["country_metadata"]
    idx_meta_country = meta_coll.create_index([("country", ASCENDING)], unique=True, name="idx_meta_country_unique")
    idx_meta_continent = meta_coll.create_index([("continent", ASCENDING)], name="idx_meta_continent")
    created["country_metadata"] = [idx_meta_country, idx_meta_continent]

    return created

def close_connection():
    """Safely close client connections."""
    global _mongo_client
    if _mongo_client is not None:
        _mongo_client.close()
        _mongo_client = None
        logger.info("MongoDB connection closed.")

