"""
Safe MongoDB Atlas Connection Test Script
-----------------------------------------
Validates MongoDB Atlas connectivity, verifies cluster reachability, and
safely reports database and collection statistics without exposing credentials.
"""

import os
import re
import sys
import time
from dotenv import load_dotenv

# Configure DNS resolver fallback to prevent router timeouts on SRV lookups
try:
    import dns.resolver
    dns.resolver.default_resolver = dns.resolver.Resolver(configure=False)
    dns.resolver.default_resolver.nameservers = ['8.8.8.8', '1.1.1.1']
except Exception:
    pass

from pymongo import MongoClient
from pymongo.errors import (
    ConfigurationError,
    ConnectionFailure,
    OperationFailure,
    ServerSelectionTimeoutError,
)

def mask_uri(uri: str) -> str:
    """Mask credentials in a MongoDB URI for safe display."""
    if not uri:
        return "[NOT SET]"
    return re.sub(r"://([^:]+):([^@]+)@", r"://\1:****@", uri)

def sanitize_uri(uri: str) -> str:
    """Remove any inadvertent template angle brackets around passwords."""
    if not uri:
        return ""
    return re.sub(r":<([^>]+)>@", r":\1@", uri.strip())

def test_connection():
    # Load .env file
    load_dotenv(override=True)
    
    raw_uri = os.getenv("MONGODB_URI")
    db_name = os.getenv("MONGODB_DB_NAME", "covid19_db")
    
    if not raw_uri:
        # Fallback check if user pasted bare URI into .env
        env_path = os.path.join(os.path.dirname(__file__), ".env")
        if os.path.exists(env_path):
            with open(env_path, "r", encoding="utf-8") as f:
                content = f.read()
                for line in content.splitlines():
                    line = line.strip()
                    if line.startswith("mongodb"):
                        raw_uri = line
                        break

    if not raw_uri:
        print("[ERROR] MONGODB_URI is not set in your .env file.")
        print("Please check your .env file and ensure it contains:")
        print("  MONGODB_URI=mongodb+srv://<username>:<password>@cluster.mongodb.net/?retryWrites=true&w=majority")
        return False

    uri = sanitize_uri(raw_uri)
    masked = mask_uri(uri)
    
    print("=" * 60)
    print(" MongoDB Atlas Connection Diagnostic")
    print("=" * 60)
    print(f"Target Database : {db_name}")
    print(f"Connection URI  : {masked}")
    print("Connecting to cluster...")

    start_time = time.time()
    try:
        client = MongoClient(
            uri,
            serverSelectionTimeoutMS=7000,
            connectTimeoutMS=7000,
            socketTimeoutMS=10000,
            appName="COVID19ClusterTest"
        )
        
        # Ping the admin database
        ping_response = client.admin.command("ping")
        elapsed = (time.time() - start_time) * 1000

        print(f"[SUCCESS] MongoDB Atlas ping successful! ({elapsed:.1f} ms)")
        print(f"Server response : {ping_response}")

        # List existing databases
        dbs = client.list_database_names()
        print(f"Visible Databases: {dbs}")

        # Check target database status
        db = client[db_name]
        collections = db.list_collection_names()
        print(f"Target DB '{db_name}' Collections: {collections if collections else 'None yet (empty database)'}")

        if collections:
            print("\nCollection Document Counts:")
            for coll_name in sorted(collections):
                count = db[coll_name].count_documents({})
                print(f"  - {coll_name:22s}: {count:,} documents")

        print("=" * 60)
        print("MongoDB Atlas integration verified successfully.")
        print("=" * 60)
        client.close()
        return True

    except ServerSelectionTimeoutError as e:
        print("\n[ERROR] Connection timed out while reaching MongoDB Atlas.")
        print("Likely causes:")
        print("  1. IP Access List: Ensure your current IP is whitelisted in MongoDB Atlas.")
        print("     (Atlas -> Network Access -> Add IP Address -> 'Allow Access from Anywhere' or your IP)")
        print("  2. Cluster is paused or initiating.")
        print("  3. DNS lookup issues on your local router.")
        return False

    except OperationFailure as e:
        print(f"\n[ERROR] Authentication failed: {e.details.get('errmsg', str(e))}")
        print("Likely causes:")
        print("  1. Incorrect username or password in MONGODB_URI.")
        print("  2. Special characters in password need URL encoding (e.g., %40 for @, %23 for #).")
        print("  3. Database user does not have readWrite permissions on 'covid19_db'.")
        return False

    except ConfigurationError as e:
        print(f"\n[ERROR] Configuration / URI error: {e}")
        return False

    except Exception as e:
        print(f"\n[ERROR] Unexpected connection error: {type(e).__name__}: {e}")
        return False

if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)

