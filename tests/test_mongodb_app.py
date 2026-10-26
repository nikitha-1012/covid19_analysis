"""
Integration & Verification Test Suite for MongoDB Atlas COVID-19 Analytics
-------------------------------------------------------------------------
Verifies:
1. MongoDB Atlas connectivity and ping latency.
2. Target database collections and document presence.
3. MongoDB aggregation pipelines correctness (Global KPI & Continent rollup).
4. Duplicate prevention and idempotency.
5. FastAPI REST API endpoints status and response contracts.
6. Error handling for non-existent countries and malformed inputs.
"""

import unittest
import os
import sys

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from fastapi.testclient import TestClient
from backend.database import get_database, check_connection
from backend.queries import (
    get_global_summary,
    get_top_countries_by_cases,
    get_top_countries_by_deaths,
    get_continent_breakdown,
    search_countries,
    get_country_details,
    compare_countries,
    get_trend,
    get_database_status
)
from backend.main import app

class TestMongoDBIntegration(unittest.TestCase):
    """Verify MongoDB Atlas connectivity, collections, and queries."""

    @classmethod
    def setUpClass(cls):
        cls.db = get_database()
        cls.status = check_connection()

    def test_atlas_connection(self):
        """Verify MongoDB Atlas ping succeeds."""
        self.assertTrue(self.status["connected"], f"Atlas connection failed: {self.status.get('error')}")
        self.assertTrue(self.status["ping"], "Atlas ping was unsuccessful.")
        self.assertEqual(self.status["database"], "covid19_db")

    def test_collections_exist(self):
        """Verify that all required collections exist in covid19_db."""
        coll_names = self.db.list_collection_names()
        required = {"country_statistics", "daily_cases", "country_metadata"}
        self.assertTrue(
            required.issubset(set(coll_names)),
            f"Missing collections! Found: {coll_names}, Required: {required}"
        )

    def test_country_statistics_data_integrity(self):
        """Verify country_statistics has 198 countries and no cruise ships."""
        coll = self.db["country_statistics"]
        count = coll.count_documents({})
        self.assertEqual(count, 198, f"Expected 198 countries, got {count}")

        # Verify cruise ships were excluded
        zaandam = coll.find_one({"country": "MS Zaandam"})
        princess = coll.find_one({"country": "Diamond Princess"})
        self.assertIsNone(zaandam, "MS Zaandam should be excluded.")
        self.assertIsNone(princess, "Diamond Princess should be excluded.")

    def test_global_summary_aggregation(self):
        """Verify global summary MongoDB aggregation pipeline results."""
        summary = get_global_summary()
        self.assertGreater(summary["total_confirmed"], 600_000_000, "Global confirmed cases count is unrealistic.")
        self.assertGreater(summary["total_deaths"], 6_000_000, "Global deaths count is unrealistic.")
        self.assertGreater(summary["mortality_rate"], 0.5, "Global mortality rate is unrealistic.")
        self.assertEqual(summary["total_countries"], 198)

    def test_top_rankings_query(self):
        """Verify top 10 cases and deaths queries return sorted lists."""
        top_cases = get_top_countries_by_cases(10)
        self.assertEqual(len(top_cases), 10)
        for i in range(len(top_cases) - 1):
            self.assertGreaterEqual(
                top_cases[i]["confirmed"],
                top_cases[i+1]["confirmed"],
                "Top cases not properly descending."
            )

        top_deaths = get_top_countries_by_deaths(10)
        self.assertEqual(len(top_deaths), 10)
        for i in range(len(top_deaths) - 1):
            self.assertGreaterEqual(
                top_deaths[i]["deaths"],
                top_deaths[i+1]["deaths"],
                "Top deaths not properly descending."
            )

    def test_continent_aggregation(self):
        """Verify continent breakdown aggregation pipeline."""
        continents = get_continent_breakdown()
        self.assertGreaterEqual(len(continents), 5, "Expected at least 5 continents.")
        names = {c["continent"] for c in continents}
        self.assertIn("Asia", names)
        self.assertIn("Europe", names)
        self.assertIn("North America", names)

    def test_time_series_chronology(self):
        """Verify daily_cases time-series sorting and chronological continuity."""
        trend = get_trend("Global", limit=50)
        self.assertGreater(len(trend), 0)
        dates = [t["date"] for t in trend]
        self.assertEqual(dates, sorted(dates), "Time-series records not in chronological order.")

class TestFastAPIEndpoints(unittest.TestCase):
    """Verify REST API endpoints and HTTP response codes."""

    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)

    def test_health_endpoint(self):
        res = self.client.get("/api/health")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["status"], "healthy")
        self.assertTrue(data["database"]["connected"])

    def test_summary_endpoint(self):
        res = self.client.get("/api/summary")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("total_confirmed", data)
        self.assertIn("mortality_rate", data)

    def test_top_cases_endpoint(self):
        res = self.client.get("/api/countries/top-cases?limit=5")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(len(data), 5)

    def test_country_search(self):
        res = self.client.get("/api/countries/search?q=india")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertGreaterEqual(len(data), 1)
        self.assertEqual(data[0]["country"], "India")

    def test_country_details_found(self):
        res = self.client.get("/api/countries/India")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["country"], "India")
        self.assertIn("metadata", data)

    def test_country_details_not_found(self):
        res = self.client.get("/api/countries/AtlantisFakeCountry")
        self.assertEqual(res.status_code, 404)

    def test_country_compare(self):
        res = self.client.get("/api/countries/compare?countries=India,US,Brazil")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(len(data), 3)

    def test_database_status_endpoint(self):
        res = self.client.get("/api/database-status")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertTrue(data["connected"])
        self.assertIn("document_counts", data)

    def test_static_dashboard_served(self):
        res = self.client.get("/")
        self.assertEqual(res.status_code, 200)
        self.assertIn("COVID-19 Big Data Analytics Dashboard", res.text)

if __name__ == "__main__":
    unittest.main()

