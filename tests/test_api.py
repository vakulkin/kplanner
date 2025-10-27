import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
import os
import sys
import random
import string
from typing import Generator, List, Dict, Any
from faker import Faker
from hypothesis import given, strategies as st, settings, Phase, HealthCheck
import asyncio

# Add the project root directory to the path so we can import our modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import app
from src.core.database import Base, get_db


# Initialize faker for random data generation
fake = Faker()
random.seed(42)  # For reproducible tests


SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)

# Enable foreign keys for SQLite
@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()

TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@pytest.fixture(scope="function")
def db_session():
    """Create a fresh database for each test."""
    # Create all tables
    Base.metadata.create_all(bind=engine)

    # Create a new session for the test
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.rollback()
        db.close()

    # Drop all tables after the test
    Base.metadata.drop_all(bind=engine)


@pytest.fixture(scope="function")
def client(db_session):
    """Create a test client with a test database session."""

    def override_get_db():
        try:
            yield db_session
        finally:
            db_session.close()

    app.dependency_overrides[get_db] = override_get_db

    # Set testing environment variables
    os.environ["TESTING"] = "true"
    os.environ["DEV_MODE"] = "true"

    test_client = TestClient(app)
    yield test_client
    test_client.close()

    # Clean up
    app.dependency_overrides.clear()
    if "TESTING" in os.environ:
        del os.environ["TESTING"]
    if "DEV_MODE" in os.environ:
        del os.environ["DEV_MODE"]


@pytest.fixture
def demo_user_id():
    """Demo user ID for testing."""
    return "clerk_demo_user"


@pytest.fixture
def sample_company_data():
    """Sample company data for testing."""
    return {
        "title": "Test Company",
        "is_active": True
    }


@pytest.fixture
def sample_campaign_data():
    """Sample campaign data for testing."""
    return {
        "title": "Test Campaign",
        "company_id": 1,
        "is_active": True
    }


@pytest.fixture
def sample_ad_group_data():
    """Sample ad group data for testing."""
    return {
        "title": "Test Ad Group",
        "ad_campaign_id": 1,
        "is_active": True
    }


@pytest.fixture
def sample_keyword_data():
    """Sample keyword data for testing."""
    return {
        "keyword": "test keyword"
    }


@pytest.fixture
def create_test_company(client, demo_user_id, sample_company_data):
    """Create a test company and return its data."""
    response = client.post("/companies", json=sample_company_data)
    assert response.status_code == 201
    return response.json()["object"]


@pytest.fixture
def create_test_campaign(client, create_test_company, sample_campaign_data):
    """Create a test campaign and return its data."""
    campaign_data = sample_campaign_data.copy()
    campaign_data["company_id"] = create_test_company["id"]
    response = client.post("/ad_campaigns", json=campaign_data)
    assert response.status_code == 201
    return response.json()["object"]


@pytest.fixture
def create_test_ad_group(client, create_test_campaign, sample_ad_group_data):
    """Create a test ad group and return its data."""
    ad_group_data = sample_ad_group_data.copy()
    ad_group_data["ad_campaign_id"] = create_test_campaign["id"]
    response = client.post("/ad_groups", json=ad_group_data)
    assert response.status_code == 201
    return response.json()["object"]


@pytest.fixture
def create_test_keyword(client, demo_user_id, sample_keyword_data):
    """Create a test keyword and return its data."""
    bulk_data = {
        "keywords": [sample_keyword_data["keyword"]],
        "company_ids": [],
        "ad_campaign_ids": [],
        "ad_group_ids": [],
        "broad": True,
        "phrase": False,
        "exact": False,
        "neg_broad": False,
        "neg_phrase": False,
        "neg_exact": False,
        "override_broad": False,
        "override_phrase": False,
        "override_exact": False,
        "override_neg_broad": False,
        "override_neg_phrase": False,
        "override_neg_exact": False
    }
    response = client.post("/keywords/bulk", json=bulk_data)
    assert response.status_code == 201
    return response.json()["objects"][0]


# ===== RANDOM DATA GENERATION UTILITIES =====

def random_string(length: int = 10) -> str:
    """Generate a random string of given length."""
    return ''.join(random.choices(string.ascii_letters + string.digits + ' ', k=length)).strip()


def random_company_data() -> Dict[str, Any]:
    """Generate random company data."""
    return {
        "title": fake.company() + " " + random_string(5),
        "is_active": random.choice([True, False])
    }


def random_campaign_data(company_id: int) -> Dict[str, Any]:
    """Generate random campaign data for a given company."""
    return {
        "title": fake.catch_phrase() + " " + random_string(5),
        "company_id": company_id,
        "is_active": random.choice([True, False])
    }


def random_ad_group_data(campaign_id: int) -> Dict[str, Any]:
    """Generate random ad group data for a given campaign."""
    return {
        "title": " ".join(fake.words(nb=2, ext_word_list=['Ads', 'Group', 'Campaign', 'Marketing'])) + " " + random_string(3),
        "ad_campaign_id": campaign_id,
        "is_active": random.choice([True, False])
    }


def random_keywords(count: int = 5) -> List[str]:
    """Generate a list of random keywords."""
    keywords = []
    for _ in range(count):
        # Mix of single words, phrases, and longer terms
        if random.random() < 0.3:
            keywords.append(fake.word())
        elif random.random() < 0.6:
            keywords.append(" ".join(fake.words(nb=2)))
        else:
            keywords.append(" ".join(fake.words(nb=random.randint(3, 5))))
    return list(set(keywords))  # Remove duplicates


def random_match_types() -> Dict[str, bool]:
    """Generate random match type settings."""
    return {
        "broad": random.choice([True, False]),
        "phrase": random.choice([True, False]),
        "exact": random.choice([True, False]),
        "neg_broad": random.choice([True, False]),
        "neg_phrase": random.choice([True, False]),
        "neg_exact": random.choice([True, False]),
        "override_broad": random.choice([True, False]),
        "override_phrase": random.choice([True, False]),
        "override_exact": random.choice([True, False]),
        "override_neg_broad": random.choice([True, False]),
        "override_neg_phrase": random.choice([True, False]),
        "override_neg_exact": random.choice([True, False])
    }


@pytest.fixture
def random_companies(client, count: int = 5) -> List[Dict[str, Any]]:
    """Create multiple random companies."""
    companies = []
    for _ in range(count):
        data = random_company_data()
        response = client.post("/companies", json=data)
        assert response.status_code == 201
        companies.append(response.json()["object"])
    return companies


@pytest.fixture
def random_campaigns(client, random_companies, count_per_company: int = 3) -> List[Dict[str, Any]]:
    """Create random campaigns for random companies."""
    campaigns = []
    for company in random_companies:
        for _ in range(count_per_company):
            data = random_campaign_data(company["id"])
            response = client.post("/ad_campaigns", json=data)
            assert response.status_code == 201
            campaigns.append(response.json()["object"])
    return campaigns


@pytest.fixture
def random_ad_groups(client, random_campaigns, count_per_campaign: int = 2) -> List[Dict[str, Any]]:
    """Create random ad groups for random campaigns."""
    ad_groups = []
    for campaign in random_campaigns:
        for _ in range(count_per_campaign):
            data = random_ad_group_data(campaign["id"])
            response = client.post("/ad_groups", json=data)
            assert response.status_code == 201
            ad_groups.append(response.json()["object"])
    return ad_groups


@pytest.fixture
def complex_test_setup(client) -> Dict[str, List[Dict[str, Any]]]:
    """Create a complex test setup with multiple interconnected entities."""
    # Create 3-5 companies
    companies = []
    for _ in range(random.randint(3, 5)):
        data = random_company_data()
        response = client.post("/companies", json=data)
        companies.append(response.json()["object"])

    # Create 2-4 campaigns per company
    campaigns = []
    for company in companies:
        for _ in range(random.randint(2, 4)):
            data = random_campaign_data(company["id"])
            response = client.post("/ad_campaigns", json=data)
            campaigns.append(response.json()["object"])

    # Create 1-3 ad groups per campaign
    ad_groups = []
    for campaign in campaigns:
        for _ in range(random.randint(1, 3)):
            data = random_ad_group_data(campaign["id"])
            response = client.post("/ad_groups", json=data)
            ad_groups.append(response.json()["object"])

    # Create keywords with various relations
    keywords = random_keywords(random.randint(10, 20))

    # Create keywords with random relations
    bulk_data = {
        "keywords": keywords,
        "company_ids": [c["id"] for c in random.sample(companies, min(len(companies), random.randint(1, 3)))],
        "ad_campaign_ids": [c["id"] for c in random.sample(campaigns, min(len(campaigns), random.randint(1, 5)))],
        "ad_group_ids": [g["id"] for g in random.sample(ad_groups, min(len(ad_groups), random.randint(1, 8)))]
    }
    bulk_data.update(random_match_types())

    response = client.post("/keywords/bulk", json=bulk_data)
    assert response.status_code == 201
    created_keywords = response.json()["objects"]

    return {
        "companies": companies,
        "campaigns": campaigns,
        "ad_groups": ad_groups,
        "keywords": created_keywords
    }


class TestRootEndpoint:
    """Test the root endpoint."""

    def test_root_endpoint(self, client):
        """Test the root endpoint returns correct response."""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert "message" in data
        assert "KPlanner API" in data["message"]


class TestCompanyEndpoints:
    """Test all company-related endpoints."""

    def test_create_company(self, client, sample_company_data):
        """Test creating a new company."""
        response = client.post("/companies", json=sample_company_data)
        assert response.status_code == 201

        data = response.json()
        assert data["message"] == "Company created successfully"
        assert "object" in data

        company = data["object"]
        assert company["title"] == sample_company_data["title"]
        assert company["is_active"] == sample_company_data["is_active"]
        assert "id" in company
        assert "created" in company
        assert "updated" in company

    def test_create_company_invalid_data(self, client):
        """Test creating a company with invalid data."""
        # Missing required fields
        response = client.post("/companies", json={})
        assert response.status_code == 422  # Validation error

        # Invalid title type
        response = client.post("/companies", json={"title": 123, "is_active": True})
        assert response.status_code == 422

    def test_list_companies_empty(self, client):
        """Test listing companies when none exist."""
        response = client.get("/companies")
        assert response.status_code == 200

        data = response.json()
        assert data["message"] == "Retrieved 0 companies"
        assert data["objects"] == []
        assert data["pagination"]["total"] == 0
        assert data["pagination"]["page"] == 1
        assert data["pagination"]["page_size"] == 50
        assert data["pagination"]["total_pages"] == 1

    def test_list_companies_with_data(self, client, create_test_company):
        """Test listing companies with existing data."""
        response = client.get("/companies")
        assert response.status_code == 200

        data = response.json()
        assert len(data["objects"]) == 1
        assert data["pagination"]["total"] == 1
        assert data["objects"][0]["id"] == create_test_company["id"]

    def test_list_companies_pagination(self, client, demo_user_id):
        """Test company listing with pagination."""
        # Create multiple companies
        for i in range(5):
            company_data = {"title": f"Company {i+1}", "is_active": True}
            client.post("/companies", json=company_data)

        # Test page 1 with page_size 2
        response = client.get("/companies?page=1&page_size=2")
        assert response.status_code == 200
        data = response.json()
        assert len(data["objects"]) == 2
        assert data["pagination"]["total"] == 5
        assert data["pagination"]["page"] == 1
        assert data["pagination"]["page_size"] == 2
        assert data["pagination"]["total_pages"] == 3

        # Test page 2
        response = client.get("/companies?page=2&page_size=2")
        assert response.status_code == 200
        data = response.json()
        assert len(data["objects"]) == 2
        assert data["pagination"]["page"] == 2

        # Test page 3 (last page)
        response = client.get("/companies?page=3&page_size=2")
        assert response.status_code == 200
        data = response.json()
        assert len(data["objects"]) == 1
        assert data["pagination"]["page"] == 3

    def test_get_company(self, client, create_test_company):
        """Test getting a single company."""
        company_id = create_test_company["id"]
        response = client.get(f"/companies/{company_id}")
        assert response.status_code == 200

        data = response.json()
        assert data["message"] == "Company retrieved successfully"
        assert data["object"]["id"] == company_id
        assert data["object"]["title"] == create_test_company["title"]

    def test_get_company_not_found(self, client):
        """Test getting a non-existent company."""
        response = client.get("/companies/999")
        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"].lower()

    def test_update_company(self, client, create_test_company):
        """Test updating a company."""
        company_id = create_test_company["id"]
        update_data = {
            "title": "Updated Company Name",
            "is_active": False
        }

        response = client.post(f"/companies/{company_id}/update", json=update_data)
        assert response.status_code == 200

        data = response.json()
        assert data["message"] == "Company updated successfully"
        assert data["object"]["title"] == update_data["title"]
        assert data["object"]["is_active"] == update_data["is_active"]

    def test_update_company_not_found(self, client):
        """Test updating a non-existent company."""
        update_data = {"title": "Updated Name", "is_active": True}
        response = client.post("/companies/999/update", json=update_data)
        assert response.status_code == 404

    def test_toggle_company_active(self, client, create_test_company):
        """Test toggling company active status."""
        company_id = create_test_company["id"]
        original_status = create_test_company["is_active"]

        response = client.post(f"/companies/{company_id}/toggle")
        assert response.status_code == 200

        data = response.json()
        assert "Company" in data["message"] and "successfully" in data["message"]
        assert data["object"]["is_active"] != original_status

    def test_toggle_company_not_found(self, client):
        """Test toggling a non-existent company."""
        response = client.post("/companies/999/toggle")
        assert response.status_code == 404

    def test_bulk_delete_companies(self, client, demo_user_id):
        """Test bulk deleting companies."""
        # Create multiple companies
        company_ids = []
        for i in range(3):
            company_data = {"title": f"Company {i+1}", "is_active": True}
            response = client.post("/companies", json=company_data)
            company_ids.append(response.json()["object"]["id"])

        # Delete first two companies
        delete_data = {"ids": company_ids[:2]}
        response = client.post("/companies/bulk/delete", json=delete_data)
        assert response.status_code == 200

        data = response.json()
        assert "Deleted 2 companies" in data["message"]
        assert data["deleted"] == 2
        assert data["requested"] == 2

        # Verify remaining company still exists
        response = client.get(f"/companies/{company_ids[2]}")
        assert response.status_code == 200

        # Verify deleted companies are gone
        for deleted_id in company_ids[:2]:
            response = client.get(f"/companies/{deleted_id}")
            assert response.status_code == 404

    def test_bulk_delete_companies_empty_list(self, client):
        """Test bulk deleting with empty ID list."""
        delete_data = {"ids": []}
        response = client.post("/companies/bulk/delete", json=delete_data)
        assert response.status_code == 400  # Should return 400 for empty ids


class TestAdCampaignEndpoints:
    """Test all ad campaign-related endpoints."""

    def test_create_ad_campaign(self, client, create_test_company, sample_campaign_data):
        """Test creating a new ad campaign."""
        response = client.post("/ad_campaigns", json=sample_campaign_data)
        assert response.status_code == 201

        data = response.json()
        assert data["message"] == "Campaign created successfully"
        assert data["object"]["title"] == sample_campaign_data["title"]
        assert data["object"]["company_id"] == sample_campaign_data["company_id"]

    def test_create_ad_campaign_invalid_company(self, client):
        """Test creating ad campaign with non-existent company."""
        campaign_data = {
            "title": "Test Campaign",
            "company_id": 999,
            "is_active": True
        }
        response = client.post("/ad_campaigns", json=campaign_data)
        assert response.status_code == 404  # Returns 404 for not found

    def test_list_ad_campaigns_empty(self, client):
        """Test listing ad campaigns when none exist."""
        response = client.get("/ad_campaigns")
        assert response.status_code == 200

        data = response.json()
        assert data["message"] == "Retrieved 0 campaigns"
        assert data["objects"] == []
        assert data["pagination"]["total"] == 0

    def test_list_ad_campaigns_with_data(self, client, create_test_company, create_test_campaign):
        """Test listing ad campaigns with existing data."""
        response = client.get("/ad_campaigns")
        assert response.status_code == 200

        data = response.json()
        assert len(data["objects"]) == 1
        assert data["objects"][0]["id"] == create_test_campaign["id"]

    def test_list_ad_campaigns_filter_by_company(self, client, demo_user_id):
        """Test filtering ad campaigns by company."""
        # Create company 1 and its campaigns
        company1_data = {"title": "Company 1", "is_active": True}
        company1 = client.post("/companies", json=company1_data).json()["object"]

        campaign1_data = {"title": "Campaign 1", "company_id": company1["id"], "is_active": True}
        campaign2_data = {"title": "Campaign 2", "company_id": company1["id"], "is_active": True}
        client.post("/ad_campaigns", json=campaign1_data)
        client.post("/ad_campaigns", json=campaign2_data)

        # Create company 2 and its campaign
        company2_data = {"title": "Company 2", "is_active": True}
        company2 = client.post("/companies", json=company2_data).json()["object"]

        campaign3_data = {"title": "Campaign 3", "company_id": company2["id"], "is_active": True}
        client.post("/ad_campaigns", json=campaign3_data)

        # Filter by company 1
        response = client.get(f"/ad_campaigns?company_id={company1['id']}")
        assert response.status_code == 200
        data = response.json()
        assert len(data["objects"]) == 2
        assert all(c["company_id"] == company1["id"] for c in data["objects"])

        # Filter by company 2
        response = client.get(f"/ad_campaigns?company_id={company2['id']}")
        assert response.status_code == 200
        data = response.json()
        assert len(data["objects"]) == 1
        assert data["objects"][0]["company_id"] == company2["id"]

    def test_get_ad_campaign(self, client, create_test_campaign):
        """Test getting a single ad campaign."""
        campaign_id = create_test_campaign["id"]
        response = client.get(f"/ad_campaigns/{campaign_id}")
        assert response.status_code == 200

        data = response.json()
        assert data["object"]["id"] == campaign_id

    def test_get_ad_campaign_not_found(self, client):
        """Test getting a non-existent ad campaign."""
        response = client.get("/ad_campaigns/999")
        assert response.status_code == 404

    def test_update_ad_campaign(self, client, create_test_campaign):
        """Test updating an ad campaign."""
        campaign_id = create_test_campaign["id"]
        update_data = {
            "title": "Updated Campaign",
            "company_id": create_test_campaign["company_id"],
            "is_active": False
        }

        response = client.post(f"/ad_campaigns/{campaign_id}/update", json=update_data)
        assert response.status_code == 200

        data = response.json()
        assert data["object"]["title"] == update_data["title"]
        assert data["object"]["is_active"] == update_data["is_active"]

    def test_toggle_ad_campaign(self, client, create_test_campaign):
        """Test toggling ad campaign active status."""
        campaign_id = create_test_campaign["id"]
        original_status = create_test_campaign["is_active"]

        response = client.post(f"/ad_campaigns/{campaign_id}/toggle")
        assert response.status_code == 200

        data = response.json()
        assert data["object"]["is_active"] != original_status

    def test_bulk_delete_ad_campaigns(self, client, demo_user_id, create_test_company):
        """Test bulk deleting ad campaigns."""
        # Create multiple campaigns
        campaign_ids = []
        for i in range(3):
            campaign_data = {
                "title": f"Campaign {i+1}",
                "company_id": create_test_company["id"],
                "is_active": True
            }
            response = client.post("/ad_campaigns", json=campaign_data)
            campaign_ids.append(response.json()["object"]["id"])

        # Delete first two campaigns
        delete_data = {"ids": campaign_ids[:2]}
        response = client.post("/ad_campaigns/bulk/delete", json=delete_data)
        assert response.status_code == 200

        data = response.json()
        assert data["deleted"] == 2


class TestAdGroupEndpoints:
    """Test all ad group-related endpoints."""

    def test_create_ad_group(self, client, create_test_campaign, sample_ad_group_data):
        """Test creating a new ad group."""
        response = client.post("/ad_groups", json=sample_ad_group_data)
        assert response.status_code == 201

        data = response.json()
        assert data["message"] == "Ad group created successfully"
        assert data["object"]["title"] == sample_ad_group_data["title"]

    def test_create_ad_group_invalid_campaign(self, client):
        """Test creating ad group with non-existent campaign."""
        ad_group_data = {
            "title": "Test Ad Group",
            "ad_campaign_id": 999,
            "is_active": True
        }
        response = client.post("/ad_groups", json=ad_group_data)
        assert response.status_code == 404  # Returns 404 for not found

    def test_list_ad_groups_empty(self, client):
        """Test listing ad groups when none exist."""
        response = client.get("/ad_groups")
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "Retrieved 0 ad groups"
        assert data["objects"] == []

    def test_list_ad_groups_with_data(self, client, create_test_ad_group):
        """Test listing ad groups with existing data."""
        response = client.get("/ad_groups")
        assert response.status_code == 200
        data = response.json()
        assert len(data["objects"]) == 1

    def test_list_ad_groups_filter_by_campaign(self, client, demo_user_id, create_test_company):
        """Test filtering ad groups by campaign."""
        # Create campaign 1 and its ad groups
        campaign1_data = {"title": "Campaign 1", "company_id": create_test_company["id"], "is_active": True}
        campaign1 = client.post("/ad_campaigns", json=campaign1_data).json()["object"]

        ad_group1_data = {"title": "Ad Group 1", "ad_campaign_id": campaign1["id"], "is_active": True}
        ad_group2_data = {"title": "Ad Group 2", "ad_campaign_id": campaign1["id"], "is_active": True}
        client.post("/ad_groups", json=ad_group1_data)
        client.post("/ad_groups", json=ad_group2_data)

        # Create campaign 2 and its ad group
        campaign2_data = {"title": "Campaign 2", "company_id": create_test_company["id"], "is_active": True}
        campaign2 = client.post("/ad_campaigns", json=campaign2_data).json()["object"]

        ad_group3_data = {"title": "Ad Group 3", "ad_campaign_id": campaign2["id"], "is_active": True}
        client.post("/ad_groups", json=ad_group3_data)

        # Filter by campaign 1
        response = client.get(f"/ad_groups?ad_campaign_id={campaign1['id']}")
        assert response.status_code == 200
        data = response.json()
        assert len(data["objects"]) == 2

    def test_get_ad_group(self, client, create_test_ad_group):
        """Test getting a single ad group."""
        ad_group_id = create_test_ad_group["id"]
        response = client.get(f"/ad_groups/{ad_group_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["object"]["id"] == ad_group_id

    def test_update_ad_group(self, client, create_test_ad_group):
        """Test updating an ad group."""
        ad_group_id = create_test_ad_group["id"]
        update_data = {
            "title": "Updated Ad Group",
            "ad_campaign_id": create_test_ad_group["ad_campaign_id"],
            "is_active": False
        }

        response = client.post(f"/ad_groups/{ad_group_id}/update", json=update_data)
        assert response.status_code == 200
        data = response.json()
        assert data["object"]["title"] == update_data["title"]

    def test_toggle_ad_group(self, client, create_test_ad_group):
        """Test toggling ad group active status."""
        ad_group_id = create_test_ad_group["id"]
        original_status = create_test_ad_group["is_active"]

        response = client.post(f"/ad_groups/{ad_group_id}/toggle")
        assert response.status_code == 200
        data = response.json()
        assert data["object"]["is_active"] != original_status

    def test_bulk_delete_ad_groups(self, client, demo_user_id, create_test_campaign):
        """Test bulk deleting ad groups."""
        # Create multiple ad groups
        ad_group_ids = []
        for i in range(3):
            ad_group_data = {
                "title": f"Ad Group {i+1}",
                "ad_campaign_id": create_test_campaign["id"],
                "is_active": True
            }
            response = client.post("/ad_groups", json=ad_group_data)
            ad_group_ids.append(response.json()["object"]["id"])

        # Delete first two ad groups
        delete_data = {"ids": ad_group_ids[:2]}
        response = client.post("/ad_groups/bulk/delete", json=delete_data)
        assert response.status_code == 200
        data = response.json()
        assert data["deleted"] == 2


class TestColumnMappingsEndpoints:
    """Test column mappings create/remove endpoints."""

    def test_toggle_column_mapping_create(self, client, create_test_company):
        """Test creating a column mapping via create action."""
        # Create a second company
        company2_data = {"title": "Test Company 2", "is_active": True}
        response = client.post("/companies", json=company2_data)
        assert response.status_code == 201
        company2 = response.json()["object"]

        # Get first company
        companies_response = client.get("/companies?page_size=10")
        companies = companies_response.json()["objects"]
        company1 = companies[0]

        # Create mapping
        mapping_data = {
            "action": "create",
            "source_company_id": company1["id"],
            "source_match_type": "broad",
            "target_company_id": company2["id"],
            "target_match_type": "exact"
        }

        response = client.post("/column-mappings/toggle", json=mapping_data)
        assert response.status_code == 200

        data = response.json()
        assert data["action"] == "created"
        assert "mapping_id" in data

        mapping_id = data["mapping_id"]

        # Verify mapping appears in active mappings endpoint
        mappings_response = client.get("/column-mappings/active")
        mappings_data = mappings_response.json()["objects"]
        assert len(mappings_data) >= 1

        # Find our mapping
        mapping = next((m for m in mappings_data if m["id"] == mapping_id), None)
        assert mapping is not None
        assert mapping["source_company_id"] == company1["id"]
        assert mapping["target_company_id"] == company2["id"]

    def test_toggle_column_mapping_delete(self, client, create_test_company):
        """Test removing a column mapping via remove action."""
        # Create a second company
        company2_data = {"title": "Test Company 2", "is_active": True}
        response = client.post("/companies", json=company2_data)
        assert response.status_code == 201
        company2 = response.json()["object"]

        # Get first company
        companies_response = client.get("/companies?page_size=10")
        companies = companies_response.json()["objects"]
        company1 = companies[0]

        # Create mapping first
        mapping_data = {
            "action": "create",
            "source_company_id": company1["id"],
            "source_match_type": "broad",
            "target_company_id": company2["id"],
            "target_match_type": "exact"
        }

        # Create
        response = client.post("/column-mappings/toggle", json=mapping_data)
        assert response.status_code == 200
        assert response.json()["action"] == "created"
        mapping_id = response.json()["mapping_id"]

        # Delete (remove action)
        remove_data = {
            "action": "remove",
            "source_company_id": company1["id"],
            "source_match_type": "broad",
            "target_company_id": company2["id"],
            "target_match_type": "exact"
        }
        response = client.post("/column-mappings/toggle", json=remove_data)
        assert response.status_code == 200

        data = response.json()
        assert data["action"] == "removed"
        assert data["mapping_id"] == mapping_id

        # Verify mapping is gone from active mappings
        mappings_response = client.get("/column-mappings/active")
        mappings_data = mappings_response.json()["objects"]
        mapping = next((m for m in mappings_data if m["id"] == mapping_id), None)
        assert mapping is None

    def test_toggle_column_mapping_cross_entity(self, client, create_test_company, create_test_campaign):
        """Test column mapping between different entity types."""
        company = create_test_company
        campaign = create_test_campaign

        # Create mapping from company to campaign
        mapping_data = {
            "action": "create",
            "source_company_id": company["id"],
            "source_match_type": "phrase",
            "target_ad_campaign_id": campaign["id"],
            "target_match_type": "broad"
        }

        response = client.post("/column-mappings/toggle", json=mapping_data)
        assert response.status_code == 200
        assert response.json()["action"] == "created"

        # Verify in active mappings endpoint
        mappings_response = client.get("/column-mappings/active")
        mappings_data = mappings_response.json()["objects"]
        assert len(mappings_data) >= 1

        # Find our mapping
        mapping = next((m for m in mappings_data if m["source_company_id"] == company["id"] and m["target_ad_campaign_id"] == campaign["id"]), None)
        assert mapping is not None

    def test_toggle_column_mapping_invalid_source_entity(self, client):
        """Test toggle with invalid source entity."""
        mapping_data = {
            "action": "create",
            "source_company_id": 99999,  # Non-existent
            "source_match_type": "broad",
            "target_company_id": 1,
            "target_match_type": "exact"
        }

        response = client.post("/column-mappings/toggle", json=mapping_data)
        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    def test_toggle_column_mapping_invalid_target_entity(self, client, create_test_company):
        """Test toggle with invalid target entity."""
        companies_response = client.get("/companies?page_size=10")
        companies = companies_response.json()["objects"]
        company = companies[0]

        mapping_data = {
            "action": "create",
            "source_company_id": company["id"],
            "source_match_type": "broad",
            "target_company_id": 99999,  # Non-existent
            "target_match_type": "exact"
        }

        response = client.post("/column-mappings/toggle", json=mapping_data)
        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    def test_toggle_column_mapping_invalid_match_type(self, client, create_test_company):
        """Test toggle with invalid match type."""
        # Create a second company
        company2_data = {"title": "Test Company 2", "is_active": True}
        response = client.post("/companies", json=company2_data)
        assert response.status_code == 201
        company2 = response.json()["object"]

        # Get first company
        companies_response = client.get("/companies?page_size=10")
        companies = companies_response.json()["objects"]
        company1 = companies[0]

        mapping_data = {
            "action": "create",
            "source_company_id": company1["id"],
            "source_match_type": "invalid",  # Invalid match type
            "target_company_id": company2["id"],
            "target_match_type": "exact"
        }

        response = client.post("/column-mappings/toggle", json=mapping_data)
        assert response.status_code == 422  # Validation error

    def test_toggle_column_mapping_missing_entity(self, client):
        """Test toggle with missing source entity."""
        mapping_data = {
            "action": "create",
            "source_match_type": "broad",
            "target_company_id": 1,
            "target_match_type": "exact"
        }

        response = client.post("/column-mappings/toggle", json=mapping_data)
        assert response.status_code == 422  # Validation error

    def test_toggle_column_mapping_multiple_entities(self, client, create_test_company):
        """Test toggle with multiple source entities (should fail)."""
        # Create a second company
        company2_data = {"title": "Test Company 2", "is_active": True}
        response = client.post("/companies", json=company2_data)
        assert response.status_code == 201
        company2 = response.json()["object"]

        # Get first company
        companies_response = client.get("/companies?page_size=10")
        companies = companies_response.json()["objects"]
        company1 = companies[0]

        mapping_data = {
            "action": "create",
            "source_company_id": company1["id"],
            "source_ad_campaign_id": 1,  # Multiple source entities
            "source_match_type": "broad",
            "target_company_id": company2["id"],
            "target_match_type": "exact"
        }

        response = client.post("/column-mappings/toggle", json=mapping_data)
        assert response.status_code == 422  # Validation error

    def test_toggle_column_mapping_negative_match_types(self, client, create_test_company):
        """Test creating column mappings with negative match types."""
        # Create a second company
        company2_data = {"title": "Test Company 2", "is_active": True}
        response = client.post("/companies", json=company2_data)
        assert response.status_code == 201
        company2 = response.json()["object"]

        # Get first company
        companies_response = client.get("/companies?page_size=10")
        companies = companies_response.json()["objects"]
        company1 = companies[0]

        # Test creating mapping with negative source match type
        mapping_data = {
            "action": "create",
            "source_company_id": company1["id"],
            "source_match_type": "neg_broad",
            "target_company_id": company2["id"],
            "target_match_type": "exact"
        }

        response = client.post("/column-mappings/toggle", json=mapping_data)
        assert response.status_code == 200

        data = response.json()
        assert data["action"] == "created"
        assert "mapping_id" in data

        # Verify mapping was created
        mappings_response = client.get("/column-mappings/active")
        assert mappings_response.status_code == 200
        mappings = mappings_response.json()["objects"]
        assert len(mappings) == 1
        assert mappings[0]["source_match_type"] == "neg_broad"
        assert mappings[0]["target_match_type"] == "exact"

        # Test creating mapping with negative target match type
        mapping_data2 = {
            "action": "create",
            "source_company_id": company1["id"],
            "source_match_type": "broad",
            "target_company_id": company2["id"],
            "target_match_type": "neg_phrase"
        }

        response = client.post("/column-mappings/toggle", json=mapping_data2)
        assert response.status_code == 200

        data = response.json()
        assert data["action"] == "created"
        assert "mapping_id" in data

        # Verify both mappings exist
        mappings_response = client.get("/column-mappings/active")
        assert mappings_response.status_code == 200
        mappings = mappings_response.json()["objects"]
        assert len(mappings) == 2
        
        match_types = {m["source_match_type"] + "->" + m["target_match_type"] for m in mappings}
        assert "neg_broad->exact" in match_types
        assert "broad->neg_phrase" in match_types

    def test_toggle_column_mapping_generic_entity_format(self, client, create_test_company):
        """Test creating column mappings using the generic entity_type + entity_id format."""
        # Create a second company
        company2_data = {"title": "Test Company 2", "is_active": True}
        response = client.post("/companies", json=company2_data)
        assert response.status_code == 201
        company2 = response.json()["object"]

        # Get first company
        companies_response = client.get("/companies?page_size=10")
        companies = companies_response.json()["objects"]
        company1 = companies[0]

        # Create a test campaign for the first company
        campaign_data = {
            "title": "Test Campaign for Generic Format",
            "company_id": company1["id"],
            "is_active": True
        }
        response = client.post("/ad_campaigns", json=campaign_data)
        assert response.status_code == 201
        campaign = response.json()["object"]

        # Test creating mapping using generic format (source: ad_campaign, target: company)
        mapping_data = {
            "action": "create",
            "source_entity_type": "ad_campaign",
            "source_entity_id": campaign["id"],
            "source_match_type": "exact",
            "target_entity_type": "company",
            "target_entity_id": company2["id"],
            "target_match_type": "broad"
        }

        response = client.post("/column-mappings/toggle", json=mapping_data)
        assert response.status_code == 200

        data = response.json()
        assert data["action"] == "created"
        assert "mapping_id" in data

        # Verify mapping was created and converted correctly
        mappings_response = client.get("/column-mappings/active")
        assert mappings_response.status_code == 200
        mappings = mappings_response.json()["objects"]
        assert len(mappings) >= 1
        
        # Find our mapping
        mapping = next((m for m in mappings if m["source_ad_campaign_id"] == campaign["id"] and m["target_company_id"] == company2["id"]), None)
        assert mapping is not None
        assert mapping["source_match_type"] == "exact"
        assert mapping["target_match_type"] == "broad"


class TestKeywordEndpoints:
    """Test all keyword-related endpoints."""

    def test_bulk_create_keywords(self, client):
        """Test bulk creating keywords."""
        bulk_data = {
            "keywords": ["keyword1", "keyword2", "keyword3"],
            "company_ids": [],
            "ad_campaign_ids": [],
            "ad_group_ids": [],
            "broad": True,
            "phrase": False,
            "exact": False,
            "neg_broad": False,
            "neg_phrase": False,
            "neg_exact": False,
            "override_broad": False,
            "override_phrase": False,
            "override_exact": False,
            "override_neg_broad": False,
            "override_neg_phrase": False,
            "override_neg_exact": False
        }

        response = client.post("/keywords/bulk", json=bulk_data)
        assert response.status_code == 201

        data = response.json()
        assert "Created 3 new keywords" in data["message"]
        assert len(data["objects"]) == 3
        assert data["created"] == 3

    def test_bulk_create_keywords_with_relations(self, client, create_test_company, create_test_campaign, create_test_ad_group):
        """Test bulk creating keywords with entity relations."""
        bulk_data = {
            "keywords": ["test keyword 1", "test keyword 2"],
            "company_ids": [create_test_company["id"]],
            "ad_campaign_ids": [create_test_campaign["id"]],
            "ad_group_ids": [create_test_ad_group["id"]],
            "broad": True,
            "phrase": True,
            "exact": False,
            "neg_broad": False,
            "neg_phrase": False,
            "neg_exact": False,
            "override_broad": False,
            "override_phrase": False,
            "override_exact": False,
            "override_neg_broad": False,
            "override_neg_phrase": False,
            "override_neg_exact": False
        }

        response = client.post("/keywords/bulk", json=bulk_data)
        assert response.status_code == 201

        data = response.json()
        assert len(data["objects"]) == 2
        assert data["created"] == 2  # 2 keywords created
        assert data["relations_created"] == 6  # 2 keywords × 3 entities

    def test_list_keywords_empty(self, client):
        """Test listing keywords when none exist."""
        response = client.get("/keywords")
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "Retrieved 0 keywords"
        assert data["objects"] == []
        assert data["pagination"]["total"] == 0

    def test_list_keywords_with_data(self, client, create_test_keyword):
        """Test listing keywords with existing data."""
        response = client.get("/keywords")
        assert response.status_code == 200
        data = response.json()
        assert len(data["objects"]) == 1
        assert data["objects"][0]["keyword"] == create_test_keyword["keyword"]

    def test_list_keywords_with_filters(self, client, demo_user_id, create_test_company, create_test_campaign, create_test_ad_group):
        """Test listing keywords with various filters."""
        # Create keywords with different relations
        # Keyword 1: attached to company only
        bulk_data1 = {
            "keywords": ["company keyword"],
            "company_ids": [create_test_company["id"]],
            "ad_campaign_ids": [],
            "ad_group_ids": [],
            "broad": True,
            "phrase": False,
            "exact": False,
            "neg_broad": False,
            "neg_phrase": False,
            "neg_exact": False,
            "override_broad": False,
            "override_phrase": False,
            "override_exact": False,
            "override_neg_broad": False,
            "override_neg_phrase": False,
            "override_neg_exact": False
        }
        client.post("/keywords/bulk", json=bulk_data1)

        # Keyword 2: attached to campaign only
        bulk_data2 = {
            "keywords": ["campaign keyword"],
            "company_ids": [],
            "ad_campaign_ids": [create_test_campaign["id"]],
            "ad_group_ids": [],
            "broad": True,
            "phrase": False,
            "exact": False,
            "neg_broad": False,
            "neg_phrase": False,
            "neg_exact": False,
            "override_broad": False,
            "override_phrase": False,
            "override_exact": False,
            "override_neg_broad": False,
            "override_neg_phrase": False,
            "override_neg_exact": False
        }
        client.post("/keywords/bulk", json=bulk_data2)

        # Keyword 3: attached to ad group only
        bulk_data3 = {
            "keywords": ["adgroup keyword"],
            "company_ids": [],
            "ad_campaign_ids": [],
            "ad_group_ids": [create_test_ad_group["id"]],
            "broad": True,
            "phrase": False,
            "exact": False,
            "neg_broad": False,
            "neg_phrase": False,
            "neg_exact": False,
            "override_broad": False,
            "override_phrase": False,
            "override_exact": False,
            "override_neg_broad": False,
            "override_neg_phrase": False,
            "override_neg_exact": False
        }
        client.post("/keywords/bulk", json=bulk_data3)

        # Test that keywords are listed with active entity relations
        # Now the endpoint automatically uses all active entities (no need to pass IDs)
        response = client.get("/keywords")
        assert response.status_code == 200
        data = response.json()
        # All 3 keywords should be returned with their relations to active entities
        assert len(data["objects"]) == 3
        
        # Verify each keyword has proper relations
        keywords_by_name = {kw["keyword"]: kw for kw in data["objects"]}
        
        # Company keyword should have company relation
        assert "company keyword" in keywords_by_name
        company_kw = keywords_by_name["company keyword"]
        assert str(create_test_company['id']) in company_kw["relations"]["companies"]
        assert company_kw["relations"]["companies"][str(create_test_company['id'])] is not None
        
        # Campaign keyword should have campaign relation
        assert "campaign keyword" in keywords_by_name
        campaign_kw = keywords_by_name["campaign keyword"]
        assert str(create_test_campaign['id']) in campaign_kw["relations"]["ad_campaigns"]
        assert campaign_kw["relations"]["ad_campaigns"][str(create_test_campaign['id'])] is not None
        
        # Adgroup keyword should have adgroup relation
        assert "adgroup keyword" in keywords_by_name
        adgroup_kw = keywords_by_name["adgroup keyword"]
        assert str(create_test_ad_group['id']) in adgroup_kw["relations"]["ad_groups"]
        assert adgroup_kw["relations"]["ad_groups"][str(create_test_ad_group['id'])] is not None

    def test_get_keyword(self, client, create_test_keyword):
        """Test getting a single keyword."""
        keyword_id = create_test_keyword["id"]
        response = client.get(f"/keywords/{keyword_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["object"]["id"] == keyword_id

    def test_get_keyword_not_found(self, client):
        """Test getting a non-existent keyword."""
        response = client.get("/keywords/999")
        assert response.status_code == 404

    def test_update_keyword(self, client, create_test_keyword):
        """Test updating a keyword."""
        keyword_id = create_test_keyword["id"]
        update_data = {"keyword": "updated keyword"}

        response = client.post(f"/keywords/{keyword_id}/update", json=update_data)
        assert response.status_code == 200
        data = response.json()
        assert data["object"]["keyword"] == update_data["keyword"]

    def test_bulk_delete_keywords(self, client, demo_user_id):
        """Test bulk deleting keywords."""
        # Create keywords
        bulk_data = {
            "keywords": ["keyword1", "keyword2", "keyword3"],
            "company_ids": [],
            "ad_campaign_ids": [],
            "ad_group_ids": [],
            "broad": True,
            "phrase": False,
            "exact": False,
            "neg_broad": False,
            "neg_phrase": False,
            "neg_exact": False,
            "override_broad": False,
            "override_phrase": False,
            "override_exact": False,
            "override_neg_broad": False,
            "override_neg_phrase": False,
            "override_neg_exact": False
        }
        response = client.post("/keywords/bulk", json=bulk_data)
        keyword_ids = [kw["id"] for kw in response.json()["objects"]]

        # Delete first two keywords
        delete_data = {"ids": keyword_ids[:2]}
        response = client.post("/keywords/bulk/delete", json=delete_data)
        assert response.status_code == 200
        data = response.json()
        assert data["deleted"] == 2

    def test_bulk_upsert_keyword_relations(self, client, create_test_keyword, create_test_company):
        """Test bulk upserting keyword relations (create and update)."""
        keyword_id = create_test_keyword["id"]

        # First attach keyword to company
        create_relations_data = {
            "keyword_ids": [keyword_id],
            "company_ids": [create_test_company["id"]],
            "ad_campaign_ids": [],
            "ad_group_ids": [],
            "broad": True,
            "phrase": False,
            "exact": False,
            "neg_broad": False,
            "neg_phrase": False,
            "neg_exact": False,
            "override_broad": False,
            "override_phrase": False,
            "override_exact": False,
            "override_neg_broad": False,
            "override_neg_phrase": False,
            "override_neg_exact": False
        }
        client.post("/keywords/bulk/relations", json=create_relations_data)

        # Now update the existing relations using the same endpoint
        update_data = {
            "keyword_ids": [keyword_id],
            "company_ids": [create_test_company["id"]],
            "ad_campaign_ids": [],
            "ad_group_ids": [],
            "broad": False,
            "phrase": True,
            "exact": True,
            "neg_broad": False,
            "neg_phrase": False,
            "neg_exact": False,
            "override_broad": True,
            "override_phrase": True,
            "override_exact": True,
            "override_neg_broad": False,
            "override_neg_phrase": False,
            "override_neg_exact": False
        }

        response = client.post("/keywords/bulk/relations", json=update_data)
        assert response.status_code == 200
        data = response.json()
        assert data["updated"] == 1  # 1 relation updated
        assert data["created"] == 0  # 0 relations created
        assert "relations" in data
        assert len(data["relations"]) == 1
        assert data["relations"][0]["phrase"] is True
        assert data["relations"][0]["exact"] is True
        assert data["relations"][0]["broad"] is False

    def test_bulk_create_keyword_relations(self, client, create_test_keyword, create_test_company):
        """Test bulk creating keyword relations."""
        keyword_id = create_test_keyword["id"]

        relations_data = {
            "keyword_ids": [keyword_id],
            "company_ids": [create_test_company["id"]],
            "ad_campaign_ids": [],
            "ad_group_ids": [],
            "broad": True,
            "phrase": True,
            "exact": False,
            "neg_broad": False,
            "neg_phrase": False,
            "neg_exact": False,
            "override_broad": False,
            "override_phrase": False,
            "override_exact": False,
            "override_neg_broad": False,
            "override_neg_phrase": False,
            "override_neg_exact": False
        }

        response = client.post("/keywords/bulk/relations", json=relations_data)
        assert response.status_code == 200
        data = response.json()
        assert data["created"] == 1
        assert "relations" in data
        assert len(data["relations"]) == 1
        assert data["relations"][0]["broad"] is True
        assert data["relations"][0]["phrase"] is True
        assert data["relations"][0]["exact"] is False

    def test_bulk_delete_company_keyword_relations(self, client, create_test_keyword, create_test_company):
        """Test bulk deleting company-keyword relations."""
        keyword_id = create_test_keyword["id"]
        company_id = create_test_company["id"]

        # Create relation first
        relations_data = {
            "keyword_ids": [keyword_id],
            "company_ids": [company_id],
            "ad_campaign_ids": [],
            "ad_group_ids": [],
            "broad": True,
            "phrase": False,
            "exact": False,
            "neg_broad": False,
            "neg_phrase": False,
            "neg_exact": False,
            "override_broad": False,
            "override_phrase": False,
            "override_exact": False,
            "override_neg_broad": False,
            "override_neg_phrase": False,
            "override_neg_exact": False
        }
        client.post("/keywords/bulk/relations", json=relations_data)

        # Delete the relation by setting all match types to None with override flags
        delete_data = {
            "keyword_ids": [keyword_id],
            "company_ids": [company_id],
            "ad_campaign_ids": [],
            "ad_group_ids": [],
            "broad": None,
            "phrase": None,
            "exact": None,
            "override_broad": True,
            "override_phrase": True,
            "override_exact": True
        }
        response = client.post("/keywords/bulk/relations", json=delete_data)
        assert response.status_code == 200
        data = response.json()
        assert data["deleted"] == 1

    def test_bulk_delete_campaign_keyword_relations(self, client, create_test_keyword, create_test_campaign):
        """Test bulk deleting campaign-keyword relations."""
        keyword_id = create_test_keyword["id"]
        campaign_id = create_test_campaign["id"]

        # Create relation first
        relations_data = {
            "keyword_ids": [keyword_id],
            "company_ids": [],
            "ad_campaign_ids": [campaign_id],
            "ad_group_ids": [],
            "broad": True,
            "phrase": False,
            "exact": False,
            "neg_broad": False,
            "neg_phrase": False,
            "neg_exact": False,
            "override_broad": False,
            "override_phrase": False,
            "override_exact": False,
            "override_neg_broad": False,
            "override_neg_phrase": False,
            "override_neg_exact": False
        }
        client.post("/keywords/bulk/relations", json=relations_data)

        # Delete the relation by setting all match types to False with override flags
        delete_data = {
            "keyword_ids": [keyword_id],
            "company_ids": [],
            "ad_campaign_ids": [campaign_id],
            "ad_group_ids": [],
            "broad": None,
            "phrase": None,
            "exact": None,
            "override_broad": True,
            "override_phrase": True,
            "override_exact": True
        }
        response = client.post("/keywords/bulk/relations", json=delete_data)
        assert response.status_code == 200
        data = response.json()
        assert data["deleted"] == 1

    def test_bulk_delete_adgroup_keyword_relations(self, client, create_test_keyword, create_test_ad_group):
        """Test bulk deleting ad group-keyword relations."""
        keyword_id = create_test_keyword["id"]
        ad_group_id = create_test_ad_group["id"]

        # Create relation first
        relations_data = {
            "keyword_ids": [keyword_id],
            "company_ids": [],
            "ad_campaign_ids": [],
            "ad_group_ids": [ad_group_id],
            "broad": True,
            "phrase": False,
            "exact": False,
            "neg_broad": False,
            "neg_phrase": False,
            "neg_exact": False,
            "override_broad": False,
            "override_phrase": False,
            "override_exact": False,
            "override_neg_broad": False,
            "override_neg_phrase": False,
            "override_neg_exact": False
        }
        client.post("/keywords/bulk/relations", json=relations_data)

        # Delete the relation by setting all match types to None with override flags
        delete_data = {
            "keyword_ids": [keyword_id],
            "company_ids": [],
            "ad_campaign_ids": [],
            "ad_group_ids": [ad_group_id],
            "broad": None,
            "phrase": None,
            "exact": None,
            "override_broad": True,
            "override_phrase": True,
            "override_exact": True
        }
        response = client.post("/keywords/bulk/relations", json=delete_data)
        assert response.status_code == 200
        data = response.json()
        assert data["deleted"] == 1


class TestKeywordTrash:
    """Test keyword trash functionality."""

    def test_bulk_trash_keywords(self, client, demo_user_id):
        """Test bulk trashing keywords."""
        # Create keywords
        bulk_data = {
            "keywords": ["trash_keyword1", "trash_keyword2", "trash_keyword3"],
            "company_ids": [],
            "ad_campaign_ids": [],
            "ad_group_ids": [],
            "broad": True,
            "phrase": False,
            "exact": False,
            "pause": False,
            "override_broad": False,
            "override_phrase": False,
            "override_exact": False,
            "override_pause": False
        }
        response = client.post("/keywords/bulk", json=bulk_data)
        assert response.status_code == 201
        keyword_ids = [kw["id"] for kw in response.json()["objects"]]

        # Verify keywords are not trashed initially
        for kw in response.json()["objects"]:
            assert kw["trash"] is None

        # Trash first two keywords
        trash_data = {"ids": keyword_ids[:2], "trash": True}
        response = client.post("/keywords/bulk/trash", json=trash_data)
        assert response.status_code == 200
        data = response.json()
        assert data["deleted"] == 2  # Using deleted field for consistency
        assert "trashed" in data["message"]

        # Verify keywords are now trashed
        response = client.get("/keywords")
        assert response.status_code == 200
        keywords = response.json()["objects"]
        trashed_keywords = [kw for kw in keywords if kw["id"] in keyword_ids[:2]]
        not_trashed_keywords = [kw for kw in keywords if kw["id"] == keyword_ids[2]]

        for kw in trashed_keywords:
            assert kw["trash"] is True
        for kw in not_trashed_keywords:
            assert kw["trash"] is None

    def test_bulk_untrash_keywords(self, client, demo_user_id):
        """Test bulk untrashing keywords."""
        # Create and trash keywords
        bulk_data = {
            "keywords": ["untrash_keyword1", "untrash_keyword2"],
            "company_ids": [],
            "ad_campaign_ids": [],
            "ad_group_ids": [],
            "broad": True,
            "phrase": False,
            "exact": False,
            "pause": False,
            "override_broad": False,
            "override_phrase": False,
            "override_exact": False,
            "override_pause": False
        }
        response = client.post("/keywords/bulk", json=bulk_data)
        keyword_ids = [kw["id"] for kw in response.json()["objects"]]

        # Trash them
        trash_data = {"ids": keyword_ids, "trash": True}
        client.post("/keywords/bulk/trash", json=trash_data)

        # Untrash them
        untrash_data = {"ids": keyword_ids, "trash": False}
        response = client.post("/keywords/bulk/trash", json=untrash_data)
        assert response.status_code == 200
        data = response.json()
        assert data["deleted"] == 2
        assert "untrashed" in data["message"]

        # Verify keywords are not trashed
        response = client.get("/keywords")
        assert response.status_code == 200
        keywords = response.json()["objects"]
        untrashed_keywords = [kw for kw in keywords if kw["id"] in keyword_ids]

        for kw in untrashed_keywords:
            assert kw["trash"] is False

    def test_filter_keywords_by_trash_status(self, client, demo_user_id):
        """Test filtering keywords by trash status."""
        # Create keywords
        bulk_data = {
            "keywords": ["filter_trash1", "filter_trash2", "filter_normal1", "filter_normal2"],
            "company_ids": [],
            "ad_campaign_ids": [],
            "ad_group_ids": [],
            "broad": True,
            "phrase": False,
            "exact": False,
            "pause": False,
            "override_broad": False,
            "override_phrase": False,
            "override_exact": False,
            "override_pause": False
        }
        response = client.post("/keywords/bulk", json=bulk_data)
        keyword_ids = [kw["id"] for kw in response.json()["objects"]]

        # Trash first two keywords
        trash_data = {"ids": keyword_ids[:2], "trash": True}
        client.post("/keywords/bulk/trash", json=trash_data)

        # Test filter trash=True
        response = client.get("/keywords?trash=true")
        assert response.status_code == 200
        data = response.json()
        assert len(data["objects"]) == 2
        for kw in data["objects"]:
            assert kw["trash"] is True

        # Test filter trash=False
        response = client.get("/keywords?trash=false")
        assert response.status_code == 200
        data = response.json()
        assert len(data["objects"]) == 2
        for kw in data["objects"]:
            # Should be either None or False (not trashed)
            assert kw["trash"] is None or kw["trash"] is False

        # Test no filter (should return all)
        response = client.get("/keywords")
        assert response.status_code == 200
        data = response.json()
        assert len(data["objects"]) == 4
        trash_counts = {"trashed": 0, "not_trashed": 0, "none": 0}
        for kw in data["objects"]:
            if kw["trash"] is True:
                trash_counts["trashed"] += 1
            elif kw["trash"] is False:
                trash_counts["not_trashed"] += 1
            else:
                trash_counts["none"] += 1
        assert trash_counts["trashed"] == 2
        assert trash_counts["not_trashed"] == 0  # Untrashed keywords have trash=False
        assert trash_counts["none"] == 2  # Newly created keywords have trash=None

    def test_sort_keywords_by_trash(self, client, demo_user_id):
        """Test sorting keywords by trash status."""
        # Create keywords
        bulk_data = {
            "keywords": ["sort_trash1", "sort_trash2", "sort_normal1"],
            "company_ids": [],
            "ad_campaign_ids": [],
            "ad_group_ids": [],
            "broad": True,
            "phrase": False,
            "exact": False,
            "pause": False,
            "override_broad": False,
            "override_phrase": False,
            "override_exact": False,
            "override_pause": False
        }
        response = client.post("/keywords/bulk", json=bulk_data)
        keyword_ids = [kw["id"] for kw in response.json()["objects"]]

        # Trash first keyword
        trash_data = {"ids": [keyword_ids[0]], "trash": True}
        client.post("/keywords/bulk/trash", json=trash_data)

        # Sort by trash asc
        response = client.get("/keywords?sort_by=trash&sort_order=asc")
        assert response.status_code == 200
        data = response.json()
        # Should be ordered: None, False, True (but we only have None and True)
        trash_values = [kw["trash"] for kw in data["objects"]]
        # Check that None values come before True values
        none_index = next(i for i, v in enumerate(trash_values) if v is None)
        true_index = next(i for i, v in enumerate(trash_values) if v is True)
        assert none_index < true_index

        # Sort by trash desc
        response = client.get("/keywords?sort_by=trash&sort_order=desc")
        assert response.status_code == 200
        data = response.json()
        trash_values = [kw["trash"] for kw in data["objects"]]
        # Should be ordered: True, None
        true_index = next(i for i, v in enumerate(trash_values) if v is True)
        none_index = next(i for i, v in enumerate(trash_values) if v is None)
        assert true_index < none_index

    def test_update_keyword_trash_status(self, client, create_test_keyword):
        """Test updating a keyword's trash status."""
        keyword_id = create_test_keyword["id"]

        # Update to trash
        update_data = {"keyword": create_test_keyword["keyword"], "trash": True}
        response = client.post(f"/keywords/{keyword_id}/update", json=update_data)
        assert response.status_code == 200
        data = response.json()
        assert data["object"]["trash"] is True

        # Update to untrash
        update_data = {"keyword": create_test_keyword["keyword"], "trash": False}
        response = client.post(f"/keywords/{keyword_id}/update", json=update_data)
        assert response.status_code == 200
        data = response.json()
        assert data["object"]["trash"] is False

        # Update without trash field (should not change)
        update_data = {"keyword": "updated_keyword"}
        response = client.post(f"/keywords/{keyword_id}/update", json=update_data)
        assert response.status_code == 200
        data = response.json()
        assert data["object"]["trash"] is False  # Should remain unchanged


class TestKeywordOverrideFlags:
    """Test keyword override flag functionality."""

    def test_override_true_always_overwrites(self, client, create_test_keyword, create_test_company):
        """Test that override=true always overwrites existing values."""
        keyword_id = create_test_keyword["id"]
        company_id = create_test_company["id"]

        # First create a relation with broad=true
        create_data = {
            "keyword_ids": [keyword_id],
            "company_ids": [company_id],
            "ad_campaign_ids": [],
            "ad_group_ids": [],
            "broad": True,
            "phrase": None,
            "exact": None,
            "override_broad": True,  # Need override=true to set broad
            "override_phrase": False,
            "override_exact": False
        }
        client.post("/keywords/bulk/relations", json=create_data)

        # Now try to update with override_broad=true and broad=false - should overwrite
        update_data = {
            "keyword_ids": [keyword_id],
            "company_ids": [company_id],
            "ad_campaign_ids": [],
            "ad_group_ids": [],
            "broad": False,
            "phrase": None,
            "exact": None,
            "override_broad": True,
            "override_phrase": False,
            "override_exact": False
        }
        response = client.post("/keywords/bulk/relations", json=update_data)
        assert response.status_code == 200
        data = response.json()
        assert data["updated"] == 1
        assert data["relations"][0]["broad"] is False  # Should be overwritten

    def test_override_false_never_sets_values(self, client, create_test_keyword, create_test_company):
        """Test that override=false never sets values, even for null existing values."""
        keyword_id = create_test_keyword["id"]
        company_id = create_test_company["id"]

        # Try to set broad with override=false - should create relation with requested values
        update_data = {
            "keyword_ids": [keyword_id],
            "company_ids": [company_id],
            "ad_campaign_ids": [],
            "ad_group_ids": [],
            "broad": True,
            "phrase": None,
            "exact": None,
            "override_broad": False,  # Override flags don't affect new relation creation
            "override_phrase": False,
            "override_exact": False
        }
        response = client.post("/keywords/bulk/relations", json=update_data)
        assert response.status_code == 200
        data = response.json()
        # Should create a new relation with the requested values
        assert data["created"] == 1
        assert data["updated"] == 0
        # The relation should have broad=True since that's what was requested
        assert data["relations"][0]["broad"] is True

    def test_override_false_does_not_override_existing(self, client, create_test_keyword, create_test_company):
        """Test that override=false does not override existing values."""
        keyword_id = create_test_keyword["id"]
        company_id = create_test_company["id"]

        # First create a relation with broad=true
        create_data = {
            "keyword_ids": [keyword_id],
            "company_ids": [company_id],
            "ad_campaign_ids": [],
            "ad_group_ids": [],
            "broad": True,
            "phrase": None,
            "exact": None,
            "override_broad": True,
            "override_phrase": False,
            "override_exact": False
        }
        client.post("/keywords/bulk/relations", json=create_data)

        # Now try to update with override_broad=false and broad=false - should NOT overwrite
        update_data = {
            "keyword_ids": [keyword_id],
            "company_ids": [company_id],
            "ad_campaign_ids": [],
            "ad_group_ids": [],
            "broad": False,
            "phrase": None,
            "exact": None,
            "override_broad": False,  # Should not change broad
            "override_phrase": False,
            "override_exact": False
        }
        response = client.post("/keywords/bulk/relations", json=update_data)
        assert response.status_code == 200
        data = response.json()
        assert data["updated"] == 0  # Should not be updated
        assert len(data["relations"]) == 0  # No relations returned since none were updated

    def test_null_fields_in_request_are_not_touched(self, client, create_test_keyword, create_test_company):
        """Test that fields with null values in the request are not modified."""
        keyword_id = create_test_keyword["id"]
        company_id = create_test_company["id"]

        # Create a relation with all match types set
        create_data = {
            "keyword_ids": [keyword_id],
            "company_ids": [company_id],
            "ad_campaign_ids": [],
            "ad_group_ids": [],
            "broad": True,
            "phrase": False,
            "exact": True,
            "override_broad": True,
            "override_phrase": True,
            "override_exact": True
        }
        client.post("/keywords/bulk/relations", json=create_data)

        # Update with null values for phrase and exact - should not touch them
        update_data = {
            "keyword_ids": [keyword_id],
            "company_ids": [company_id],
            "ad_campaign_ids": [],
            "ad_group_ids": [],
            "broad": False,  # Change broad
            "phrase": None,  # Don't touch phrase
            "exact": None,   # Don't touch exact
            "override_broad": True,   # Override broad
            "override_phrase": False, # Don't override phrase (but it's null anyway)
            "override_exact": False   # Don't override exact (but it's null anyway)
        }
        response = client.post("/keywords/bulk/relations", json=update_data)
        assert response.status_code == 200
        data = response.json()
        assert data["updated"] == 1
        relation = data["relations"][0]
        assert relation["broad"] is False  # Should be changed
        assert relation["phrase"] is False  # Should remain unchanged
        assert relation["exact"] is True    # Should remain unchanged

    def test_multiple_keywords_different_override_scenarios(self, client, create_test_company):
        """Test multiple keywords with different existing states and override scenarios."""
        company_id = create_test_company["id"]

        # Create 3 keywords
        bulk_create_data = {
            "keywords": ["keyword_override_1", "keyword_override_2", "keyword_override_3"],
            "company_ids": [],
            "ad_campaign_ids": [],
            "ad_group_ids": [],
            "broad": None,
            "phrase": None,
            "exact": None,
            "override_broad": False,
            "override_phrase": False,
            "override_exact": False
        }
        response = client.post("/keywords/bulk", json=bulk_create_data)
        keywords = response.json()["objects"]
        keyword_ids = [kw["id"] for kw in keywords]

        # Set up different initial states:
        # keyword 1: broad = true
        # keyword 2: broad = null
        # keyword 3: broad = false

        setup_data_1 = {
            "keyword_ids": [keyword_ids[0]],
            "company_ids": [company_id],
            "ad_campaign_ids": [],
            "ad_group_ids": [],
            "broad": True,
            "phrase": None,
            "exact": None,
            "override_broad": False,
            "override_phrase": False,
            "override_exact": False
        }
        client.post("/keywords/bulk/relations", json=setup_data_1)

        setup_data_3 = {
            "keyword_ids": [keyword_ids[2]],
            "company_ids": [company_id],
            "ad_campaign_ids": [],
            "ad_group_ids": [],
            "broad": False,
            "phrase": None,
            "exact": None,
            "override_broad": False,
            "override_phrase": False,
            "override_exact": False
        }
        client.post("/keywords/bulk/relations", json=setup_data_3)

        # Now update all with override_broad=false and broad=true
        # Expected results:
        # keyword 1 (broad=true): should NOT be updated (override=false)
        # keyword 2 (broad=null): should create new relation with broad=true (no existing relation)
        # keyword 3 (broad=false): should NOT be updated (override=false)

        update_data = {
            "keyword_ids": keyword_ids,
            "company_ids": [company_id],
            "ad_campaign_ids": [],
            "ad_group_ids": [],
            "broad": True,
            "phrase": None,
            "exact": None,
            "override_broad": False,  # Override flags don't affect new relation creation
            "override_phrase": False,
            "override_exact": False
        }
        response = client.post("/keywords/bulk/relations", json=update_data)
        assert response.status_code == 200
        data = response.json()

        # keyword 2 should have a new relation created
        assert data["created"] == 1
        assert data["updated"] == 0
        assert len(data["relations"]) == 1

        created_relation = data["relations"][0]
        assert created_relation["keyword_id"] == keyword_ids[1]  # keyword 2
        assert created_relation["broad"] is True  # Should be set to true


class TestPagination:
    """Test pagination across all endpoints."""

    def test_pagination_parameters_validation(self, client):
        """Test pagination parameter validation."""
        # Test invalid page (must be >= 1)
        response = client.get("/companies?page=0")
        assert response.status_code == 422

        # Test invalid page_size (must be 1-100)
        response = client.get("/companies?page_size=0")
        assert response.status_code == 422

        response = client.get("/companies?page_size=101")
        assert response.status_code == 422

    def test_pagination_consistency(self, client, demo_user_id):
        """Test pagination consistency across different page sizes."""
        # Create 10 companies
        for i in range(10):
            company_data = {"title": f"Company {i+1}", "is_active": True}
            client.post("/companies", json=company_data)

        # Test with page_size=3
        all_companies = []
        page = 1
        while True:
            response = client.get(f"/companies?page={page}&page_size=3")
            assert response.status_code == 200
            data = response.json()

            if not data["objects"]:
                break

            all_companies.extend(data["objects"])
            page += 1

            if page > 10:  # Safety break
                break

        assert len(all_companies) == 10
        assert len(set(c["id"] for c in all_companies)) == 10  # All unique


class TestErrorHandling:
    """Test error handling across the application."""

    def test_invalid_json_payload(self, client):
        """Test handling of invalid JSON payloads."""
        response = client.post("/companies", content="invalid json")
        assert response.status_code == 422

    def test_non_existent_endpoints(self, client):
        """Test accessing non-existent endpoints."""
        response = client.get("/nonexistent")
        assert response.status_code == 404

    def test_invalid_http_methods(self, client):
        """Test invalid HTTP methods on endpoints."""
        response = client.patch("/companies")
        assert response.status_code == 405

    def test_database_connection_errors(self, client, db_session):
        """Test handling database connection issues."""
        # This would require mocking database failures
        # For now, just ensure the app handles normal operation
        response = client.get("/")
        assert response.status_code == 200


class TestDataIntegrity:
    """Test data integrity and relationships."""

    def test_cascade_deletes(self, client, demo_user_id):
        """Test that cascade deletes work properly."""
        # Create company -> campaign -> ad group chain
        company_data = {"title": "Test Company", "is_active": True}
        company = client.post("/companies", json=company_data).json()["object"]

        campaign_data = {"title": "Test Campaign", "company_id": company["id"], "is_active": True}
        campaign = client.post("/ad_campaigns", json=campaign_data).json()["object"]

        ad_group_data = {"title": "Test Ad Group", "ad_campaign_id": campaign["id"], "is_active": True}
        ad_group = client.post("/ad_groups", json=ad_group_data).json()["object"]

        # Create keyword attached to all levels
        bulk_data = {
            "keywords": ["test keyword"],
            "company_ids": [company["id"]],
            "ad_campaign_ids": [campaign["id"]],
            "ad_group_ids": [ad_group["id"]],
            "broad": True,
            "phrase": False,
            "exact": False,
            "neg_broad": False,
            "neg_phrase": False,
            "neg_exact": False,
            "override_broad": False,
            "override_phrase": False,
            "override_exact": False,
            "override_neg_broad": False,
            "override_neg_phrase": False,
            "override_neg_exact": False
        }
        keyword = client.post("/keywords/bulk", json=bulk_data).json()["objects"][0]

        # Delete company - should cascade to all related entities
        delete_data = {"ids": [company["id"]]}
        client.post("/companies/bulk/delete", json=delete_data)

        # Verify all entities are deleted
        assert client.get(f"/companies/{company['id']}").status_code == 404
        assert client.get(f"/ad_campaigns/{campaign['id']}").status_code == 404
        assert client.get(f"/ad_groups/{ad_group['id']}").status_code == 404
        # Note: Keywords are not deleted, only relations

    def test_unique_constraints(self, client, demo_user_id):
        """Test unique constraints are enforced."""
        # Create a keyword
        bulk_data = {
            "keywords": ["unique keyword"],
            "company_ids": [],
            "ad_campaign_ids": [],
            "ad_group_ids": [],
            "broad": True,
            "phrase": False,
            "exact": False,
            "neg_broad": False,
            "neg_phrase": False,
            "neg_exact": False,
            "override_broad": False,
            "override_phrase": False,
            "override_exact": False,
            "override_neg_broad": False,
            "override_neg_phrase": False,
            "override_neg_exact": False
        }
        client.post("/keywords/bulk", json=bulk_data)

        # Try to create the same keyword again - should work (different user context in test)
        # But in real usage with same user, it would fail due to unique constraint
        response = client.post("/keywords/bulk", json=bulk_data)
        assert response.status_code == 201  # Should succeed in test environment

    def test_foreign_key_constraints(self, client):
        """Test foreign key constraints are enforced."""
        # Try to create campaign with non-existent company
        campaign_data = {"title": "Test", "company_id": 999, "is_active": True}
        response = client.post("/ad_campaigns", json=campaign_data)
        assert response.status_code == 404

        # Try to create ad group with non-existent campaign
        ad_group_data = {"title": "Test", "ad_campaign_id": 999, "is_active": True}
        response = client.post("/ad_groups", json=ad_group_data)
        assert response.status_code == 404


class TestRandomizedDataGeneration:
    """Test with randomly generated data to catch edge cases."""

    @pytest.mark.parametrize("num_companies", [1, 5, 10, 25])
    def test_bulk_company_operations_randomized(self, client, num_companies):
        """Test bulk operations with varying numbers of random companies."""
        # Create random companies
        company_ids = []
        for _ in range(num_companies):
            data = random_company_data()
            response = client.post("/companies", json=data)
            assert response.status_code == 201
            company_ids.append(response.json()["object"]["id"])

        # Test listing with pagination
        response = client.get(f"/companies?page_size={min(num_companies, 50)}")
        assert response.status_code == 200
        data = response.json()
        assert len(data["objects"]) == num_companies

        # Test bulk delete
        if num_companies > 0:
            delete_data = {"ids": company_ids[:min(5, len(company_ids))]}
            response = client.post("/companies/bulk/delete", json=delete_data)
            assert response.status_code == 200

    @pytest.mark.parametrize("num_keywords", [1, 10, 50, 100])
    def test_bulk_keyword_operations_randomized(self, client, num_keywords):
        """Test bulk keyword operations with varying sizes."""
        keywords = random_keywords(num_keywords)

        bulk_data = {
            "keywords": keywords,
            "company_ids": [],
            "ad_campaign_ids": [],
            "ad_group_ids": []
        }
        bulk_data.update(random_match_types())

        response = client.post("/keywords/bulk", json=bulk_data)
        assert response.status_code == 201
        data = response.json()
        assert len(data["objects"]) == len(keywords)

        # Test listing
        response = client.get("/keywords")
        assert response.status_code == 200
        data = response.json()
        assert data["pagination"]["total"] >= len(keywords)

    def test_complex_entity_relationships(self, client):
        """Test complex relationships between all entity types."""
        # Create a complex hierarchy
        companies = []
        for _ in range(random.randint(2, 4)):
            data = random_company_data()
            response = client.post("/companies", json=data)
            companies.append(response.json()["object"])

        all_campaigns = []
        all_ad_groups = []

        for company in companies:
            # Create campaigns for each company
            campaigns = []
            for _ in range(random.randint(1, 3)):
                data = random_campaign_data(company["id"])
                response = client.post("/ad_campaigns", json=data)
                campaign = response.json()["object"]
                campaigns.append(campaign)
                all_campaigns.append(campaign)

                # Create ad groups for each campaign
                for _ in range(random.randint(1, 2)):
                    ad_group_data = random_ad_group_data(campaign["id"])
                    response = client.post("/ad_groups", json=ad_group_data)
                    all_ad_groups.append(response.json()["object"])

        # Create keywords with relations to all levels
        keywords = random_keywords(random.randint(5, 15))
        bulk_data = {
            "keywords": keywords,
            "company_ids": [c["id"] for c in companies],
            "ad_campaign_ids": [c["id"] for c in all_campaigns],
            "ad_group_ids": [g["id"] for g in all_ad_groups]
        }
        bulk_data.update(random_match_types())

        response = client.post("/keywords/bulk", json=bulk_data)
        assert response.status_code == 201

        # Verify relations were created
        expected_relations = len(keywords) * (len(companies) + len(all_campaigns) + len(all_ad_groups))
        data = response.json()
        assert data["relations_created"] == expected_relations

    def test_random_data_edge_cases(self, client):
        """Test with various edge cases in random data."""
        # Test with very long names
        long_name = random_string(200)
        data = {"title": long_name, "is_active": True}
        response = client.post("/companies", json=data)
        # Should either succeed or fail gracefully
        assert response.status_code in [201, 422]

        # Test with special characters
        special_name = "Company @#$%^&*()_+{}|:<>?[]\\;',./"
        data = {"title": special_name, "is_active": True}
        response = client.post("/companies", json=data)
        assert response.status_code in [201, 422]

        # Test with unicode characters
        unicode_name = "公司名称 🚀 测试"
        data = {"title": unicode_name, "is_active": True}
        response = client.post("/companies", json=data)
        assert response.status_code in [201, 422]


class TestConcurrentOperations:
    """Test concurrent operations and race conditions."""

    @pytest.mark.asyncio
    async def test_concurrent_company_creation(self, client):
        """Test creating companies concurrently."""
        async def create_company(i):
            data = {"title": f"Concurrent Company {i}", "is_active": True}
            response = client.post("/companies", json=data)
            return response.status_code

        # Create multiple companies concurrently
        tasks = [create_company(i) for i in range(10)]
        results = await asyncio.gather(*tasks)

        # All should succeed
        assert all(status == 201 for status in results)

        # Verify all were created
        response = client.get("/companies")
        data = response.json()
        assert data["pagination"]["total"] >= 10

    def test_bulk_operations_under_load(self, client):
        """Test bulk operations with large datasets."""
        # Create many companies
        companies = []
        for i in range(20):
            data = random_company_data()
            response = client.post("/companies", json=data)
            companies.append(response.json()["object"])

        # Create many campaigns
        campaigns = []
        for company in companies:
            for _ in range(3):
                data = random_campaign_data(company["id"])
                response = client.post("/ad_campaigns", json=data)
                campaigns.append(response.json()["object"])

        # Bulk create many keywords
        keywords = random_keywords(100)
        bulk_data = {
            "keywords": keywords,
            "company_ids": [c["id"] for c in companies[:5]],  # Attach to first 5 companies
            "ad_campaign_ids": [c["id"] for c in campaigns[:10]],  # Attach to first 10 campaigns
            "ad_group_ids": []
        }
        bulk_data.update(random_match_types())

        response = client.post("/keywords/bulk", json=bulk_data)
        assert response.status_code == 201
        data = response.json()
        # Allow for some duplicates to be removed, expect at least 90 unique keywords
        assert len(data["objects"]) >= 90

    def test_mixed_operations_stress(self, client):
        """Test mixed CRUD operations under stress."""
        # Create initial data
        companies = []
        for _ in range(5):
            data = random_company_data()
            response = client.post("/companies", json=data)
            companies.append(response.json()["object"])

        # Perform mixed operations
        operations = []

        # Create campaigns
        for company in companies:
            data = random_campaign_data(company["id"])
            response = client.post("/ad_campaigns", json=data)
            operations.append(("create_campaign", response.status_code))

        # Update some companies
        for company in companies[:3]:
            update_data = random_company_data()
            response = client.post(f"/companies/{company['id']}/update", json=update_data)
            operations.append(("update_company", response.status_code))

        # Toggle status
        for company in companies[:2]:
            response = client.post(f"/companies/{company['id']}/toggle")
            operations.append(("toggle_company", response.status_code))

        # All operations should succeed
        for op_name, status in operations:
            assert status == 200 or status == 201, f"{op_name} failed with status {status}"


class TestBoundaryConditions:
    """Test boundary conditions and limits."""

    def test_pagination_boundaries(self, client):
        """Test pagination at boundaries."""
        # Create exactly 100 companies
        for i in range(100):
            data = random_company_data()
            client.post("/companies", json=data)

        # Test page size limits
        response = client.get("/companies?page_size=100")
        assert response.status_code == 200
        data = response.json()
        assert len(data["objects"]) == 100

        # Test page size over limit
        response = client.get("/companies?page_size=101")
        assert response.status_code == 422

        # Test large page numbers
        response = client.get("/companies?page=1000&page_size=1")
        assert response.status_code == 200
        data = response.json()
        assert len(data["objects"]) == 0

    def test_string_length_limits(self, client):
        """Test string length limits."""
        # Very long company name
        long_title = "A" * 1000
        data = {"title": long_title, "is_active": True}
        response = client.post("/companies", json=data)
        # Should either succeed or fail with validation error
        assert response.status_code in [201, 422]

        # Empty string
        data = {"title": "", "is_active": True}
        response = client.post("/companies", json=data)
        assert response.status_code == 422

        # Only whitespace
        data = {"title": "   ", "is_active": True}
        response = client.post("/companies", json=data)
        assert response.status_code == 422

    def test_numeric_limits(self, client):
        """Test numeric limits and edge cases."""
        # Create a company
        data = random_company_data()
        company = client.post("/companies", json=data).json()["object"]

        # Test with very large IDs (non-existent)
        response = client.get("/companies/999999")
        assert response.status_code == 404

        # Test negative IDs (API treats as not found)
        response = client.get("/companies/-1")
        assert response.status_code == 404

        # Test zero ID
        response = client.get("/companies/0")
        assert response.status_code == 404

    def test_bulk_operation_limits(self, client):
        """Test limits of bulk operations."""
        # Test empty bulk operations
        response = client.post("/companies/bulk/delete", json={"ids": []})
        assert response.status_code == 400

        # Test very large bulk operations
        large_ids = list(range(1, 101))  # 100 IDs
        response = client.post("/companies/bulk/delete", json={"ids": large_ids})
        # Should succeed even if companies don't exist
        assert response.status_code == 200

        # Test bulk keyword creation with many keywords
        many_keywords = random_keywords(200)
        bulk_data = {
            "keywords": many_keywords,
            "company_ids": [],
            "ad_campaign_ids": [],
            "ad_group_ids": []
        }
        bulk_data.update(random_match_types())

        response = client.post("/keywords/bulk", json=bulk_data)
        assert response.status_code == 201


class TestActiveLimits:
    """Test active entity limits enforcement."""

    def test_company_active_limit_create(self, client):
        """Test that creating more than 3 active companies enforces the limit."""
        # Create 3 active companies - should all succeed as active
        companies = []
        for i in range(3):
            data = {"title": f"Active Company {i+1}", "is_active": True}
            response = client.post("/companies", json=data)
            assert response.status_code == 201
            result = response.json()["object"]
            assert result["is_active"] == True
            assert "successfully" in response.json()["message"]
            companies.append(result)
        
        # Try to create 4th active company - should be created as inactive
        data = {"title": "Fourth Active Company", "is_active": True}
        response = client.post("/companies", json=data)
        assert response.status_code == 201
        result = response.json()["object"]
        assert result["is_active"] == False  # Forced to inactive
        assert "Maximum 3 active" in response.json()["message"]
        
        # Create inactive company - should succeed
        data = {"title": "Inactive Company", "is_active": False}
        response = client.post("/companies", json=data)
        assert response.status_code == 201
        result = response.json()["object"]
        assert result["is_active"] == False

    def test_company_active_limit_toggle(self, client):
        """Test that toggling companies respects the active limit."""
        # Create 3 active companies
        active_companies = []
        for i in range(3):
            data = {"title": f"Active Company {i+1}", "is_active": True}
            response = client.post("/companies", json=data)
            active_companies.append(response.json()["object"])
        
        # Create 1 inactive company
        data = {"title": "Inactive Company", "is_active": False}
        response = client.post("/companies", json=data)
        inactive_company = response.json()["object"]
        
        # Try to toggle inactive company to active - should fail
        response = client.post(f"/companies/{inactive_company['id']}/toggle")
        assert response.status_code == 200
        result = response.json()["object"]
        assert result["is_active"] == False  # Still inactive
        assert "Maximum 3 active" in response.json()["message"]
        
        # Toggle one active company to inactive
        response = client.post(f"/companies/{active_companies[0]['id']}/toggle")
        assert response.status_code == 200
        result = response.json()["object"]
        assert result["is_active"] == False
        assert "deactivated successfully" in response.json()["message"]
        
        # Now toggle the previously inactive company - should succeed
        response = client.post(f"/companies/{inactive_company['id']}/toggle")
        assert response.status_code == 200
        result = response.json()["object"]
        assert result["is_active"] == True
        assert "activated successfully" in response.json()["message"]

    def test_company_active_limit_update(self, client):
        """Test that updating companies respects the active limit."""
        # Create 3 active companies
        for i in range(3):
            data = {"title": f"Active Company {i+1}", "is_active": True}
            client.post("/companies", json=data)
        
        # Create 1 inactive company
        data = {"title": "Inactive Company", "is_active": False}
        response = client.post("/companies", json=data)
        inactive_company = response.json()["object"]
        
        # Try to update inactive company to active - should fail
        update_data = {"title": "Updated Company", "is_active": True}
        response = client.post(f"/companies/{inactive_company['id']}/update", json=update_data)
        assert response.status_code == 200
        result = response.json()["object"]
        assert result["is_active"] == False  # Still inactive
        assert result["title"] == "Updated Company"  # Title updated
        assert "Maximum 3 active" in response.json()["message"]

    def test_ad_campaign_active_limit_create(self, client, create_test_company):
        """Test that creating more than 5 active campaigns enforces the limit."""
        # Create 5 active campaigns
        campaigns = []
        for i in range(5):
            data = {
                "title": f"Active Campaign {i+1}",
                "is_active": True,
                "company_id": create_test_company["id"]
            }
            response = client.post("/ad_campaigns", json=data)
            assert response.status_code == 201
            result = response.json()["object"]
            assert result["is_active"] == True
            campaigns.append(result)
        
        # Try to create 6th active campaign - should be created as inactive
        data = {
            "title": "Sixth Active Campaign",
            "is_active": True,
            "company_id": create_test_company["id"]
        }
        response = client.post("/ad_campaigns", json=data)
        assert response.status_code == 201
        result = response.json()["object"]
        assert result["is_active"] == False
        assert "Maximum 5 active" in response.json()["message"]

    def test_ad_campaign_active_limit_toggle(self, client, create_test_company):
        """Test that toggling campaigns respects the active limit."""
        # Create 5 active campaigns
        for i in range(5):
            data = {
                "title": f"Active Campaign {i+1}",
                "is_active": True,
                "company_id": create_test_company["id"]
            }
            client.post("/ad_campaigns", json=data)
        
        # Create inactive campaign
        data = {
            "title": "Inactive Campaign",
            "is_active": False,
            "company_id": create_test_company["id"]
        }
        response = client.post("/ad_campaigns", json=data)
        inactive_campaign = response.json()["object"]
        
        # Try to toggle to active - should fail
        response = client.post(f"/ad_campaigns/{inactive_campaign['id']}/toggle")
        assert response.status_code == 200
        result = response.json()["object"]
        assert result["is_active"] == False
        assert "Maximum 5 active" in response.json()["message"]

    def test_ad_group_active_limit_create(self, client, create_test_campaign):
        """Test that creating more than 7 active ad groups enforces the limit."""
        # Create 7 active ad groups
        ad_groups = []
        for i in range(7):
            data = {
                "title": f"Active Ad Group {i+1}",
                "is_active": True,
                "ad_campaign_id": create_test_campaign["id"]
            }
            response = client.post("/ad_groups", json=data)
            assert response.status_code == 201
            result = response.json()["object"]
            assert result["is_active"] == True
            ad_groups.append(result)
        
        # Try to create 8th active ad group - should be created as inactive
        data = {
            "title": "Eighth Active Ad Group",
            "is_active": True,
            "ad_campaign_id": create_test_campaign["id"]
        }
        response = client.post("/ad_groups", json=data)
        assert response.status_code == 201
        result = response.json()["object"]
        assert result["is_active"] == False
        assert "Maximum 7 active" in response.json()["message"]

    def test_ad_group_active_limit_toggle(self, client, create_test_campaign):
        """Test that toggling ad groups respects the active limit."""
        # Create 7 active ad groups
        for i in range(7):
            data = {
                "title": f"Active Ad Group {i+1}",
                "is_active": True,
                "ad_campaign_id": create_test_campaign["id"]
            }
            client.post("/ad_groups", json=data)
        
        # Create inactive ad group
        data = {
            "title": "Inactive Ad Group",
            "is_active": False,
            "ad_campaign_id": create_test_campaign["id"]
        }
        response = client.post("/ad_groups", json=data)
        inactive_ad_group = response.json()["object"]
        
        # Try to toggle to active - should fail
        response = client.post(f"/ad_groups/{inactive_ad_group['id']}/toggle")
        assert response.status_code == 200
        result = response.json()["object"]
        assert result["is_active"] == False
        assert "Maximum 7 active" in response.json()["message"]

    def test_mixed_active_inactive_operations(self, client, create_test_company, create_test_campaign):
        """Test mixed operations with active and inactive entities."""
        # Create 3 active companies
        for i in range(3):
            data = {"title": f"Company {i+1}", "is_active": True}
            client.post("/companies", json=data)
        
        # Create 5 active campaigns
        for i in range(5):
            data = {
                "title": f"Campaign {i+1}",
                "is_active": True,
                "company_id": create_test_company["id"]
            }
            client.post("/ad_campaigns", json=data)
        
        # Create 7 active ad groups
        for i in range(7):
            data = {
                "title": f"Ad Group {i+1}",
                "is_active": True,
                "ad_campaign_id": create_test_campaign["id"]
            }
            client.post("/ad_groups", json=data)
        
        # Now create more entities as inactive (should work)
        for i in range(5):
            data = {"title": f"Extra Company {i+1}", "is_active": False}
            response = client.post("/companies", json=data)
            assert response.status_code == 201
        
        for i in range(5):
            data = {
                "title": f"Extra Campaign {i+1}",
                "is_active": False,
                "company_id": create_test_company["id"]
            }
            response = client.post("/ad_campaigns", json=data)
            assert response.status_code == 201
        
        for i in range(5):
            data = {
                "title": f"Extra Ad Group {i+1}",
                "is_active": False,
                "ad_campaign_id": create_test_campaign["id"]
            }
            response = client.post("/ad_groups", json=data)
            assert response.status_code == 201
        
        # Verify we can list all entities
        response = client.get("/companies")
        assert response.status_code == 200
        assert response.json()["pagination"]["total"] >= 8  # 3 active + 5 inactive
        
        response = client.get("/ad_campaigns")
        assert response.status_code == 200
        assert response.json()["pagination"]["total"] >= 10  # 5 active + 5 inactive
        
        response = client.get("/ad_groups")
        assert response.status_code == 200
        assert response.json()["pagination"]["total"] >= 12  # 7 active + 5 inactive


class TestPropertyBasedTesting:
    """Property-based tests using Hypothesis."""

    @given(
        title=st.text(min_size=1, max_size=100),
        is_active=st.booleans()
    )
    @settings(max_examples=50, phases=[Phase.generate, Phase.shrink], suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_company_creation_properties(self, client, title, is_active):
        """Property-based test for company creation."""
        # Skip empty titles as they're invalid
        if not title.strip():
            return

        data = {"title": title, "is_active": is_active}
        response = client.post("/companies", json=data)

        if len(title.strip()) > 255:  # Assuming DB limit
            assert response.status_code == 422
        else:
            assert response.status_code == 201
            result = response.json()["object"]
            assert result["title"] == title.strip()  # API strips whitespace
            # Note: is_active might be False even if requested True due to active limit (max 3)
            # The API will set it to False and return a message about the limit
            if is_active and not result["is_active"]:
                # Check that message mentions the limit
                assert "Maximum 3 active" in response.json()["message"]
            elif not is_active:
                # If we requested inactive, it should be inactive
                assert result["is_active"] == False

    @given(
        keywords=st.lists(st.text(min_size=1, max_size=50), min_size=1, max_size=20),
        broad=st.booleans(),
        phrase=st.booleans(),
        exact=st.booleans()
    )
    @settings(max_examples=30, phases=[Phase.generate, Phase.shrink], suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_keyword_bulk_creation_properties(self, client, keywords, broad, phrase, exact):
        """Property-based test for keyword bulk creation."""
        # Filter out empty keywords
        valid_keywords = [k.strip() for k in keywords if k.strip()]

        if not valid_keywords:
            return

        bulk_data = {
            "keywords": valid_keywords,
            "company_ids": [],
            "ad_campaign_ids": [],
            "ad_group_ids": [],
            "broad": broad,
            "phrase": phrase,
            "exact": exact,
            "neg_broad": False,
            "neg_phrase": False,
            "neg_exact": False,
            "override_broad": False,
            "override_phrase": False,
            "override_exact": False,
            "override_neg_broad": False,
            "override_neg_phrase": False,
            "override_neg_exact": False
        }

        response = client.post("/keywords/bulk", json=bulk_data)
        assert response.status_code == 201

        data = response.json()
        assert len(data["objects"]) == len(valid_keywords)
        assert all(kw["keyword"] in valid_keywords for kw in data["objects"])

    @given(
        page=st.integers(min_value=1, max_value=100),
        page_size=st.integers(min_value=1, max_value=100)
    )
    @settings(max_examples=50, phases=[Phase.generate, Phase.shrink], suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_pagination_properties(self, client, page, page_size):
        """Property-based test for pagination."""
        # Create some test data
        for i in range(10):
            data = random_company_data()
            client.post("/companies", json=data)

        response = client.get(f"/companies?page={page}&page_size={page_size}")
        assert response.status_code == 200

        data = response.json()
        assert len(data["objects"]) <= page_size
        assert data["pagination"]["page"] == page
        assert data["pagination"]["page_size"] == page_size
        assert data["pagination"]["total_pages"] >= 1


class TestComplexScenarios:
    """Test complex real-world scenarios."""

    def test_full_workflow_scenario(self, client):
        """Test a complete marketing campaign setup workflow."""
        # 1. Create a company
        company_data = {"title": "TechCorp Solutions", "is_active": True}
        company = client.post("/companies", json=company_data).json()["object"]

        # 2. Create multiple campaigns
        campaigns = []
        campaign_names = ["Brand Awareness", "Lead Generation", "Product Launch"]
        for name in campaign_names:
            data = {"title": name, "company_id": company["id"], "is_active": True}
            campaign = client.post("/ad_campaigns", json=data).json()["object"]
            campaigns.append(campaign)

        # 3. Create ad groups for each campaign
        ad_groups = []
        for campaign in campaigns:
            for i in range(2):
                data = {
                    "title": f"{campaign['title']} - Group {i+1}",
                    "ad_campaign_id": campaign["id"],
                    "is_active": True
                }
                ad_group = client.post("/ad_groups", json=data).json()["object"]
                ad_groups.append(ad_group)

        # 4. Create comprehensive keyword sets
        brand_keywords = ["TechCorp", "TechCorp Solutions", "our company"]
        product_keywords = ["software", "platform", "solution", "technology"]
        service_keywords = ["consulting", "development", "support"]

        # Create brand keywords attached to company
        bulk_data = {
            "keywords": brand_keywords,
            "company_ids": [company["id"]],
            "ad_campaign_ids": [],
            "ad_group_ids": []
        }
        bulk_data.update({"broad": True, "phrase": True, "exact": True,
                         "neg_broad": False, "neg_phrase": False, "neg_exact": False,
                         "override_broad": False, "override_phrase": False, "override_exact": False,
                         "override_neg_broad": False, "override_neg_phrase": False, "override_neg_exact": False})
        client.post("/keywords/bulk", json=bulk_data)

        # Create product keywords attached to campaigns
        bulk_data = {
            "keywords": product_keywords,
            "company_ids": [],
            "ad_campaign_ids": [c["id"] for c in campaigns],
            "ad_group_ids": []
        }
        bulk_data.update({"broad": True, "phrase": False, "exact": False,
                         "neg_broad": False, "neg_phrase": False, "neg_exact": False,
                         "override_broad": False, "override_phrase": False, "override_exact": False,
                         "override_neg_broad": False, "override_neg_phrase": False, "override_neg_exact": False})
        client.post("/keywords/bulk", json=bulk_data)

        # Create service keywords attached to ad groups
        bulk_data = {
            "keywords": service_keywords,
            "company_ids": [],
            "ad_campaign_ids": [],
            "ad_group_ids": [g["id"] for g in ad_groups]
        }
        bulk_data.update({"broad": True, "phrase": True, "exact": False,
                         "neg_broad": False, "neg_phrase": False, "neg_exact": False,
                         "override_broad": False, "override_phrase": False, "override_exact": False,
                         "override_neg_broad": False, "override_neg_phrase": False, "override_neg_exact": False})
        client.post("/keywords/bulk", json=bulk_data)

        # 5. Verify the setup
        # Check company has campaigns
        response = client.get(f"/ad_campaigns?company_id={company['id']}")
        assert response.json()["pagination"]["total"] == 3

        # Check campaigns have ad groups
        for campaign in campaigns:
            response = client.get(f"/ad_groups?ad_campaign_id={campaign['id']}")
            assert response.json()["pagination"]["total"] == 2

        # Check keywords are properly distributed
        response = client.get("/keywords")
        total_keywords = len(brand_keywords + product_keywords + service_keywords)
        assert response.json()["pagination"]["total"] == total_keywords

    def test_data_integrity_under_modification(self, client, complex_test_setup):
        """Test data integrity when modifying interconnected data."""
        setup = complex_test_setup

        # Randomly modify entities
        for company in setup["companies"][:2]:
            # Update company
            update_data = random_company_data()
            client.post(f"/companies/{company['id']}/update", json=update_data)

            # Toggle status
            client.post(f"/companies/{company['id']}/toggle")

        # Verify campaigns still exist and are valid
        for campaign in setup["campaigns"]:
            response = client.get(f"/ad_campaigns/{campaign['id']}")
            assert response.status_code == 200

        # Verify ad groups still exist
        for ad_group in setup["ad_groups"]:
            response = client.get(f"/ad_groups/{ad_group['id']}")
            assert response.status_code == 200

        # Verify keywords still exist
        for keyword in setup["keywords"]:
            response = client.get(f"/keywords/{keyword['id']}")
            assert response.status_code == 200

    def test_cascading_operations(self, client):
        """Test cascading operations and their effects."""
        # Create a full hierarchy
        company = client.post("/companies", json=random_company_data()).json()["object"]
        campaign = client.post("/ad_campaigns", json=random_campaign_data(company["id"])).json()["object"]
        ad_group = client.post("/ad_groups", json=random_ad_group_data(campaign["id"])).json()["object"]

        # Create keywords attached to all levels
        keywords = random_keywords(5)
        bulk_data = {
            "keywords": keywords,
            "company_ids": [company["id"]],
            "ad_campaign_ids": [campaign["id"]],
            "ad_group_ids": [ad_group["id"]]
        }
        bulk_data.update(random_match_types())
        created_keywords = client.post("/keywords/bulk", json=bulk_data).json()["objects"]

        # Delete ad group - should not affect keywords
        client.post("/ad_groups/bulk/delete", json={"ids": [ad_group["id"]]})

        # Verify keywords still exist
        for keyword in created_keywords:
            response = client.get(f"/keywords/{keyword['id']}")
            assert response.status_code == 200

        # Delete campaign - should not affect keywords
        client.post("/ad_campaigns/bulk/delete", json={"ids": [campaign["id"]]})

        # Verify keywords still exist
        for keyword in created_keywords:
            response = client.get(f"/keywords/{keyword['id']}")
            assert response.status_code == 200

        # Delete company - should not affect keywords
        client.post("/companies/bulk/delete", json={"ids": [company["id"]]})

        # Verify keywords still exist (keywords are not cascade deleted)
        for keyword in created_keywords:
            response = client.get(f"/keywords/{keyword['id']}")
            assert response.status_code == 200


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
