"""
Comprehensive API Tests for KPlanner
Tests all endpoints with bulk data operations
"""
import requests
import json
from typing import List, Dict, Any

# API Configuration
BASE_URL = "http://localhost:8000"
DEV_MODE = True  # Set to True if DEV_MODE is enabled

class APITester:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.session = requests.Session()
        # Store created IDs for testing
        self.company_ids: List[int] = []
        self.campaign_ids: List[int] = []
        self.ad_group_ids: List[int] = []
        self.keyword_ids: List[int] = []
        self.filter_ids: List[int] = []
        
    def print_result(self, test_name: str, response: requests.Response):
        """Print test results"""
        status_icon = "✅" if response.status_code < 400 else "❌"
        print(f"{status_icon} {test_name}")
        print(f"   Status: {response.status_code}")
        if response.status_code < 400:
            try:
                data = response.json()
                if isinstance(data, dict):
                    # Print key metrics using generic field names
                    for key in ['id', 'status', 'message', 'created', 'existing', 'total',
                                'deleted', 'requested', 'updated', 'processed',
                                'relations_added', 'relations_updated']:
                        if key in data:
                            print(f"   {key}: {data[key]}")
                    # For SingleObjectResponse with nested object
                    if 'object' in data and isinstance(data['object'], dict):
                        if 'id' in data['object']:
                            print(f"   object.id: {data['object']['id']}")
                    # For MultipleObjectsResponse with objects array
                    if 'objects' in data and isinstance(data['objects'], list):
                        print(f"   objects count: {len(data['objects'])}")
                elif isinstance(data, list):
                    print(f"   Count: {len(data)}")
            except:
                pass
        else:
            print(f"   Error: {response.text[:200]}")
        print()

    # ==================== Company Tests ====================
    
    def test_create_companies(self, count: int = 10):
        """Create multiple companies"""
        print(f"\n{'='*50}")
        print("TESTING COMPANIES")
        print(f"{'='*50}\n")
        
        for i in range(1, count + 1):
            response = self.session.post(
                f"{self.base_url}/companies",
                json={"title": f"Test Company {i}"}
            )
            self.print_result(f"Create Company {i}", response)
            if response.status_code == 201:
                data = response.json()
                # Extract ID from SingleObjectResponse
                entity_id = data.get("id") or data.get("object", {}).get("id")
                if entity_id:
                    self.company_ids.append(entity_id)
    
    def test_list_companies(self):
        """List all companies with pagination"""
        # Test first page
        response = self.session.get(f"{self.base_url}/companies?page=1&page_size=100")
        self.print_result("List All Companies", response)
        
        if response.status_code == 200:
            data = response.json()
            total = data.get('total', 0)
            total_pages = data.get('total_pages', 1)
            
            # If there are multiple pages, test pagination
            if total_pages > 1:
                print(f"   📄 Multiple pages detected ({total_pages} pages, {total} total items)")
                # Fetch all pages to verify pagination works
                all_companies = []
                for page in range(1, min(total_pages + 1, 6)):  # Test up to 5 pages
                    page_response = self.session.get(
                        f"{self.base_url}/companies?page={page}&page_size=20"
                    )
                    if page_response.status_code == 200:
                        page_data = page_response.json()
                        all_companies.extend(page_data.get('objects', []))
                        print(f"   ✅ Fetched page {page}/{total_pages}: {len(page_data.get('objects', []))} items")
                
                print(f"   📊 Total items fetched across pages: {len(all_companies)}")
        
        return response
    
    def test_get_company(self):
        """Get specific company"""
        if self.company_ids:
            response = self.session.get(f"{self.base_url}/companies/{self.company_ids[0]}")
            self.print_result(f"Get Company {self.company_ids[0]}", response)
    
    def test_update_company(self):
        """Update a company"""
        if self.company_ids:
            response = self.session.post(
                f"{self.base_url}/companies/{self.company_ids[0]}/update",
                json={"title": "Updated Company Name"}
            )
            self.print_result(f"Update Company {self.company_ids[0]}", response)
    
    def test_bulk_delete_companies(self):
        """Bulk delete companies (will do at end)"""
        if len(self.company_ids) > 5:
            ids_to_delete = self.company_ids[-3:]  # Delete last 3
            response = self.session.post(
                f"{self.base_url}/companies/bulk/delete",
                json={"ids": ids_to_delete}
            )
            self.print_result(f"Bulk Delete {len(ids_to_delete)} Companies", response)
            if response.status_code < 400:
                for id in ids_to_delete:
                    self.company_ids.remove(id)

    # ==================== Ad Campaign Tests ====================
    
    def test_create_campaigns(self, count: int = 15):
        """Create multiple ad campaigns"""
        print(f"\n{'='*50}")
        print("TESTING AD CAMPAIGNS")
        print(f"{'='*50}\n")
        
        for i in range(1, count + 1):
            # Associate with company if available
            company_id = self.company_ids[i % len(self.company_ids)] if self.company_ids else None
            response = self.session.post(
                f"{self.base_url}/ad_campaigns",
                json={
                    "title": f"Test Campaign {i}",
                    "company_id": company_id
                }
            )
            self.print_result(f"Create Campaign {i}", response)
            if response.status_code == 201:
                data = response.json()
                # Extract ID from SingleObjectResponse
                entity_id = data.get("id") or data.get("object", {}).get("id")
                if entity_id:
                    self.campaign_ids.append(entity_id)
    
    def test_list_campaigns(self):
        """List all campaigns with pagination"""
        response = self.session.get(f"{self.base_url}/ad_campaigns?page=1&page_size=100")
        self.print_result("List All Campaigns", response)
        
        if response.status_code == 200:
            data = response.json()
            total_pages = data.get('total_pages', 1)
            if total_pages > 1:
                print(f"   📄 Testing pagination: {total_pages} pages")
    
    def test_list_campaigns_by_company(self):
        """List campaigns filtered by company with pagination"""
        if self.company_ids:
            response = self.session.get(
                f"{self.base_url}/ad_campaigns?company_id={self.company_ids[0]}&page=1&page_size=50"
            )
            self.print_result(f"List Campaigns for Company {self.company_ids[0]}", response)
    
    def test_get_campaign(self):
        """Get specific campaign"""
        if self.campaign_ids:
            response = self.session.get(f"{self.base_url}/ad_campaigns/{self.campaign_ids[0]}")
            self.print_result(f"Get Campaign {self.campaign_ids[0]}", response)
    
    def test_update_campaign(self):
        """Update a campaign"""
        if self.campaign_ids and self.company_ids:
            response = self.session.post(
                f"{self.base_url}/ad_campaigns/{self.campaign_ids[0]}/update",
                json={
                    "title": "Updated Campaign Name",
                    "company_id": self.company_ids[0]
                }
            )
            self.print_result(f"Update Campaign {self.campaign_ids[0]}", response)

    # ==================== Ad Group Tests ====================
    
    def test_create_ad_groups(self, count: int = 20):
        """Create multiple ad groups"""
        print(f"\n{'='*50}")
        print("TESTING AD GROUPS")
        print(f"{'='*50}\n")
        
        for i in range(1, count + 1):
            # Associate with campaign if available
            campaign_id = self.campaign_ids[i % len(self.campaign_ids)] if self.campaign_ids else None
            response = self.session.post(
                f"{self.base_url}/ad_groups",
                json={
                    "title": f"Test Ad Group {i}",
                    "ad_campaign_id": campaign_id
                }
            )
            self.print_result(f"Create Ad Group {i}", response)
            if response.status_code == 201:
                data = response.json()
                # Extract ID from SingleObjectResponse
                entity_id = data.get("id") or data.get("object", {}).get("id")
                if entity_id:
                    self.ad_group_ids.append(entity_id)
    
    def test_list_ad_groups(self):
        """List all ad groups with pagination"""
        response = self.session.get(f"{self.base_url}/ad_groups?page=1&page_size=100")
        self.print_result("List All Ad Groups", response)
    
    def test_list_ad_groups_by_campaign(self):
        """List ad groups filtered by campaign with pagination"""
        if self.campaign_ids:
            response = self.session.get(
                f"{self.base_url}/ad_groups?ad_campaign_id={self.campaign_ids[0]}&page=1&page_size=50"
            )
            self.print_result(f"List Ad Groups for Campaign {self.campaign_ids[0]}", response)
    
    def test_get_ad_group(self):
        """Get specific ad group"""
        if self.ad_group_ids:
            response = self.session.get(f"{self.base_url}/ad_groups/{self.ad_group_ids[0]}")
            self.print_result(f"Get Ad Group {self.ad_group_ids[0]}", response)
    
    def test_update_ad_group(self):
        """Update an ad group"""
        if self.ad_group_ids and self.campaign_ids:
            response = self.session.post(
                f"{self.base_url}/ad_groups/{self.ad_group_ids[0]}/update",
                json={
                    "title": "Updated Ad Group Name",
                    "ad_campaign_id": self.campaign_ids[0]
                }
            )
            self.print_result(f"Update Ad Group {self.ad_group_ids[0]}", response)

    # ==================== Keyword Tests ====================
    
    def test_bulk_create_keywords(self, count: int = 50):
        """Bulk create keywords with relations"""
        print(f"\n{'='*50}")
        print("TESTING KEYWORDS")
        print(f"{'='*50}\n")
        
        keywords = [f"test keyword {i}" for i in range(1, count + 1)]
        
        # Create keywords with relations
        response = self.session.post(
            f"{self.base_url}/keywords/bulk",
            json={
                "keywords": keywords,
                "company_ids": self.company_ids[:3] if len(self.company_ids) >= 3 else self.company_ids,
                "ad_campaign_ids": self.campaign_ids[:5] if len(self.campaign_ids) >= 5 else self.campaign_ids,
                "ad_group_ids": self.ad_group_ids[:7] if len(self.ad_group_ids) >= 7 else self.ad_group_ids,
                "match_types": {
                    "broad": True,
                    "phrase": True,
                    "exact": False,
                    "neg_broad": False,
                    "neg_phrase": False,
                    "neg_exact": False
                },
                "override_broad": True,
                "override_phrase": True
            }
        )
        self.print_result(f"Bulk Create {count} Keywords with Relations", response)
        
        # Get keyword IDs
        response = self.session.get(f"{self.base_url}/keywords")
        if response.status_code == 200:
            data = response.json()
            # Handle MultipleObjectsResponse
            objects = data.get("objects", data if isinstance(data, list) else [])
            self.keyword_ids = [k["id"] for k in objects]
    
    def test_list_keywords(self):
        """List all keywords with pagination"""
        response = self.session.get(f"{self.base_url}/keywords?page=1&page_size=100")
        self.print_result("List All Keywords", response)
    
    def test_get_keyword(self):
        """Get specific keyword"""
        if self.keyword_ids:
            response = self.session.get(f"{self.base_url}/keywords/{self.keyword_ids[0]}")
            self.print_result(f"Get Keyword {self.keyword_ids[0]}", response)
    
    def test_update_keyword(self):
        """Update a keyword"""
        if self.keyword_ids:
            response = self.session.post(
                f"{self.base_url}/keywords/{self.keyword_ids[0]}/update",
                json={"keyword": "updated keyword text"}
            )
            self.print_result(f"Update Keyword {self.keyword_ids[0]}", response)
    
    def test_bulk_create_keyword_relations(self):
        """Create relations for existing keywords"""
        if self.keyword_ids and self.company_ids:
            response = self.session.post(
                f"{self.base_url}/keywords/bulk/relations",
                json={
                    "keyword_ids": self.keyword_ids[:10],
                    "company_ids": self.company_ids[:2],
                    "match_types": {
                        "exact": True,
                        "neg_exact": True
                    },
                    "override_exact": True,
                    "override_neg_exact": True
                }
            )
            self.print_result("Create Relations for 10 Keywords", response)
    
    def test_bulk_update_keyword_relations(self):
        """Update match types for existing keyword relations"""
        if self.keyword_ids:
            response = self.session.post(
                f"{self.base_url}/keywords/bulk/relations/update",
                json={
                    "keyword_ids": self.keyword_ids[:15],
                    "broad": False,
                    "exact": True,
                    "override_broad": True,
                    "override_exact": True
                }
            )
            self.print_result("Update Relations for 15 Keywords", response)

    # ==================== Filter Tests ====================
    
    def test_bulk_create_filters(self, count: int = 30):
        """Bulk create filters with relations"""
        print(f"\n{'='*50}")
        print("TESTING FILTERS")
        print(f"{'='*50}\n")
        
        filters = [f"test filter {i}" for i in range(1, count + 1)]
        
        # Create filters with relations (half negative, half positive)
        response = self.session.post(
            f"{self.base_url}/filters/bulk",
            json={
                "filters": filters[:count//2],
                "company_ids": self.company_ids[:3] if len(self.company_ids) >= 3 else self.company_ids,
                "ad_campaign_ids": self.campaign_ids[:5] if len(self.campaign_ids) >= 5 else self.campaign_ids,
                "ad_group_ids": self.ad_group_ids[:7] if len(self.ad_group_ids) >= 7 else self.ad_group_ids,
                "is_negative": True
            }
        )
        self.print_result(f"Bulk Create {count//2} Negative Filters with Relations", response)
        
        # Create positive filters
        response = self.session.post(
            f"{self.base_url}/filters/bulk",
            json={
                "filters": filters[count//2:],
                "company_ids": self.company_ids[:2] if len(self.company_ids) >= 2 else self.company_ids,
                "is_negative": False
            }
        )
        self.print_result(f"Bulk Create {count//2} Positive Filters with Relations", response)
        
        # Get filter IDs
        response = self.session.get(f"{self.base_url}/filters")
        if response.status_code == 200:
            data = response.json()
            # Handle MultipleObjectsResponse
            objects = data.get("objects", data if isinstance(data, list) else [])
            self.filter_ids = [f["id"] for f in objects]
    
    def test_list_filters(self):
        """List all filters with pagination"""
        response = self.session.get(f"{self.base_url}/filters?page=1&page_size=100")
        self.print_result("List All Filters", response)
    
    def test_get_filter(self):
        """Get specific filter"""
        if self.filter_ids:
            response = self.session.get(f"{self.base_url}/filters/{self.filter_ids[0]}")
            self.print_result(f"Get Filter {self.filter_ids[0]}", response)
    
    def test_update_filter(self):
        """Update a filter"""
        if self.filter_ids:
            response = self.session.post(
                f"{self.base_url}/filters/{self.filter_ids[0]}/update",
                json={"filter": "updated filter text"}
            )
            self.print_result(f"Update Filter {self.filter_ids[0]}", response)
    
    def test_bulk_create_filter_relations(self):
        """Create relations for existing filters"""
        if self.filter_ids and self.ad_group_ids:
            response = self.session.post(
                f"{self.base_url}/filters/bulk/relations",
                json={
                    "filter_ids": self.filter_ids[:10],
                    "ad_group_ids": self.ad_group_ids[:5],
                    "is_negative": True
                }
            )
            self.print_result("Create Relations for 10 Filters", response)
    
    def test_bulk_update_filter_relations(self):
        """Update is_negative for existing filter relations"""
        if self.filter_ids:
            response = self.session.post(
                f"{self.base_url}/filters/bulk/relations/update",
                json={
                    "filter_ids": self.filter_ids[:15],
                    "is_negative": False
                }
            )
            self.print_result("Update Relations for 15 Filters", response)

    # ==================== Cleanup Tests ====================
    
    def test_bulk_delete_all(self):
        """Test bulk deletion of all entities"""
        print(f"\n{'='*50}")
        print("TESTING BULK DELETIONS")
        print(f"{'='*50}\n")
        
        # Delete keywords
        if self.keyword_ids:
            response = self.session.post(
                f"{self.base_url}/keywords/bulk/delete",
                json={"ids": self.keyword_ids[:10]}
            )
            self.print_result(f"Bulk Delete 10 Keywords", response)
        
        # Delete filters
        if self.filter_ids:
            response = self.session.post(
                f"{self.base_url}/filters/bulk/delete",
                json={"ids": self.filter_ids[:10]}
            )
            self.print_result(f"Bulk Delete 10 Filters", response)
        
        # Delete ad groups
        if self.ad_group_ids:
            response = self.session.post(
                f"{self.base_url}/ad_groups/bulk/delete",
                json={"ids": self.ad_group_ids[:5]}
            )
            self.print_result(f"Bulk Delete 5 Ad Groups", response)
        
        # Delete campaigns
        if self.campaign_ids:
            response = self.session.post(
                f"{self.base_url}/ad_campaigns/bulk/delete",
                json={"ids": self.campaign_ids[:3]}
            )
            self.print_result(f"Bulk Delete 3 Campaigns", response)
        
        # Delete companies
        if self.company_ids:
            response = self.session.post(
                f"{self.base_url}/companies/bulk/delete",
                json={"ids": self.company_ids[:2]}
            )
            self.print_result(f"Bulk Delete 2 Companies", response)

    # ==================== Summary ====================
    
    def print_summary(self):
        """Print test summary"""
        print(f"\n{'='*50}")
        print("TEST SUMMARY")
        print(f"{'='*50}\n")
        
        # Get final counts (handle MultipleObjectsResponse)
        def get_objects_count(url):
            response = self.session.get(url)
            if response.status_code == 200:
                data = response.json()
                objects = data.get("objects", data if isinstance(data, list) else [])
                return len(objects)
            return 0
        
        companies_count = get_objects_count(f"{self.base_url}/companies")
        campaigns_count = get_objects_count(f"{self.base_url}/ad_campaigns")
        ad_groups_count = get_objects_count(f"{self.base_url}/ad_groups")
        keywords_count = get_objects_count(f"{self.base_url}/keywords")
        filters_count = get_objects_count(f"{self.base_url}/filters")
        
        print(f"📊 Final Database State:")
        print(f"   Companies: {companies_count}")
        print(f"   Campaigns: {campaigns_count}")
        print(f"   Ad Groups: {ad_groups_count}")
        print(f"   Keywords: {keywords_count}")
        print(f"   Filters: {filters_count}")
        print()

    # ==================== Main Test Runner ====================
    
    def run_all_tests(self):
        """Run all tests in sequence"""
        print("\n" + "="*50)
        print("KPLANNER API COMPREHENSIVE TESTS")
        print("="*50 + "\n")
        
        # Test root endpoint
        response = self.session.get(f"{self.base_url}/")
        self.print_result("Root Endpoint", response)
        
        # Companies
        self.test_create_companies(10)
        self.test_list_companies()
        self.test_get_company()
        self.test_update_company()
        
        # Campaigns
        self.test_create_campaigns(15)
        self.test_list_campaigns()
        self.test_list_campaigns_by_company()
        self.test_get_campaign()
        self.test_update_campaign()
        
        # Ad Groups
        self.test_create_ad_groups(20)
        self.test_list_ad_groups()
        self.test_list_ad_groups_by_campaign()
        self.test_get_ad_group()
        self.test_update_ad_group()
        
        # Keywords
        self.test_bulk_create_keywords(50)
        self.test_list_keywords()
        self.test_get_keyword()
        self.test_update_keyword()
        self.test_bulk_create_keyword_relations()
        self.test_bulk_update_keyword_relations()
        
        # Filters
        self.test_bulk_create_filters(30)
        self.test_list_filters()
        self.test_get_filter()
        self.test_update_filter()
        self.test_bulk_create_filter_relations()
        self.test_bulk_update_filter_relations()
        
        # Pagination Edge Cases
        self.test_pagination_edge_cases()
        
        # Cleanup
        self.test_bulk_delete_all()
        
        # Summary
        self.print_summary()
        
        print("\n" + "="*50)
        print("ALL TESTS COMPLETED!")
        print("="*50 + "\n")
    
    def test_pagination_edge_cases(self):
        """Test pagination edge cases"""
        print(f"\n{'='*50}")
        print("TESTING PAGINATION EDGE CASES")
        print(f"{'='*50}\n")
        
        # Test 1: Page 0 (should default to 1)
        print("✅ Test 1: Page 0 (should default to page 1)")
        response = self.session.get(f"{self.base_url}/companies?page=0&page_size=10")
        if response.status_code == 200:
            data = response.json()
            print(f"   Requested page: 0, Returned page: {data.get('page')}")
        
        # Test 2: Page size > 100 (should be clamped to 100)
        print("\n✅ Test 2: Page size 200 (should be clamped to 100)")
        response = self.session.get(f"{self.base_url}/companies?page=1&page_size=200")
        if response.status_code == 200:
            data = response.json()
            print(f"   Requested page_size: 200, Returned page_size: {data.get('page_size')}")
        
        # Test 3: Negative page size (should default to minimum)
        print("\n✅ Test 3: Negative page size (should use minimum)")
        response = self.session.get(f"{self.base_url}/companies?page=1&page_size=-5")
        if response.status_code == 200:
            data = response.json()
            print(f"   Requested page_size: -5, Returned page_size: {data.get('page_size')}")
        
        # Test 4: Very large page number
        print("\n✅ Test 4: Page beyond total_pages (should return empty)")
        response = self.session.get(f"{self.base_url}/companies?page=999999&page_size=10")
        if response.status_code == 200:
            data = response.json()
            print(f"   Requested page: 999999, Items returned: {len(data.get('objects', []))}")
        
        # Test 5: Small page size (test multiple pages)
        print("\n✅ Test 5: Small page size (5 items per page)")
        response = self.session.get(f"{self.base_url}/companies?page=1&page_size=5")
        if response.status_code == 200:
            data = response.json()
            print(f"   Page size: {data.get('page_size')}, Total pages: {data.get('total_pages')}")
            print(f"   Total items: {data.get('total')}, Items on this page: {len(data.get('objects', []))}")
        
        print("\n" + "="*50 + "\n")


if __name__ == "__main__":
    tester = APITester(BASE_URL)
    try:
        tester.run_all_tests()
    except KeyboardInterrupt:
        print("\n\n❌ Tests interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Error during testing: {str(e)}")
        import traceback
        traceback.print_exc()
