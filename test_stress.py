"""
Stress Tests for KPlanner API
Creates large amounts of data and tests performance with pagination support
"""
import requests
import json
import time
from typing import List, Dict, Any

BASE_URL = "http://localhost:8000"

class StressTester:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.session = requests.Session()
    
    def fetch_all_pages(self, endpoint: str, **params) -> List[Dict[str, Any]]:
        """Fetch all pages from a paginated endpoint"""
        all_items = []
        page = 1
        
        while True:
            params['page'] = page
            params.setdefault('page_size', 100)
            
            response = self.session.get(f"{self.base_url}/{endpoint}", params=params)
            if response.status_code != 200:
                break
            
            data = response.json()
            items = data.get('objects', [])
            all_items.extend(items)
            
            total_pages = data.get('total_pages', 1)
            if page >= total_pages or not items:
                break
            
            page += 1
        
        return all_items
        
    def print_result(self, test_name: str, response: requests.Response, duration: float):
        """Print test results with timing"""
        status_icon = "✅" if response.status_code < 400 else "❌"
        print(f"{status_icon} {test_name}")
        print(f"   Status: {response.status_code} | Time: {duration:.2f}s")
        if response.status_code < 400:
            try:
                data = response.json()
                if isinstance(data, dict):
                    for key in ['status', 'message', 'created', 'existing', 'total',
                                'deleted', 'requested', 'updated', 'processed',
                                'relations_added', 'relations_updated', 'batches_processed', 'batch_size']:
                        if key in data:
                            print(f"   {key}: {data[key]}")
                    # For SingleObjectResponse with nested object
                    if 'object' in data and isinstance(data['object'], dict):
                        if 'id' in data['object']:
                            print(f"   object.id: {data['object']['id']}")
                    # For MultipleObjectsResponse with objects array and pagination
                    if 'objects' in data and isinstance(data['objects'], list):
                        print(f"   objects count: {len(data['objects'])}")
                        if 'total_pages' in data:
                            print(f"   page {data.get('page', 1)}/{data.get('total_pages', 1)} (total: {data.get('total', 0)})")
                elif isinstance(data, list):
                    print(f"   Count: {len(data)}")
            except:
                pass
        else:
            print(f"   Error: {response.text[:100]}")
        print()

    def stress_test_bulk_keywords(self):
        """Create a large number of keywords with relations"""
        print(f"\n{'='*60}")
        print("STRESS TEST: BULK KEYWORDS")
        print(f"{'='*60}\n")
        
        # First, create companies, campaigns, and ad groups
        print("Setting up entities for keyword relations...")
        
        # Create 20 companies
        company_ids = []
        for i in range(1, 21):
            response = self.session.post(
                f"{self.base_url}/companies",
                json={"title": f"Stress Test Company {i}"}
            )
            if response.status_code == 201:
                data = response.json()
                # Extract ID from SingleObjectResponse
                entity_id = data.get("id") or data.get("object", {}).get("id")
                if entity_id:
                    company_ids.append(entity_id)
        print(f"✅ Created {len(company_ids)} companies")
        
        # Create 50 campaigns
        campaign_ids = []
        for i in range(1, 51):
            response = self.session.post(
                f"{self.base_url}/ad_campaigns",
                json={
                    "title": f"Stress Test Campaign {i}",
                    "company_id": company_ids[i % len(company_ids)]
                }
            )
            if response.status_code == 201:
                data = response.json()
                # Extract ID from SingleObjectResponse
                entity_id = data.get("id") or data.get("object", {}).get("id")
                if entity_id:
                    campaign_ids.append(entity_id)
        print(f"✅ Created {len(campaign_ids)} campaigns")
        
        # Create 100 ad groups
        ad_group_ids = []
        for i in range(1, 101):
            response = self.session.post(
                f"{self.base_url}/ad_groups",
                json={
                    "title": f"Stress Test Ad Group {i}",
                    "ad_campaign_id": campaign_ids[i % len(campaign_ids)]
                }
            )
            if response.status_code == 201:
                data = response.json()
                # Extract ID from SingleObjectResponse
                entity_id = data.get("id") or data.get("object", {}).get("id")
                if entity_id:
                    ad_group_ids.append(entity_id)
        print(f"✅ Created {len(ad_group_ids)} ad groups\n")
        
        # Test 1: Create 500 keywords with relations
        keywords_500 = [f"stress keyword {i}" for i in range(1, 501)]
        start = time.time()
        response = self.session.post(
            f"{self.base_url}/keywords/bulk",
            json={
                "keywords": keywords_500,
                "company_ids": company_ids[:10],
                "ad_campaign_ids": campaign_ids[:20],
                "ad_group_ids": ad_group_ids[:30],
                "match_types": {
                    "broad": True,
                    "phrase": True,
                    "exact": True
                }
            }
        )
        duration = time.time() - start
        self.print_result("Create 500 Keywords with Relations", response, duration)
        
        # Get all keyword IDs
        response = self.session.get(f"{self.base_url}/keywords")
        if response.status_code == 200:
            data = response.json()
            # Handle MultipleObjectsResponse
            objects = data.get("objects", data if isinstance(data, list) else [])
            keyword_ids = [k["id"] for k in objects]
        else:
            keyword_ids = []
        
        # Test 2: Update relations for 200 keywords
        start = time.time()
        response = self.session.post(
            f"{self.base_url}/keywords/bulk/relations/update",
            json={
                "keyword_ids": keyword_ids[:200],
                "broad": False,
                "exact": True,
                "neg_broad": True,
                "override_broad": True,
                "override_exact": True,
                "override_neg_broad": True
            }
        )
        duration = time.time() - start
        self.print_result("Update Relations for 200 Keywords", response, duration)
        
        # Test 3: Create additional relations for 150 keywords
        start = time.time()
        response = self.session.post(
            f"{self.base_url}/keywords/bulk/relations",
            json={
                "keyword_ids": keyword_ids[:150],
                "company_ids": company_ids[10:15],
                "ad_campaign_ids": campaign_ids[30:40],
                "match_types": {
                    "phrase": True,
                    "neg_phrase": True
                }
            }
        )
        duration = time.time() - start
        self.print_result("Create Additional Relations for 150 Keywords", response, duration)
        
        # Test 4: Bulk delete 100 keywords
        start = time.time()
        response = self.session.post(
            f"{self.base_url}/keywords/bulk/delete",
            json={"ids": keyword_ids[:100]}
        )
        duration = time.time() - start
        self.print_result("Bulk Delete 100 Keywords", response, duration)
        
        return company_ids, campaign_ids, ad_group_ids

    def stress_test_bulk_filters(self, company_ids, campaign_ids, ad_group_ids):
        """Create a large number of filters with relations"""
        print(f"\n{'='*60}")
        print("STRESS TEST: BULK FILTERS")
        print(f"{'='*60}\n")
        
        # Test 1: Create 300 filters with relations
        filters_300 = [f"stress filter {i}" for i in range(1, 301)]
        start = time.time()
        response = self.session.post(
            f"{self.base_url}/filters/bulk",
            json={
                "filters": filters_300[:150],
                "company_ids": company_ids[:10],
                "ad_campaign_ids": campaign_ids[:20],
                "ad_group_ids": ad_group_ids[:30],
                "is_negative": True
            }
        )
        duration = time.time() - start
        self.print_result("Create 150 Negative Filters with Relations", response, duration)
        
        # Create positive filters
        start = time.time()
        response = self.session.post(
            f"{self.base_url}/filters/bulk",
            json={
                "filters": filters_300[150:],
                "company_ids": company_ids[10:15],
                "ad_campaign_ids": campaign_ids[20:30],
                "is_negative": False
            }
        )
        duration = time.time() - start
        self.print_result("Create 150 Positive Filters with Relations", response, duration)
        
        # Get all filter IDs
        response = self.session.get(f"{self.base_url}/filters")
        if response.status_code == 200:
            data = response.json()
            # Handle MultipleObjectsResponse
            objects = data.get("objects", data if isinstance(data, list) else [])
            filter_ids = [f["id"] for f in objects]
        else:
            filter_ids = []
        
        # Test 2: Update relations for 100 filters
        start = time.time()
        response = self.session.post(
            f"{self.base_url}/filters/bulk/relations/update",
            json={
                "filter_ids": filter_ids[:100],
                "is_negative": False
            }
        )
        duration = time.time() - start
        self.print_result("Update Relations for 100 Filters", response, duration)
        
        # Test 3: Create additional relations for 80 filters
        start = time.time()
        response = self.session.post(
            f"{self.base_url}/filters/bulk/relations",
            json={
                "filter_ids": filter_ids[:80],
                "ad_group_ids": ad_group_ids[40:60],
                "is_negative": True
            }
        )
        duration = time.time() - start
        self.print_result("Create Additional Relations for 80 Filters", response, duration)
        
        # Test 4: Bulk delete 50 filters
        start = time.time()
        response = self.session.post(
            f"{self.base_url}/filters/bulk/delete",
            json={"ids": filter_ids[:50]}
        )
        duration = time.time() - start
        self.print_result("Bulk Delete 50 Filters", response, duration)

    def stress_test_updates(self):
        """Test rapid updates of entities"""
        print(f"\n{'='*60}")
        print("STRESS TEST: RAPID UPDATES")
        print(f"{'='*60}\n")
        
        # Helper function to get objects from response
        def get_objects(response):
            if response.status_code != 200:
                return []
            data = response.json()
            return data.get("objects", data if isinstance(data, list) else [])
        
        # Get some entities to update
        companies = get_objects(self.session.get(f"{self.base_url}/companies"))[:10]
        campaigns = get_objects(self.session.get(f"{self.base_url}/ad_campaigns"))[:10]
        ad_groups = get_objects(self.session.get(f"{self.base_url}/ad_groups"))[:10]
        keywords = get_objects(self.session.get(f"{self.base_url}/keywords"))[:10]
        filters = get_objects(self.session.get(f"{self.base_url}/filters"))[:10]
        
        # Update companies rapidly
        start = time.time()
        for i, company in enumerate(companies):
            self.session.post(
                f"{self.base_url}/companies/{company['id']}/update",
                json={"title": f"Updated Company {i}"}
            )
        duration = time.time() - start
        print(f"✅ Updated {len(companies)} companies in {duration:.2f}s")
        
        # Update campaigns rapidly
        start = time.time()
        for i, campaign in enumerate(campaigns):
            self.session.post(
                f"{self.base_url}/ad_campaigns/{campaign['id']}/update",
                json={
                    "title": f"Updated Campaign {i}",
                    "company_id": campaign['company_id']
                }
            )
        duration = time.time() - start
        print(f"✅ Updated {len(campaigns)} campaigns in {duration:.2f}s")
        
        # Update ad groups rapidly
        start = time.time()
        for i, ad_group in enumerate(ad_groups):
            self.session.post(
                f"{self.base_url}/ad_groups/{ad_group['id']}/update",
                json={
                    "title": f"Updated Ad Group {i}",
                    "ad_campaign_id": ad_group['ad_campaign_id']
                }
            )
        duration = time.time() - start
        print(f"✅ Updated {len(ad_groups)} ad groups in {duration:.2f}s")
        
        # Update keywords rapidly
        start = time.time()
        for i, keyword in enumerate(keywords):
            self.session.post(
                f"{self.base_url}/keywords/{keyword['id']}/update",
                json={"keyword": f"updated keyword {i}"}
            )
        duration = time.time() - start
        print(f"✅ Updated {len(keywords)} keywords in {duration:.2f}s")
        
        # Update filters rapidly
        start = time.time()
        for i, filter_obj in enumerate(filters):
            self.session.post(
                f"{self.base_url}/filters/{filter_obj['id']}/update",
                json={"filter": f"updated filter {i}"}
            )
        duration = time.time() - start
        print(f"✅ Updated {len(filters)} filters in {duration:.2f}s\n")

    def stress_test_list_operations(self):
        """Test listing operations with large datasets"""
        print(f"\n{'='*60}")
        print("STRESS TEST: LIST OPERATIONS")
        print(f"{'='*60}\n")
        
        operations = [
            ("List All Companies", f"{self.base_url}/companies"),
            ("List All Campaigns", f"{self.base_url}/ad_campaigns"),
            ("List All Ad Groups", f"{self.base_url}/ad_groups"),
            ("List All Keywords", f"{self.base_url}/keywords"),
            ("List All Filters", f"{self.base_url}/filters"),
        ]
        
        for name, url in operations:
            start = time.time()
            response = self.session.get(url)
            duration = time.time() - start
            if response.status_code == 200:
                data = response.json()
                objects = data.get("objects", data if isinstance(data, list) else [])
                count = len(objects)
            else:
                count = 0
            print(f"✅ {name}: {count} items in {duration:.2f}s")

    def cleanup_all(self):
        """Clean up all test data"""
        print(f"\n{'='*60}")
        print("CLEANUP: DELETING ALL TEST DATA")
        print(f"{'='*60}\n")
        
        # Helper function to get objects from response
        def get_objects(response):
            if response.status_code != 200:
                return []
            data = response.json()
            return data.get("objects", data if isinstance(data, list) else [])
        
        # Get all IDs
        keywords = get_objects(self.session.get(f"{self.base_url}/keywords"))
        filters = get_objects(self.session.get(f"{self.base_url}/filters"))
        ad_groups = get_objects(self.session.get(f"{self.base_url}/ad_groups"))
        campaigns = get_objects(self.session.get(f"{self.base_url}/ad_campaigns"))
        companies = get_objects(self.session.get(f"{self.base_url}/companies"))
        
        # Delete in batches
        def delete_in_batches(name, url, ids, batch_size=50):
            total = len(ids)
            deleted = 0
            for i in range(0, total, batch_size):
                batch = ids[i:i+batch_size]
                response = self.session.post(url, json={"ids": batch})
                if response.status_code < 400:
                    deleted += response.json().get('deleted', 0)
            print(f"✅ Deleted {deleted}/{total} {name}")
        
        delete_in_batches("keyword", f"{self.base_url}/keywords/bulk/delete", 
                         [k['id'] for k in keywords])
        delete_in_batches("filter", f"{self.base_url}/filters/bulk/delete", 
                         [f['id'] for f in filters])
        delete_in_batches("ad_group", f"{self.base_url}/ad_groups/bulk/delete", 
                         [a['id'] for a in ad_groups])
        delete_in_batches("campaign", f"{self.base_url}/ad_campaigns/bulk/delete", 
                         [c['id'] for c in campaigns])
        delete_in_batches("company", f"{self.base_url}/companies/bulk/delete", 
                         [c['id'] for c in companies])

    def run_stress_tests(self):
        """Run all stress tests"""
        print("\n" + "="*60)
        print("KPLANNER API STRESS TESTS")
        print("Creating and testing with large datasets...")
        print("="*60 + "\n")
        
        start_time = time.time()
        
        # Run tests
        company_ids, campaign_ids, ad_group_ids = self.stress_test_bulk_keywords()
        self.stress_test_bulk_filters(company_ids, campaign_ids, ad_group_ids)
        self.stress_test_updates()
        self.stress_test_list_operations()
        self.stress_test_pagination_performance()
        
        total_duration = time.time() - start_time
        
        # Final stats
        print(f"\n{'='*60}")
        print("FINAL STATISTICS")
        print(f"{'='*60}\n")
        
        print("📊 Fetching database state (using pagination)...")
        companies = self.fetch_all_pages("companies")
        campaigns = self.fetch_all_pages("ad_campaigns")
        ad_groups = self.fetch_all_pages("ad_groups")
        keywords = self.fetch_all_pages("keywords")
        filters = self.fetch_all_pages("filters")
        
        print(f"   Companies: {len(companies)}")
        print(f"   Campaigns: {len(campaigns)}")
        print(f"   Ad Groups: {len(ad_groups)}")
        print(f"   Keywords: {len(keywords)}")
        print(f"   Filters: {len(filters)}")
        print(f"\n⏱️  Total Test Duration: {total_duration:.2f}s")
        
        # Cleanup
        print()
        cleanup = input("Clean up all test data? (y/n): ")
        if cleanup.lower() == 'y':
            self.cleanup_all()
        
        print("\n" + "="*60)
        print("STRESS TESTS COMPLETED!")
        print("="*60 + "\n")
    
    def stress_test_pagination_performance(self):
        """Test pagination performance with large datasets"""
        print(f"\n{'='*60}")
        print("STRESS TEST: PAGINATION PERFORMANCE")
        print(f"{'='*60}\n")
        
        # Test 1: Fetch all companies across all pages
        print("📄 Test 1: Fetching all companies (paginated)")
        start = time.time()
        all_companies = self.fetch_all_pages("companies")
        duration = time.time() - start
        print(f"   ✅ Fetched {len(all_companies)} companies in {duration:.2f}s")
        
        # Test 2: Different page sizes performance comparison
        print("\n📄 Test 2: Page size performance comparison")
        for page_size in [10, 25, 50, 100]:
            start = time.time()
            params = {'page': 1, 'page_size': page_size}
            response = self.session.get(f"{self.base_url}/companies", params=params)
            duration = time.time() - start
            if response.status_code == 200:
                data = response.json()
                print(f"   Page size {page_size:3d}: {duration:.3f}s | {len(data.get('objects', []))} items | {data.get('total_pages')} pages")
        
        # Test 3: Filtered pagination (campaigns by company)
        if all_companies:
            print("\n📄 Test 3: Filtered pagination (campaigns by company)")
            company_id = all_companies[0]['id']
            start = time.time()
            campaigns = self.fetch_all_pages("ad_campaigns", company_id=company_id)
            duration = time.time() - start
            print(f"   ✅ Fetched {len(campaigns)} campaigns for company {company_id} in {duration:.3f}s")
        
        # Test 4: Sequential vs parallel page access (simulate)
        print("\n📄 Test 4: Sequential page access time")
        start = time.time()
        for page in range(1, min(6, 11)):  # Test first 5 pages or less
            params = {'page': page, 'page_size': 20}
            response = self.session.get(f"{self.base_url}/keywords", params=params)
            if response.status_code == 200:
                data = response.json()
                if data.get('objects'):
                    print(f"   Page {page}: {len(data.get('objects', []))} items")
                else:
                    break
        duration = time.time() - start
        print(f"   ⏱️  Total sequential access time: {duration:.2f}s")
        
        # Test 5: Large page numbers (edge case)
        print("\n📄 Test 5: Accessing high page numbers")
        for page_num in [100, 500, 1000]:
            start = time.time()
            response = self.session.get(
                f"{self.base_url}/companies",
                params={'page': page_num, 'page_size': 10}
            )
            duration = time.time() - start
            if response.status_code == 200:
                data = response.json()
                print(f"   Page {page_num}: {duration:.3f}s | {len(data.get('objects', []))} items returned")
        
        print()


if __name__ == "__main__":
    tester = StressTester(BASE_URL)
    try:
        tester.run_stress_tests()
    except KeyboardInterrupt:
        print("\n\n❌ Tests interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Error during testing: {str(e)}")
        import traceback
        traceback.print_exc()
