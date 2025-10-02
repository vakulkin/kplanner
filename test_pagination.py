#!/usr/bin/env python3
"""
Test script for pagination and batch processing features.
"""
import requests
import json

BASE_URL = "http://localhost:8000"

def print_section(title):
    """Print a formatted section header."""
    print(f"\n{'='*60}")
    print(f"{title}")
    print('='*60)

def test_pagination():
    """Test pagination on list endpoints."""
    print_section("TESTING PAGINATION")
    
    # Test 1: Get first page of companies
    print("\n1. Get first page of companies (page_size=10)")
    response = requests.get(f"{BASE_URL}/companies", params={
        "page": 1,
        "page_size": 10
    })
    data = response.json()
    print(f"   Status: {response.status_code}")
    print(f"   Total companies: {data.get('total')}")
    print(f"   Current page: {data.get('page')}")
    print(f"   Page size: {data.get('page_size')}")
    print(f"   Total pages: {data.get('total_pages')}")
    print(f"   Items returned: {len(data.get('objects', []))}")
    
    # Test 2: Get second page
    if data.get('total_pages', 0) > 1:
        print("\n2. Get second page of companies")
        response = requests.get(f"{BASE_URL}/companies", params={
            "page": 2,
            "page_size": 10
        })
        data = response.json()
        print(f"   Status: {response.status_code}")
        print(f"   Current page: {data.get('page')}")
        print(f"   Items returned: {len(data.get('objects', []))}")
    
    # Test 3: Get all pages
    print("\n3. Fetch all companies across all pages")
    all_companies = []
    page = 1
    while True:
        response = requests.get(f"{BASE_URL}/companies", params={
            "page": page,
            "page_size": 100
        })
        data = response.json()
        all_companies.extend(data.get('objects', []))
        print(f"   Fetched page {page}/{data.get('total_pages')}: {len(data.get('objects', []))} items")
        if page >= data.get('total_pages', 1):
            break
        page += 1
    print(f"   Total companies fetched: {len(all_companies)}")
    
    # Test 4: Pagination on filtered results
    print("\n4. Get paginated ad campaigns for first company")
    if all_companies:
        company_id = all_companies[0].get('id')
        response = requests.get(f"{BASE_URL}/ad_campaigns", params={
            "company_id": company_id,
            "page": 1,
            "page_size": 10
        })
        data = response.json()
        print(f"   Status: {response.status_code}")
        print(f"   Total campaigns for company {company_id}: {data.get('total')}")
        print(f"   Items returned: {len(data.get('objects', []))}")

def test_batch_processing():
    """Test batch processing on bulk update endpoints."""
    print_section("TESTING BATCH PROCESSING")
    
    # Get some keywords to update
    print("\n1. Fetch keywords for batch update testing")
    response = requests.get(f"{BASE_URL}/keywords", params={
        "page": 1,
        "page_size": 50
    })
    data = response.json()
    keywords = data.get('objects', [])
    keyword_ids = [k['id'] for k in keywords[:30]]  # Take up to 30 keywords
    
    print(f"   Found {len(keywords)} keywords")
    print(f"   Selected {len(keyword_ids)} keywords for testing")
    
    if not keyword_ids:
        print("   ⚠️  No keywords available for testing")
        return
    
    # Test 2: Batch update keyword relations
    print("\n2. Batch update keyword relations (should process in batches of 25)")
    response = requests.post(
        f"{BASE_URL}/keywords/bulk/relations/update",
        json={
            "keyword_ids": keyword_ids,
            "broad": True,
            "exact": False,
            "override_broad": True,
            "override_exact": True
        }
    )
    
    if response.status_code == 200:
        data = response.json()
        print(f"   ✅ Status: {response.status_code}")
        print(f"   Message: {data.get('message')}")
        print(f"   Keywords updated: {data.get('updated')}")
        print(f"   Relations updated: {data.get('relations_updated')}")
        print(f"   Batches processed: {data.get('batches_processed')}")
        print(f"   Batch size: {data.get('batch_size')}")
    else:
        print(f"   ❌ Status: {response.status_code}")
        print(f"   Error: {response.json()}")
    
    # Test 3: Test maximum limit (100 items)
    print("\n3. Test request limit (should fail if > 100 items)")
    
    # Get more keywords if available
    response = requests.get(f"{BASE_URL}/keywords", params={
        "page": 1,
        "page_size": 100
    })
    data = response.json()
    all_keyword_ids = [k['id'] for k in data.get('objects', [])]
    
    if len(all_keyword_ids) > 100:
        print(f"   Testing with {len(all_keyword_ids[:101])} keyword IDs (should fail)")
        response = requests.post(
            f"{BASE_URL}/keywords/bulk/relations/update",
            json={
                "keyword_ids": all_keyword_ids[:101],
                "broad": True,
                "override_broad": True
            }
        )
        if response.status_code == 400:
            print(f"   ✅ Correctly rejected: {response.json().get('detail')}")
        else:
            print(f"   ⚠️  Expected 400 error, got {response.status_code}")
    else:
        print(f"   Only {len(all_keyword_ids)} keywords available (need >100 to test limit)")

def test_edge_cases():
    """Test edge cases for pagination."""
    print_section("TESTING EDGE CASES")
    
    # Test 1: Invalid page number (0)
    print("\n1. Request page 0 (should default to page 1)")
    response = requests.get(f"{BASE_URL}/companies", params={
        "page": 0,
        "page_size": 10
    })
    data = response.json()
    print(f"   Status: {response.status_code}")
    print(f"   Returned page: {data.get('page')}")
    
    # Test 2: Page size > 100 (should be clamped to 100)
    print("\n2. Request page_size=200 (should be clamped to 100)")
    response = requests.get(f"{BASE_URL}/companies", params={
        "page": 1,
        "page_size": 200
    })
    data = response.json()
    print(f"   Status: {response.status_code}")
    print(f"   Returned page_size: {data.get('page_size')}")
    
    # Test 3: Page beyond total pages
    print("\n3. Request page beyond total_pages")
    response = requests.get(f"{BASE_URL}/companies", params={
        "page": 1,
        "page_size": 10
    })
    first_page = response.json()
    total_pages = first_page.get('total_pages', 1)
    
    response = requests.get(f"{BASE_URL}/companies", params={
        "page": total_pages + 10,
        "page_size": 10
    })
    data = response.json()
    print(f"   Status: {response.status_code}")
    print(f"   Total pages: {total_pages}")
    print(f"   Requested page: {total_pages + 10}")
    print(f"   Items returned: {len(data.get('objects', []))}")

def main():
    """Run all tests."""
    print("\n" + "="*60)
    print("PAGINATION AND BATCH PROCESSING TEST SUITE")
    print("="*60)
    
    try:
        test_pagination()
        test_batch_processing()
        test_edge_cases()
        
        print("\n" + "="*60)
        print("✅ ALL TESTS COMPLETED")
        print("="*60)
        
    except requests.exceptions.ConnectionError:
        print("\n❌ ERROR: Could not connect to API server")
        print("   Make sure the server is running at http://localhost:8000")
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
