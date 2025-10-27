#!/usr/bin/env python3
"""
Demo Data Management Script for KPlanner API

This script provides functionality to:
1. Generate and import large amounts of demo data
2. Clean up/remove demo data
3. Verify data import
4. Test performance with various data volumes

Usage:
    python3 demo_data.py import [small|medium|large|huge]
    python3 demo_data.py cleanup
    python3 demo_data.py verify
    python3 demo_data.py stats
"""

import os
import argparse
import random
import sys
from datetime import datetime
from typing import List
import requests
from faker import Faker

# Add the project root directory to the path so we can import our modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configuration
API_BASE_URL = os.getenv("API_URL", "http://localhost:8000")
DEMO_USER_ID = "clerk_demo_user"

# Data sizes configuration
DATA_SIZES = {
    "small": {
        "companies": 5,
        "campaigns_per_company": 3,
        "adgroups_per_campaign": 3,
        "keywords": 100,
        "relations_per_keyword": 3  # Average relations per keyword
    },
    "medium": {
        "companies": 10,
        "campaigns_per_company": 5,
        "adgroups_per_campaign": 5,
        "keywords": 1000,
        "relations_per_keyword": 5
    },
    "large": {
        "companies": 20,
        "campaigns_per_company": 10,
        "adgroups_per_campaign": 10,
        "keywords": 5000,
        "relations_per_keyword": 8
    },
    "huge": {
        "companies": 50,
        "campaigns_per_company": 20,
        "adgroups_per_campaign": 15,
        "keywords": 20000,
        "relations_per_keyword": 10
    }
}

# Initialize Faker for realistic data
fake = Faker()

# Color codes for terminal output


class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    END = '\033[0m'
    BOLD = '\033[1m'


def print_header(text: str):
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'='*60}{Colors.END}")
    print(f"{Colors.HEADER}{Colors.BOLD}{text:^60}{Colors.END}")
    print(f"{Colors.HEADER}{Colors.BOLD}{'='*60}{Colors.END}\n")


def print_success(text: str):
    print(f"{Colors.GREEN}✓ {text}{Colors.END}")


def print_error(text: str):
    print(f"{Colors.RED}✗ {text}{Colors.END}")


def print_info(text: str):
    print(f"{Colors.CYAN}ℹ {text}{Colors.END}")


def print_warning(text: str):
    print(f"{Colors.YELLOW}⚠ {text}{Colors.END}")


def generate_company_name() -> str:
    """Generate a realistic company name"""
    templates = [
        f"{fake.company()} {random.choice(['Inc', 'LLC', 'Corp', 'Group', 'Solutions'])}",
        f"{fake.last_name()} {random.choice(['Industries', 'Enterprises', 'Technologies', 'Consulting'])}",
        f"{fake.word().capitalize()} {random.choice(['Digital', 'Global', 'Pro', 'Tech', 'Systems'])}"
    ]
    return random.choice(templates)


def generate_campaign_name(company_name: str) -> str:
    """Generate a realistic campaign name"""
    season = random.choice(
        ['Spring', 'Summer', 'Fall', 'Winter', 'Q1', 'Q2', 'Q3', 'Q4'])
    year = random.choice(['2024', '2025'])
    campaign_type = random.choice(
        ['Brand', 'Performance', 'Awareness', 'Conversion', 'Retargeting'])
    return f"{company_name.split()[0]} {season} {year} {campaign_type}"


def generate_adgroup_name(campaign_name: str) -> str:
    """Generate a realistic ad group name"""
    target = random.choice(['Desktop', 'Mobile', 'Tablet', 'All Devices'])
    audience = random.choice(
        ['General', 'Young Adults', 'Professionals', 'Students', 'Seniors'])
    return f"{campaign_name.split()[0]} - {target} - {audience}"


def generate_keywords(count: int) -> List[str]:
    """Generate realistic keywords"""
    keywords = []

    # Product-related keywords
    products = ['shoes', 'laptop', 'phone', 'watch', 'camera', 'headphones', 'tablet',
                'furniture', 'clothing', 'books', 'software', 'service']
    modifiers = ['best', 'cheap', 'professional', 'premium', 'affordable', 'quality',
                 'top', 'discount', 'sale', 'online', 'near me', 'reviews']

    for _ in range(count):
        if random.random() < 0.5:
            # Two-word keywords
            word1 = random.choice(
                modifiers if random.random() < 0.5 else products)
            word2 = random.choice(products)
            keywords.append(f"{word1} {word2}")
        else:
            # Three-word keywords
            word1 = random.choice(modifiers)
            word2 = random.choice(products)
            word3 = random.choice(
                ['store', 'shop', 'price', 'deals', 'buy', 'online'])
            keywords.append(f"{word1} {word2} {word3}")

    return list(set(keywords))  # Remove duplicates


def create_companies(count: int, activate_limit: int = 3) -> List[int]:
    """Create companies and return their IDs"""
    print_info(f"Creating {count} companies...")
    company_ids = []

    for i in range(count):
        is_active = i < activate_limit  # Only first N are active
        response = requests.post(
            f"{API_BASE_URL}/companies",
            json={
                "title": generate_company_name(),
                "is_active": is_active
            }
        )

        if response.status_code == 201:
            company_id = response.json()["object"]["id"]
            company_ids.append(company_id)
            if (i + 1) % 10 == 0:
                print(f"  Created {i + 1}/{count} companies...")
        else:
            print_error(f"Failed to create company: {response.text}")

    print_success(
        f"Created {len(company_ids)} companies ({activate_limit} active)")
    return company_ids


def create_campaigns(company_ids: List[int], per_company: int, activate_limit: int = 5) -> List[int]:
    """Create campaigns for companies and return their IDs"""
    print_info(f"Creating {per_company} campaigns per company...")
    campaign_ids = []
    total = len(company_ids) * per_company
    created = 0

    for company_id in company_ids:
        company_name = f"Company_{company_id}"
        for i in range(per_company):
            # Only first N are active
            is_active = len(campaign_ids) < activate_limit
            response = requests.post(
                f"{API_BASE_URL}/ad_campaigns",
                json={
                    "title": generate_campaign_name(company_name),
                    "company_id": company_id,
                    "is_active": is_active
                }
            )

            if response.status_code == 201:
                campaign_id = response.json()["object"]["id"]
                campaign_ids.append(campaign_id)
                created += 1
                if created % 25 == 0:
                    print(f"  Created {created}/{total} campaigns...")
            else:
                print_error(f"Failed to create campaign: {response.text}")

    print_success(
        f"Created {len(campaign_ids)} campaigns ({activate_limit} active)")
    return campaign_ids


def create_adgroups(campaign_ids: List[int], per_campaign: int, activate_limit: int = 7) -> List[int]:
    """Create ad groups for campaigns and return their IDs"""
    print_info(f"Creating {per_campaign} ad groups per campaign...")
    adgroup_ids = []
    total = len(campaign_ids) * per_campaign
    created = 0

    for campaign_id in campaign_ids:
        campaign_name = f"Campaign_{campaign_id}"
        for i in range(per_campaign):
            # Only first N are active
            is_active = len(adgroup_ids) < activate_limit
            response = requests.post(
                f"{API_BASE_URL}/ad_groups",
                json={
                    "title": generate_adgroup_name(campaign_name),
                    "ad_campaign_id": campaign_id,
                    "is_active": is_active
                }
            )

            if response.status_code == 201:
                adgroup_id = response.json()["object"]["id"]
                adgroup_ids.append(adgroup_id)
                created += 1
                if created % 50 == 0:
                    print(f"  Created {created}/{total} ad groups...")
            else:
                print_error(f"Failed to create ad group: {response.text}")

    print_success(
        f"Created {len(adgroup_ids)} ad groups ({activate_limit} active)")
    return adgroup_ids


def create_keywords_with_relations(
    keywords: List[str],
    company_ids: List[int],
    campaign_ids: List[int],
    adgroup_ids: List[int],
    avg_relations: int,
    batch_size: int = 100
) -> int:
    """Create keywords with relations in batches"""
    print_info(f"Creating {len(keywords)} keywords with relations...")
    total_created = 0

    # Process in batches
    for i in range(0, len(keywords), batch_size):
        batch = keywords[i:i + batch_size]

        # Randomly select entities for this batch
        selected_companies = random.sample(
            company_ids, min(avg_relations, len(company_ids)))
        selected_campaigns = random.sample(
            campaign_ids, min(avg_relations, len(campaign_ids)))
        selected_adgroups = random.sample(
            adgroup_ids, min(avg_relations, len(adgroup_ids)))

        # Random match types
        response = requests.post(
            f"{API_BASE_URL}/keywords/bulk",
            json={
                "keywords": batch,
                "company_ids": selected_companies,
                "ad_campaign_ids": selected_campaigns,
                "ad_group_ids": selected_adgroups,
                "broad": random.choice([True, False]),
                "phrase": random.choice([True, False]),
                "exact": random.choice([True, False]),
                "override_broad": True,
                "override_phrase": True,
                "override_exact": True,
            },
            params={"batch_size": 25}
        )

        if response.status_code == 201:
            result = response.json()
            total_created += result.get("processed", 0)
            print(
                f"  Created batch {i//batch_size + 1}: {result.get('created', 0)} new, {result.get('existing', 0)} existing")
        else:
            print_error(f"Failed to create keyword batch: {response.text}")

    print_success(f"Created/processed {total_created} keywords")
    return total_created


def import_demo_data(size: str = "medium"):
    """Import demo data of specified size"""
    if size not in DATA_SIZES:
        print_error(
            f"Invalid size. Choose from: {', '.join(DATA_SIZES.keys())}")
        return False

    config = DATA_SIZES[size]
    print_header(f"Importing {size.upper()} Demo Data")

    print_info(f"Configuration:")
    print(f"  Companies: {config['companies']}")
    print(f"  Campaigns per company: {config['campaigns_per_company']}")
    print(f"  Ad groups per campaign: {config['adgroups_per_campaign']}")
    print(f"  Keywords: {config['keywords']}")
    print(f"  Avg relations per keyword: {config['relations_per_keyword']}")

    total_campaigns = config['companies'] * config['campaigns_per_company']
    total_adgroups = total_campaigns * config['adgroups_per_campaign']
    print(f"\n  Total campaigns: {total_campaigns}")
    print(f"  Total ad groups: {total_adgroups}")
    print()

    try:
        start_time = datetime.now()

        # Create entities
        company_ids = create_companies(config['companies'])
        campaign_ids = create_campaigns(
            company_ids, config['campaigns_per_company'])
        adgroup_ids = create_adgroups(
            campaign_ids, config['adgroups_per_campaign'])

        # Generate and create keywords
        keywords = generate_keywords(config['keywords'])
        total_keywords = create_keywords_with_relations(
            keywords,
            company_ids,
            campaign_ids,
            adgroup_ids,
            config['relations_per_keyword']
        )

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print_header("Import Complete!")
        print_success(f"Import completed in {duration:.2f} seconds")
        print_info(f"Created:")
        print(f"  • {len(company_ids)} companies")
        print(f"  • {len(campaign_ids)} campaigns")
        print(f"  • {len(adgroup_ids)} ad groups")
        print(f"  • {total_keywords} keywords")

        return True

    except Exception as e:
        print_error(f"Import failed: {str(e)}")
        return False


def fetch_all_ids(endpoint: str, max_items: int = 100000) -> List[int]:
    """Fetch all IDs for an endpoint using pagination"""
    all_ids = []
    page = 1
    page_size = 100

    while True:
        response = requests.get(
            f"{API_BASE_URL}/{endpoint}",
            params={"page": page, "page_size": page_size,
                    "only_attached": False}
        )

        if response.status_code != 200:
            print_error(f"Failed to fetch {endpoint}: {response.text}")
            break

        data = response.json()
        objects = data.get("objects", [])

        if not objects:
            break

        all_ids.extend([obj["id"] for obj in objects])

        # Check if we've fetched everything
        total_pages = data.get("total_pages", 1)
        if page >= total_pages or len(all_ids) >= max_items:
            break

        page += 1

    return all_ids


def cleanup_demo_data():
    """Remove all demo data using pagination to fetch everything"""
    print_header("Cleaning Up Demo Data")
    print_warning("This will delete ALL data for the demo user!")

    confirm = input("Are you sure? Type 'yes' to confirm: ")
    if confirm.lower() != 'yes':
        print_info("Cleanup cancelled")
        return False

    try:
        # Get all data to delete with pagination
        print_info("Fetching all entities (this may take a moment)...")

        keyword_ids = fetch_all_ids("keywords")
        adgroup_ids = fetch_all_ids("ad_groups")
        campaign_ids = fetch_all_ids("ad_campaigns")
        company_ids = fetch_all_ids("companies")

        print_info(f"Found:")
        print(f"  • {len(company_ids)} companies")
        print(f"  • {len(campaign_ids)} campaigns")
        print(f"  • {len(adgroup_ids)} ad groups")
        print(f"  • {len(keyword_ids)} keywords")

        total_deleted = 0

        # Delete in reverse order (keywords first to handle relations)
        if keyword_ids:
            print_info(f"Deleting {len(keyword_ids)} keywords in batches...")
            # Delete in batches of 500
            for i in range(0, len(keyword_ids), 500):
                batch = keyword_ids[i:i + 500]
                response = requests.post(
                    f"{API_BASE_URL}/keywords/bulk/delete",
                    json={"ids": batch},
                    params={"batch_size": 50}
                )
                if response.status_code == 200:
                    deleted = response.json()['deleted']
                    total_deleted += deleted
                    print(f"  Deleted {deleted} keywords (batch {i//500 + 1})")
                else:
                    print_error(
                        f"Failed to delete keyword batch: {response.text}")
            print_success(f"Deleted all {len(keyword_ids)} keywords")

        if adgroup_ids:
            print_info(f"Deleting {len(adgroup_ids)} ad groups in batches...")
            for i in range(0, len(adgroup_ids), 500):
                batch = adgroup_ids[i:i + 500]
                response = requests.post(
                    f"{API_BASE_URL}/ad_groups/bulk/delete",
                    json={"ids": batch},
                    params={"batch_size": 50}
                )
                if response.status_code == 200:
                    deleted = response.json()['deleted']
                    total_deleted += deleted
                    print(
                        f"  Deleted {deleted} ad groups (batch {i//500 + 1})")
                else:
                    print_error(
                        f"Failed to delete ad group batch: {response.text}")
            print_success(f"Deleted all {len(adgroup_ids)} ad groups")

        if campaign_ids:
            print_info(f"Deleting {len(campaign_ids)} campaigns in batches...")
            for i in range(0, len(campaign_ids), 500):
                batch = campaign_ids[i:i + 500]
                response = requests.post(
                    f"{API_BASE_URL}/ad_campaigns/bulk/delete",
                    json={"ids": batch},
                    params={"batch_size": 50}
                )
                if response.status_code == 200:
                    deleted = response.json()['deleted']
                    total_deleted += deleted
                    print(
                        f"  Deleted {deleted} campaigns (batch {i//500 + 1})")
                else:
                    print_error(
                        f"Failed to delete campaign batch: {response.text}")
            print_success(f"Deleted all {len(campaign_ids)} campaigns")

        if company_ids:
            print_info(f"Deleting {len(company_ids)} companies in batches...")
            for i in range(0, len(company_ids), 500):
                batch = company_ids[i:i + 500]
                response = requests.post(
                    f"{API_BASE_URL}/companies/bulk/delete",
                    json={"ids": batch},
                    params={"batch_size": 50}
                )
                if response.status_code == 200:
                    deleted = response.json()['deleted']
                    total_deleted += deleted
                    print(
                        f"  Deleted {deleted} companies (batch {i//500 + 1})")
                else:
                    print_error(
                        f"Failed to delete company batch: {response.text}")
            print_success(f"Deleted all {len(company_ids)} companies")

        print_header("Cleanup Complete!")
        print_success(
            f"All demo data has been removed ({total_deleted} total records)")

        # Verify cleanup
        print_info("Verifying cleanup...")
        verify_response = requests.get(
            f"{API_BASE_URL}/keywords", params={"page_size": 1, "only_attached": False})
        if verify_response.status_code == 200:
            remaining = verify_response.json().get("total", 0)
            if remaining == 0:
                print_success("✓ Cleanup verified - no data remaining")
            else:
                print_warning(
                    f"⚠ {remaining} keywords still remain - run cleanup again if needed")

        return True

    except Exception as e:
        print_error(f"Cleanup failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def verify_data():
    """Verify the imported data"""
    print_header("Verifying Demo Data")

    try:
        # Fetch statistics
        companies_response = requests.get(
            f"{API_BASE_URL}/companies", params={"page_size": 1})
        campaigns_response = requests.get(
            f"{API_BASE_URL}/ad_campaigns", params={"page_size": 1})
        adgroups_response = requests.get(
            f"{API_BASE_URL}/ad_groups", params={"page_size": 1})
        keywords_response = requests.get(
            f"{API_BASE_URL}/keywords", params={"page_size": 1, "only_attached": False})

        companies_data = companies_response.json()
        campaigns_data = campaigns_response.json()
        adgroups_data = adgroups_response.json()
        keywords_data = keywords_response.json()

        print_success("Data Statistics:")
        print(f"  • Companies: {companies_data.get('total', 0)}")
        print(f"  • Campaigns: {campaigns_data.get('total', 0)}")
        print(f"  • Ad Groups: {adgroups_data.get('total', 0)}")
        print(f"  • Keywords: {keywords_data.get('total', 0)}")

        # Test performance
        print_info("\nTesting list performance...")
        start_time = datetime.now()
        response = requests.get(
            f"{API_BASE_URL}/keywords", params={"page_size": 50})
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds() * 1000

        print_success(f"Keywords list endpoint: {duration:.2f}ms")

        if duration < 500:
            print_success("Performance: Excellent! ⚡")
        elif duration < 1000:
            print_info("Performance: Good 👍")
        else:
            print_warning("Performance: Could be improved 🤔")

        return True

    except Exception as e:
        print_error(f"Verification failed: {str(e)}")
        return False


def show_stats():
    """Show detailed statistics about the data"""
    print_header("Demo Data Statistics")

    try:
        # Fetch with pagination to get accurate counts
        companies_response = requests.get(
            f"{API_BASE_URL}/companies", params={"page_size": 100})
        campaigns_response = requests.get(
            f"{API_BASE_URL}/ad_campaigns", params={"page_size": 100})
        adgroups_response = requests.get(
            f"{API_BASE_URL}/ad_groups", params={"page_size": 100})
        keywords_response = requests.get(
            f"{API_BASE_URL}/keywords", params={"page_size": 100, "only_attached": False})

        companies = companies_response.json().get("objects", [])
        campaigns = campaigns_response.json().get("objects", [])
        adgroups = adgroups_response.json().get("objects", [])
        keywords = keywords_response.json().get("objects", [])

        active_companies = sum(1 for c in companies if c.get("is_active"))
        active_campaigns = sum(1 for c in campaigns if c.get("is_active"))
        active_adgroups = sum(1 for a in adgroups if a.get("is_active"))

        print(f"{Colors.BOLD}Entities:{Colors.END}")
        print(
            f"  Companies: {len(companies)} total ({active_companies} active)")
        print(
            f"  Campaigns: {len(campaigns)} total ({active_campaigns} active)")
        print(f"  Ad Groups: {len(adgroups)} total ({active_adgroups} active)")
        print(f"  Keywords: {len(keywords)} total")

        print(f"\n{Colors.BOLD}Ratios:{Colors.END}")
        if len(companies) > 0:
            print(
                f"  Campaigns per company: {len(campaigns) / len(companies):.1f}")
        if len(campaigns) > 0:
            print(
                f"  Ad groups per campaign: {len(adgroups) / len(campaigns):.1f}")

        return True

    except Exception as e:
        print_error(f"Failed to get statistics: {str(e)}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="KPlanner Demo Data Management")
    subparsers = parser.add_subparsers(
        dest="command", help="Command to execute")

    # Import command
    import_parser = subparsers.add_parser("import", help="Import demo data")
    import_parser.add_argument(
        "size",
        choices=["small", "medium", "large", "huge"],
        nargs="?",
        default="medium",
        help="Size of demo data to import (default: medium)"
    )

    # Cleanup command
    subparsers.add_parser("cleanup", help="Remove all demo data")

    # Verify command
    subparsers.add_parser("verify", help="Verify imported data")

    # Stats command
    subparsers.add_parser("stats", help="Show detailed statistics")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    # Check API availability
    try:
        response = requests.get(API_BASE_URL)
        if response.status_code != 200:
            print_error(f"API not available at {API_BASE_URL}")
            return
    except Exception as e:
        print_error(f"Cannot connect to API at {API_BASE_URL}: {str(e)}")
        return

    # Execute command
    if args.command == "import":
        import_demo_data(args.size)
    elif args.command == "cleanup":
        cleanup_demo_data()
    elif args.command == "verify":
        verify_data()
    elif args.command == "stats":
        show_stats()


if __name__ == "__main__":
    main()
