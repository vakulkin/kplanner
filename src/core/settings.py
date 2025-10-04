import os
from clerk_backend_api import Clerk


# App settings
TITLE = "KPlanner API"
VERSION = "1.0.0"

# Check for dev mode
DEV_MODE = os.getenv("DEV_MODE", "false").lower() == "true"
DEMO_USER_ID = "clerk_demo_user"

# Active entity limits
COMPANY_ACTIVE_LIMIT = 3
AD_CAMPAIGN_ACTIVE_LIMIT = 5
AD_GROUP_ACTIVE_LIMIT = 7

# Pagination and batch processing constants
DEFAULT_PAGE = 1
PAGE_SIZE = 50  # Default page size
MAX_PAGE_SIZE = 100  # Maximum page size
BATCH_SIZE = 25
MAX_KEYWORDS_PER_REQUEST = 100

# Initialize Clerk SDK (only if not in dev mode)
clerk_sdk = None
if not DEV_MODE:
    clerk_secret_key = os.getenv("CLERK_SECRET_KEY")
    if not clerk_secret_key:
        raise ValueError("CLERK_SECRET_KEY environment variable is required when DEV_MODE is not enabled")
    clerk_sdk = Clerk(bearer_auth=clerk_secret_key)

