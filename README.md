# KPlanner API

A FastAPI-based advertising campaign management system with Clerk authentication.

## Features

- **Clerk Authentication**: Secure authentication using Clerk SDK
- **User-scoped Data**: All resources are automatically scoped to the authenticated user
- **Campaign Management**: Create and manage companies, ad campaigns, and ad groups
- **RESTful API**: Full CRUD operations with proper HTTP status codes

## Setup

### 1. Environment Configuration

Copy the example environment file:
```bash
cp .env.example .env
```

Edit `.env` and configure:
- Database credentials (MySQL)
- Clerk secret key from your [Clerk Dashboard](https://dashboard.clerk.com)

### 2. Install Dependencies

```bash
cd /Users/antonvakulov/kplanner
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Database Setup

Run the schema to create tables:
```bash
mysql -u your_user -p your_database < schema.sql
```

### 4. Run the Server

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

## API Documentation

Once the server is running, visit:
- **Interactive docs**: http://localhost:8000/docs
- **Alternative docs**: http://localhost:8000/redoc

## Authentication

### Development Mode (Default)

For local development and testing, you can enable `DEV_MODE`:

```bash
# In .env file
DEV_MODE=true
```

When `DEV_MODE=true`:
- ✅ No authentication required
- ✅ All requests use demo user: `clerk_demo_user`
- ✅ No need for Clerk secret key
- ✅ Perfect for testing and development

### Production Mode

For production, disable dev mode and provide your Clerk secret key:

```bash
# In .env file
DEV_MODE=false
CLERK_SECRET_KEY=sk_live_xxxxx
```

When `DEV_MODE=false`:
- 🔒 All endpoints require Clerk authentication
- 🔒 Must include session token in requests
- 🔒 User data is scoped per authenticated user

### How to Authenticate (Production Mode)

Include your Clerk session token in the request headers:

```bash
Authorization: Bearer <your_clerk_session_token>
```

Or include the `__clerk_db_jwt` cookie from your frontend Clerk session.

### Getting a Session Token

From your frontend (using Clerk's JavaScript SDK):
```javascript
const token = await window.Clerk.session.getToken();
// Use this token in the Authorization header
```

## API Endpoints

### Companies

- `POST /companies` - Create a new company
  ```json
  {
    "title": "My Company"
  }
  ```
  
  **Dev Mode Example (no auth required):**
  ```bash
  curl -X POST "http://localhost:8000/companies" \
    -H "Content-Type: application/json" \
    -d '{"title": "My Company"}'
  ```
  
  **Production Mode Example (auth required):**
  ```bash
  curl -X POST "http://localhost:8000/companies" \
    -H "Authorization: Bearer YOUR_TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"title": "My Company"}'
  ```

- `GET /companies` - List all companies for authenticated user
- `GET /companies/{company_id}` - Get specific company

### Ad Campaigns

- `POST /ad_campaigns` - Create a new ad campaign
  ```json
  {
    "title": "Summer Campaign",
    "company_id": 1  // optional
  }
  ```
- `GET /ad_campaigns` - List all campaigns (optional query: `?company_id=1`)
- `GET /ad_campaigns/{campaign_id}` - Get specific campaign

### Ad Groups

- `POST /ad_groups` - Create a new ad group
  ```json
  {
    "title": "Product Keywords",
    "ad_campaign_id": 1  // optional
  }
  ```
- `GET /ad_groups` - List all ad groups (optional query: `?ad_campaign_id=1`)
- `GET /ad_groups/{ad_group_id}` - Get specific ad group

### Keywords (Bulk)

- `POST /keywords/bulk` - **Bulk create keywords with optional associations**
  ```json
  {
    "keywords": ["keyword1", "keyword2", "keyword3"],
    "match_types": {
      "broad": true,
      "phrase": true,
      "exact": false,
      "neg_broad": false,
      "neg_phrase": false,
      "neg_exact": false
    },
    "company_ids": [1, 2],        // optional
    "ad_campaign_ids": [5],       // optional
    "ad_group_ids": [10, 11]      // optional
  }
  ```
  
  **Features:**
  - Create multiple keywords at once
  - Automatically deduplicate existing keywords
  - Optionally associate with companies, campaigns, and/or ad groups
  - Apply match types (broad, phrase, exact, negative variants) to all associations
  
  **See [BULK_KEYWORDS_API.md](BULK_KEYWORDS_API.md) for detailed examples and documentation.**

- `PUT /keywords/bulk/relations` - **Bulk update keyword relations**
  ```json
  {
    "keyword_ids": [1, 2, 3],
    "match_types": {
      "broad": true,
      "phrase": true,
      "exact": false
    },
    "company_ids": [1],           // optional
    "ad_campaign_ids": [5],       // optional
    "ad_group_ids": [10, 11],     // optional
    "remove_associations": false  // false=add/update, true=remove
  }
  ```
  
  **Features:**
  - Update associations for multiple keywords at once
  - Add new associations or update existing ones
  - Change match types for existing associations
  - Remove associations (set `remove_associations: true`)
  
  **See [BULK_UPDATE_KEYWORDS_API.md](BULK_UPDATE_KEYWORDS_API.md) for detailed examples.**

- `GET /keywords` - List all keywords for authenticated user
- `GET /keywords/{keyword_id}` - Get specific keyword

## Project Structure

```
kplanner/
├── main.py           # FastAPI application and routes
├── models.py         # SQLAlchemy database models
├── schemas.py        # Pydantic request/response schemas
├── database.py       # Database configuration
├── schema.sql        # MySQL database schema
├── requirements.txt  # Python dependencies
├── .env             # Environment variables (not in git)
└── .env.example     # Example environment configuration
```

## Security

- ✅ All endpoints require Clerk authentication
- ✅ Users can only access their own data
- ✅ Foreign key validation ensures data integrity
- ✅ CORS configured (update for production)

## Development

The API automatically validates:
- User authentication via Clerk
- Company ownership before creating campaigns
- Campaign ownership before creating ad groups
- User can only view/modify their own resources