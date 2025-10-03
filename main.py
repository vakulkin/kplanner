from fastapi import FastAPI, Depends, HTTPException, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import Optional
from datetime import datetime
import os
import httpx
from clerk_backend_api import Clerk
from clerk_backend_api.security.types import AuthenticateRequestOptions
import models
from database import engine, get_db
from schemas import (
    Company, AdCampaign, AdGroup, Keyword,
    CompanyCreate, AdCampaignCreate, AdGroupCreate, KeywordCreate,
    BulkKeywordCreate, BulkKeywordUpdateRelations, BulkKeywordCreateRelations,
    BulkDeleteRequest,
    SingleObjectResponse, MultipleObjectsResponse,
    BulkDeleteResponse, BulkKeywordCreateResponse, 
    BulkRelationUpdateResponse, BulkRelationCreateResponse,
)
import math

# Create tables (skip if in testing mode)
if not os.getenv("TESTING"):
    models.Base.metadata.create_all(bind=engine)

# Initialize FastAPI app
app = FastAPI(
    title="KPlanner API",
    version="1.0.0",
    # Exclude None values from all JSON responses
    response_model_exclude_none=True
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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


# Dependency to get authenticated user ID from Clerk
async def get_current_user_id(request: Request) -> str:
    if DEV_MODE:
        return DEMO_USER_ID
    
    # Production mode: authenticate with Clerk
    # Convert FastAPI request to httpx request for Clerk SDK
    httpx_request = httpx.Request(
        method=request.method,
        url=str(request.url),
        headers=request.headers,
    )
    
    try:
        request_state = clerk_sdk.authenticate_request(
            httpx_request,
            AuthenticateRequestOptions()
        )
        
        if not request_state.is_signed_in:
            raise HTTPException(
                status_code=401,
                detail=f"Authentication failed: {request_state.reason or 'Not signed in'}"
            )
        
        # Extract user_id from the token payload
        user_id = request_state.payload.get("sub")
        if not user_id:
            raise HTTPException(
                status_code=401,
                detail="User ID not found in token"
            )
        
        return user_id
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(status_code=401, detail=f"Authentication error: {str(e)}")


# Helper function for pagination
def paginate_query(query, page: int = DEFAULT_PAGE, page_size: int = PAGE_SIZE):
    # Validate and limit page_size
    page_size = min(max(1, page_size), MAX_PAGE_SIZE)  # Between 1 and MAX_PAGE_SIZE
    page = max(DEFAULT_PAGE, page)  # At least DEFAULT_PAGE
    
    # Get total count
    total_count = query.count()
    
    # Calculate total pages
    total_pages = math.ceil(total_count / page_size) if total_count > 0 else 1
    
    # Apply pagination
    offset = (page - 1) * page_size
    items = query.offset(offset).limit(page_size).all()
    
    return items, total_count, total_pages


# Helper function to apply common date filters
def _apply_date_filters(query, model_class, created_after, created_before, updated_after, updated_before):
    """Apply common date filters to a query."""
    if created_after:
        query = query.filter(model_class.created >= created_after)
    if created_before:
        query = query.filter(model_class.created <= created_before)
    if updated_after:
        query = query.filter(model_class.updated >= updated_after)
    if updated_before:
        query = query.filter(model_class.updated <= updated_before)
    return query


# Helper function to apply sorting
def _apply_sorting(query, model_class, sort_by, sort_order, sort_fields_map, default_field="created"):
    """Apply sorting to a query based on field mapping."""
    sort_field = sort_by.lower() if sort_by else default_field
    sort_direction = sort_order.lower() if sort_order else "desc"
    
    if sort_field in sort_fields_map:
        order_column = sort_fields_map[sort_field]
        if sort_direction == "asc":
            query = query.order_by(order_column.asc())
        else:
            query = query.order_by(order_column.desc())
    else:
        # Default sorting
        default_column = sort_fields_map.get(default_field, model_class.created)
        query = query.order_by(default_column.desc())
    
    return query


# Helper function to generate common metadata structure
def _generate_metadata(entity_type, parent_field=None, additional_sort_fields=None):
    """Generate common filter and sorting metadata for entity endpoints."""
    filters = {}
    
    # Add parent filter if applicable
    if parent_field:
        filters[parent_field] = {
            "type": "integer",
            "description": f"Filter by parent {parent_field.replace('_', ' ')}"
        }
    
    # Add search filter
    filters["search"] = {
        "type": "string",
        "description": f"Search by {entity_type} title (case-insensitive, partial match)"
    }
    
    # Add is_active filter
    filters["is_active"] = {
        "type": "boolean",
        "description": "Filter by is_active status",
        "available_values": [True, False]
    }
    
    # Add common date filters
    date_filters = {
        "created_after": {
            "type": "datetime",
            "description": "Filter by created date (after)",
            "format": "ISO 8601 (YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD)"
        },
        "created_before": {
            "type": "datetime",
            "description": "Filter by created date (before)",
            "format": "ISO 8601 (YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD)"
        },
        "updated_after": {
            "type": "datetime",
            "description": "Filter by updated date (after)",
            "format": "ISO 8601 (YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD)"
        },
        "updated_before": {
            "type": "datetime",
            "description": "Filter by updated date (before)",
            "format": "ISO 8601 (YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD)"
        }
    }
    filters.update(date_filters)
    
    # Generate sorting metadata
    sort_values = ["id", "title", "is_active", "created", "updated"]
    if parent_field:
        sort_values.insert(-2, parent_field)  # Insert before 'created'
    if additional_sort_fields:
        sort_values.extend(additional_sort_fields)
    
    sorting = {
        "sort_by": {
            "type": "string",
            "description": "Field to sort by",
            "available_values": sort_values,
            "default": "created"
        },
        "sort_order": {
            "type": "string",
            "description": "Sort direction",
            "available_values": ["asc", "desc"],
            "default": "desc"
        }
    }
    
    return filters, sorting


# Helper function to get sort fields map for standard entities
def _get_entity_sort_fields(model_class, parent_field: str = None):
    """Generate sort fields map for entities with optional parent field."""
    fields = {
        "id": model_class.id,
        "title": model_class.title,
        "is_active": model_class.is_active,
        "created": model_class.created,
        "updated": model_class.updated
    }
    
    if parent_field:
        fields[parent_field] = getattr(model_class, parent_field)
    
    return fields


# Helper functions for generating API metadata
def _get_companies_metadata():
    """Get metadata for companies endpoint including available filters and sorting."""
    return _generate_metadata("company")


def _get_ad_campaigns_metadata():
    """Get metadata for ad campaigns endpoint including available filters and sorting."""
    return _generate_metadata("campaign", parent_field="company_id")


def _get_ad_groups_metadata():
    """Get metadata for ad groups endpoint including available filters and sorting."""
    return _generate_metadata("ad group", parent_field="ad_campaign_id")


def _get_keywords_metadata():
    """Get metadata for keywords endpoint including available filters and sorting."""
    filters = {
        "only_attached": {
            "type": "boolean",
            "description": "Show only keywords attached to at least one entity",
            "available_values": [True, False],
            "default": False
        },
        "search": {
            "type": "string",
            "description": "Search by keyword text (case-insensitive, partial match)"
        },
        "created_after": {
            "type": "datetime",
            "description": "Filter by created date (after)",
            "format": "ISO 8601 (YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD)"
        },
        "created_before": {
            "type": "datetime",
            "description": "Filter by created date (before)",
            "format": "ISO 8601 (YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD)"
        },
        "updated_after": {
            "type": "datetime",
            "description": "Filter by updated date (after)",
            "format": "ISO 8601 (YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD)"
        },
        "updated_before": {
            "type": "datetime",
            "description": "Filter by updated date (before)",
            "format": "ISO 8601 (YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD)"
        },
        "has_broad": {
            "type": "boolean",
            "description": "Filter keywords with at least one broad match relation",
            "available_values": [True, False]
        },
        "has_phrase": {
            "type": "boolean",
            "description": "Filter keywords with at least one phrase match relation",
            "available_values": [True, False]
        },
        "has_exact": {
            "type": "boolean",
            "description": "Filter keywords with at least one exact match relation",
            "available_values": [True, False]
        },
        "has_neg_broad": {
            "type": "boolean",
            "description": "Filter keywords with at least one negative broad match relation",
            "available_values": [True, False]
        },
        "has_neg_phrase": {
            "type": "boolean",
            "description": "Filter keywords with at least one negative phrase match relation",
            "available_values": [True, False]
        },
        "has_neg_exact": {
            "type": "boolean",
            "description": "Filter keywords with at least one negative exact match relation",
            "available_values": [True, False]
        }
    }
    
    sorting = {
        "sort_by": {
            "type": "string",
            "description": "Primary sort field",
            "available_values": ["id", "keyword", "created", "updated", "has_broad", "has_phrase", "has_exact", "has_neg_broad", "has_neg_phrase", "has_neg_exact"],
            "default": "created"
        },
        "sort_order": {
            "type": "string",
            "description": "Primary sort direction",
            "available_values": ["asc", "desc"],
            "default": "desc"
        },
        "sort_by_2": {
            "type": "string",
            "description": "Secondary sort field (optional)",
            "available_values": ["id", "keyword", "created", "updated", "has_broad", "has_phrase", "has_exact", "has_neg_broad", "has_neg_phrase", "has_neg_exact"]
        },
        "sort_order_2": {
            "type": "string",
            "description": "Secondary sort direction",
            "available_values": ["asc", "desc"]
        },
        "sort_by_3": {
            "type": "string",
            "description": "Tertiary sort field (optional)",
            "available_values": ["id", "keyword", "created", "updated", "has_broad", "has_phrase", "has_exact", "has_neg_broad", "has_neg_phrase", "has_neg_exact"]
        },
        "sort_order_3": {
            "type": "string",
            "description": "Tertiary sort direction",
            "available_values": ["asc", "desc"]
        }
    }
    
    special_features = {
        "multi_level_sorting": {
            "description": "Supports up to 3 levels of sorting (primary, secondary, tertiary)",
            "max_levels": 3
        },
        "match_type_sorting": {
            "description": "Can sort by match type presence (has_broad, has_phrase, etc.)",
            "note": "When sorting desc, keywords WITH the match type appear first (value 1). When sorting asc, keywords WITHOUT the match type appear first (value 0)."
        }
    }
    
    return filters, sorting, special_features


# Helper function for batch processing
def process_in_batches(items: list, batch_size: int = BATCH_SIZE):
    batch_size = max(1, batch_size)  # Ensure at least 1
    
    for i in range(0, len(items), batch_size):
        yield items[i:i + batch_size]


def _bulk_delete_with_batches(
    db: Session,
    user_id: str,
    ids: list[int],
    model_class,
    ownership_field: str,
    message_template: str,
    batch_size: int = BATCH_SIZE
) -> BulkDeleteResponse:
    """Generic helper for bulk delete operations with batching and ownership validation."""
    if not ids:
        raise HTTPException(status_code=400, detail="ids is required")
    
    deleted_count = 0
    batches_processed = 0
    
    # Process deletions in batches
    for id_batch in process_in_batches(ids, batch_size=batch_size):
        # Filter by ownership directly - works for all entities and relations!
        batch_deleted = db.query(model_class).filter(
            getattr(model_class, 'id').in_(id_batch),
            getattr(model_class, ownership_field) == user_id
        ).delete(synchronize_session=False)
        
        deleted_count += batch_deleted
        db.commit()
        batches_processed += 1
    
    return BulkDeleteResponse(
        message=message_template.format(deleted_count),
        deleted=deleted_count,
        processed=deleted_count,
        requested=len(ids),
        batches_processed=batches_processed,
        batch_size=batch_size
    )


def _check_active_limit(
    db: Session,
    user_id: str,
    model_class,
    limit: int,
    entity_name: str,
    exclude_id: int = None
) -> tuple[bool, str]:
    """Check if activating an entity would exceed the limit.
    
    Returns (can_activate, message)
    """
    query = db.query(model_class).filter(
        getattr(model_class, 'clerk_user_id') == user_id,
        model_class.is_active == True
    )
    
    # Exclude current entity when checking (for toggle/update operations)
    if exclude_id:
        query = query.filter(model_class.id != exclude_id)
    
    active_count = query.count()
    
    if active_count >= limit:
        return False, f"Maximum {limit} active {entity_name}s allowed. Please deactivate another {entity_name} first."
    
    return True, ""


def _toggle_entity_active(
    db: Session,
    entity_id: int,
    user_id: str,
    model_class,
    schema_class,
    entity_name: str,
    active_limit: int = None
) -> SingleObjectResponse:
    """Generic helper for toggling is_active status of entities."""
    entity = db.query(model_class).filter(
        model_class.id == entity_id,
        getattr(model_class, 'clerk_user_id') == user_id
    ).first()
    if not entity:
        raise HTTPException(status_code=404, detail=f"{entity_name.capitalize()} not found")
    
    # If trying to activate, check limit
    if not entity.is_active and active_limit:
        can_activate, limit_message = _check_active_limit(
            db, user_id, model_class, active_limit, entity_name, exclude_id=entity_id
        )
        if not can_activate:
            return SingleObjectResponse(
                message=limit_message,
                object=schema_class.model_validate(entity)
            )
    
    entity.is_active = not entity.is_active
    db.commit()
    db.refresh(entity)
    
    return SingleObjectResponse(
        message=f"{entity_name.capitalize()} {'activated' if entity.is_active else 'deactivated'} successfully",
        object=schema_class.model_validate(entity)
    )


def _validate_parent_entity(db: Session, user_id: str, parent_id: int, parent_model, parent_name: str):
    """Validate that a parent entity exists and belongs to the user."""
    parent = db.query(parent_model).filter(
        parent_model.id == parent_id,
        getattr(parent_model, 'clerk_user_id') == user_id
    ).first()
    if not parent:
        raise HTTPException(status_code=404, detail=f"{parent_name.capitalize()} not found")
    return parent


def _get_entity_by_id(
    db: Session,
    user_id: str,
    entity_id: int,
    model_class,
    schema_class,
    entity_name: str
) -> SingleObjectResponse:
    """Generic helper for retrieving a single entity by ID."""
    entity = db.query(model_class).filter(
        model_class.id == entity_id,
        getattr(model_class, 'clerk_user_id') == user_id
    ).first()
    if not entity:
        raise HTTPException(status_code=404, detail=f"{entity_name.capitalize()} not found")
    return SingleObjectResponse(
        message=f"{entity_name.capitalize()} retrieved successfully",
        object=schema_class.model_validate(entity)
    )


def _update_simple_entity(
    db: Session,
    user_id: str,
    entity_id: int,
    entity_update,
    model_class,
    schema_class,
    entity_name: str,
    update_fields: dict
) -> SingleObjectResponse:
    """Generic helper for updating entities without active limits or parent validation."""
    entity = db.query(model_class).filter(
        model_class.id == entity_id,
        getattr(model_class, 'clerk_user_id') == user_id
    ).first()
    if not entity:
        raise HTTPException(status_code=404, detail=f"{entity_name.capitalize()} not found")
    
    # Update fields dynamically
    for field_name, field_value in update_fields.items():
        setattr(entity, field_name, field_value)
    
    db.commit()
    db.refresh(entity)
    
    return SingleObjectResponse(
        message=f"{entity_name.capitalize()} updated successfully",
        object=schema_class.model_validate(entity)
    )


def _list_entities_with_filters(
    db: Session,
    user_id: str,
    model_class,
    schema_class,
    entity_name: str,
    entity_name_plural: str,
    page: int,
    page_size: int,
    search: Optional[str],
    is_active: Optional[bool],
    created_after: Optional[datetime],
    created_before: Optional[datetime],
    updated_after: Optional[datetime],
    updated_before: Optional[datetime],
    sort_by: str,
    sort_order: str,
    sort_fields_map: dict,
    metadata_func,
    parent_filter: Optional[tuple] = None
) -> MultipleObjectsResponse:
    """Generic helper for listing entities with filtering, sorting, and pagination.
    
    Args:
        parent_filter: Optional tuple of (field_name, field_value) for parent filtering
        entity_name_plural: Plural form of entity name for messages
    """
    # Build base query with user filter
    if parent_filter:
        field_name, field_value = parent_filter
        if field_value is not None:
            query = db.query(model_class).filter(
                getattr(model_class, 'clerk_user_id') == user_id,
                getattr(model_class, field_name) == field_value
            )
        else:
            query = db.query(model_class).filter(getattr(model_class, 'clerk_user_id') == user_id)
    else:
        query = db.query(model_class).filter(getattr(model_class, 'clerk_user_id') == user_id)
    
    # Add search filter if provided
    if search:
        query = query.filter(model_class.title.ilike(f"%{search}%"))
    
    # Add is_active filter
    if is_active is not None:
        query = query.filter(model_class.is_active == is_active)
    
    # Apply date filters
    query = _apply_date_filters(query, model_class, created_after, created_before, updated_after, updated_before)
    
    # Apply sorting
    query = _apply_sorting(query, model_class, sort_by, sort_order, sort_fields_map)
    
    # Paginate
    entities, total_count, total_pages = paginate_query(query, page, page_size)
    
    # Get metadata
    filters, sorting = metadata_func()
    
    # Build response
    return MultipleObjectsResponse(
        message=f"Retrieved {total_count} {entity_name_plural}",
        objects=[schema_class.model_validate(e) for e in entities],
        pagination={
            "total": total_count,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages
        },
        filters=filters,
        sorting=sorting
    )


def _create_entity_with_limit(
    db: Session,
    user_id: str,
    entity_data,
    model_class,
    schema_class,
    entity_name: str,
    active_limit: int = None,
    parent_field: str = None,
    parent_model = None,
    parent_name: str = None,
    **extra_fields
) -> SingleObjectResponse:
    """Generic helper for creating entities with active limit checking and optional parent validation."""
    # Validate parent if required
    if parent_field and parent_model:
        parent_id = getattr(entity_data, parent_field)
        _validate_parent_entity(db, user_id, parent_id, parent_model, parent_name)
    
    # Check if trying to create as active and limit is reached
    is_active = entity_data.is_active
    limit_message = None
    
    if is_active and active_limit:
        can_activate, limit_msg = _check_active_limit(
            db, user_id, model_class, active_limit, entity_name
        )
        if not can_activate:
            is_active = False
            limit_message = limit_msg
    
    # Build entity data
    entity_dict = {
        'title': entity_data.title,
        'is_active': is_active,
        'clerk_user_id': user_id,
        **extra_fields
    }
    
    # Add parent field if provided
    if parent_field:
        entity_dict[parent_field] = getattr(entity_data, parent_field)
    
    db_entity = model_class(**entity_dict)
    db.add(db_entity)
    db.commit()
    db.refresh(db_entity)
    
    message = f"{entity_name.capitalize()} created successfully"
    if limit_message:
        message = f"{entity_name.capitalize()} created as inactive. {limit_message}"
    
    return SingleObjectResponse(
        message=message,
        object=schema_class.model_validate(db_entity)
    )


def _update_entity_with_limit(
    db: Session,
    user_id: str,
    entity_id: int,
    entity_update,
    model_class,
    schema_class,
    entity_name: str,
    active_limit: int = None,
    parent_field: str = None,
    parent_model = None,
    parent_name: str = None
) -> SingleObjectResponse:
    """Generic helper for updating entities with active limit checking and optional parent validation."""
    # Get existing entity
    entity = db.query(model_class).filter(
        model_class.id == entity_id,
        getattr(model_class, 'clerk_user_id') == user_id
    ).first()
    if not entity:
        raise HTTPException(status_code=404, detail=f"{entity_name.capitalize()} not found")
    
    # Validate parent if required
    if parent_field and parent_model:
        parent_id = getattr(entity_update, parent_field)
        _validate_parent_entity(db, user_id, parent_id, parent_model, parent_name)
    
    # Check if trying to activate and limit is reached
    is_active = entity_update.is_active
    limit_message = None
    
    if is_active and not entity.is_active and active_limit:  # Trying to activate
        can_activate, limit_msg = _check_active_limit(
            db, user_id, model_class, active_limit, entity_name, exclude_id=entity_id
        )
        if not can_activate:
            is_active = False
            limit_message = limit_msg
    
    # Update entity
    entity.title = entity_update.title
    entity.is_active = is_active
    if parent_field:
        setattr(entity, parent_field, getattr(entity_update, parent_field))
    
    db.commit()
    db.refresh(entity)
    
    message = f"{entity_name.capitalize()} updated successfully"
    if limit_message:
        message = f"{entity_name.capitalize()} updated but kept inactive. {limit_message}"
    
    return SingleObjectResponse(
        message=message,
        object=schema_class.model_validate(entity)
    )


@app.get("/")
async def root():
    return {
        "message": "Welcome to KPlanner API",
        "mode": "development" if DEV_MODE else "production",
        "demo_user": DEMO_USER_ID if DEV_MODE else None
    }

# Entity configuration for CRUD operations
ENTITY_CONFIGS = {
    "company": {
        "model_class": models.Company,
        "schema_class": Company,
        "create_schema": CompanyCreate,
        "entity_name": "company",
        "entity_name_plural": "companies",
        "active_limit": COMPANY_ACTIVE_LIMIT,
        "id_param": "company_id",
        "parent_field": None,
        "parent_model": None,
        "parent_name": None,
    },
    "campaign": {
        "model_class": models.AdCampaign,
        "schema_class": AdCampaign,
        "create_schema": AdCampaignCreate,
        "entity_name": "campaign",
        "entity_name_plural": "campaigns",
        "active_limit": AD_CAMPAIGN_ACTIVE_LIMIT,
        "id_param": "campaign_id",
        "parent_field": "company_id",
        "parent_model": models.Company,
        "parent_name": "company",
    },
    "ad_group": {
        "model_class": models.AdGroup,
        "schema_class": AdGroup,
        "create_schema": AdGroupCreate,
        "entity_name": "ad group",
        "entity_name_plural": "ad groups",
        "active_limit": AD_GROUP_ACTIVE_LIMIT,
        "id_param": "ad_group_id",
        "parent_field": "ad_campaign_id",
        "parent_model": models.AdCampaign,
        "parent_name": "ad campaign",
    },
}


# Generic endpoint handler functions
def _handle_create_entity(entity_data, db: Session, user_id: str, config: dict):
    """Generic handler for entity creation."""
    return _create_entity_with_limit(
        db=db,
        user_id=user_id,
        entity_data=entity_data,
        model_class=config["model_class"],
        schema_class=config["schema_class"],
        entity_name=config["entity_name"],
        active_limit=config["active_limit"],
        parent_field=config["parent_field"],
        parent_model=config["parent_model"],
        parent_name=config["parent_name"]
    )


def _handle_list_entities(
    db: Session,
    user_id: str,
    config: dict,
    page: int,
    page_size: int,
    search: Optional[str],
    is_active: Optional[bool],
    created_after: Optional[datetime],
    created_before: Optional[datetime],
    updated_after: Optional[datetime],
    updated_before: Optional[datetime],
    sort_by: str,
    sort_order: str,
    metadata_func,
    parent_id: Optional[int] = None
):
    """Generic handler for entity listing."""
    parent_filter = None
    if config["parent_field"] and parent_id is not None:
        parent_filter = (config["parent_field"], parent_id)
    
    return _list_entities_with_filters(
        db=db,
        user_id=user_id,
        model_class=config["model_class"],
        schema_class=config["schema_class"],
        entity_name=config["entity_name"],
        entity_name_plural=config["entity_name_plural"],
        page=page,
        page_size=page_size,
        search=search,
        is_active=is_active,
        created_after=created_after,
        created_before=created_before,
        updated_after=updated_after,
        updated_before=updated_before,
        sort_by=sort_by,
        sort_order=sort_order,
        sort_fields_map=_get_entity_sort_fields(config["model_class"], config["parent_field"]),
        metadata_func=metadata_func,
        parent_filter=parent_filter
    )


def _handle_get_entity(entity_id: int, db: Session, user_id: str, config: dict):
    """Generic handler for getting a single entity."""
    return _get_entity_by_id(
        db=db,
        user_id=user_id,
        entity_id=entity_id,
        model_class=config["model_class"],
        schema_class=config["schema_class"],
        entity_name=config["entity_name"]
    )


def _handle_update_entity(entity_id: int, entity_update, db: Session, user_id: str, config: dict):
    """Generic handler for entity updates."""
    return _update_entity_with_limit(
        db=db,
        user_id=user_id,
        entity_id=entity_id,
        entity_update=entity_update,
        model_class=config["model_class"],
        schema_class=config["schema_class"],
        entity_name=config["entity_name"],
        active_limit=config["active_limit"],
        parent_field=config["parent_field"],
        parent_model=config["parent_model"],
        parent_name=config["parent_name"]
    )


def _handle_toggle_entity(entity_id: int, db: Session, user_id: str, config: dict):
    """Generic handler for toggling entity active status."""
    return _toggle_entity_active(
        db=db,
        entity_id=entity_id,
        user_id=user_id,
        model_class=config["model_class"],
        schema_class=config["schema_class"],
        entity_name=config["entity_name"],
        active_limit=config["active_limit"]
    )


def _handle_bulk_delete(delete_data: BulkDeleteRequest, db: Session, user_id: str, config: dict, batch_size: int):
    """Generic handler for bulk delete operations."""
    return _bulk_delete_with_batches(
        db=db,
        user_id=user_id,
        ids=delete_data.ids,
        model_class=config["model_class"],
        ownership_field="clerk_user_id",
        message_template=f"Deleted {{0}} {config['entity_name_plural']}",
        batch_size=batch_size
    )


# Company endpoints
@app.post("/companies", response_model=SingleObjectResponse, status_code=201)
async def create_company(
    company: CompanyCreate,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Create a new company"""
    return _handle_create_entity(company, db, user_id, ENTITY_CONFIGS["company"])

@app.get("/companies", response_model=MultipleObjectsResponse)
async def list_companies(
    page: int = Query(DEFAULT_PAGE, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE, description=f"Items per page (max {MAX_PAGE_SIZE})"),
    search: Optional[str] = Query(None, description="Search by company title (case-insensitive, partial match)"),
    is_active: Optional[bool] = Query(None, description="Filter by is_active status"),
    created_after: Optional[datetime] = Query(None, description="Filter by created date (after)"),
    created_before: Optional[datetime] = Query(None, description="Filter by created date (before)"),
    updated_after: Optional[datetime] = Query(None, description="Filter by updated date (after)"),
    updated_before: Optional[datetime] = Query(None, description="Filter by updated date (before)"),
    sort_by: Optional[str] = Query("created", description="Sort by field: id, title, is_active, created, updated"),
    sort_order: Optional[str] = Query("desc", description="Sort order: asc or desc"),
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """List all companies for the authenticated user with pagination, filters, and sorting"""
    return _handle_list_entities(
        db, user_id, ENTITY_CONFIGS["company"], page, page_size, search, is_active,
        created_after, created_before, updated_after, updated_before,
        sort_by, sort_order, _get_companies_metadata
    )

@app.get("/companies/{company_id}", response_model=SingleObjectResponse)
async def get_company(
    company_id: int,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Get a specific company by ID"""
    return _handle_get_entity(company_id, db, user_id, ENTITY_CONFIGS["company"])

@app.post("/companies/{company_id}/update", response_model=SingleObjectResponse)
async def update_company(
    company_id: int,
    company_update: CompanyCreate,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Update a company"""
    return _handle_update_entity(company_id, company_update, db, user_id, ENTITY_CONFIGS["company"])

@app.post("/companies/bulk/delete", response_model=BulkDeleteResponse)
async def bulk_delete_companies(
    delete_data: BulkDeleteRequest,
    batch_size: int = Query(BATCH_SIZE, ge=1, description="Batch size for processing"),
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Bulk delete companies"""
    return _handle_bulk_delete(delete_data, db, user_id, ENTITY_CONFIGS["company"], batch_size)

@app.post("/companies/{company_id}/toggle", response_model=SingleObjectResponse)
async def toggle_company_active(
    company_id: int,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Toggle is_active status for a company"""
    return _handle_toggle_entity(company_id, db, user_id, ENTITY_CONFIGS["company"])

# Ad Campaign endpoints
@app.post("/ad_campaigns", response_model=SingleObjectResponse, status_code=201)
async def create_ad_campaign(
    campaign: AdCampaignCreate,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Create a new ad campaign"""
    return _handle_create_entity(campaign, db, user_id, ENTITY_CONFIGS["campaign"])

@app.get("/ad_campaigns", response_model=MultipleObjectsResponse)
async def list_ad_campaigns(
    company_id: Optional[int] = None,
    page: int = Query(DEFAULT_PAGE, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE, description=f"Items per page (max {MAX_PAGE_SIZE})"),
    search: Optional[str] = Query(None, description="Search by campaign title (case-insensitive, partial match)"),
    is_active: Optional[bool] = Query(None, description="Filter by is_active status"),
    created_after: Optional[datetime] = Query(None, description="Filter by created date (after)"),
    created_before: Optional[datetime] = Query(None, description="Filter by created date (before)"),
    updated_after: Optional[datetime] = Query(None, description="Filter by updated date (after)"),
    updated_before: Optional[datetime] = Query(None, description="Filter by updated date (before)"),
    sort_by: Optional[str] = Query("created", description="Sort by field: id, title, is_active, company_id, created, updated"),
    sort_order: Optional[str] = Query("desc", description="Sort order: asc or desc"),
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """List all ad campaigns for the authenticated user with pagination, filters, and sorting"""
    return _handle_list_entities(
        db, user_id, ENTITY_CONFIGS["campaign"], page, page_size, search, is_active,
        created_after, created_before, updated_after, updated_before,
        sort_by, sort_order, _get_ad_campaigns_metadata, company_id
    )

@app.get("/ad_campaigns/{campaign_id}", response_model=SingleObjectResponse)
async def get_ad_campaign(
    campaign_id: int,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Get a specific ad campaign by ID"""
    return _handle_get_entity(campaign_id, db, user_id, ENTITY_CONFIGS["campaign"])

@app.post("/ad_campaigns/{campaign_id}/update", response_model=SingleObjectResponse)
async def update_ad_campaign(
    campaign_id: int,
    campaign_update: AdCampaignCreate,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Update an ad campaign"""
    return _handle_update_entity(campaign_id, campaign_update, db, user_id, ENTITY_CONFIGS["campaign"])

@app.post("/ad_campaigns/bulk/delete", response_model=BulkDeleteResponse)
async def bulk_delete_ad_campaigns(
    delete_data: BulkDeleteRequest,
    batch_size: int = Query(BATCH_SIZE, ge=1, description="Batch size for processing"),
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Bulk delete ad campaigns"""
    return _handle_bulk_delete(delete_data, db, user_id, ENTITY_CONFIGS["campaign"], batch_size)

@app.post("/ad_campaigns/{campaign_id}/toggle", response_model=SingleObjectResponse)
async def toggle_ad_campaign_active(
    campaign_id: int,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Toggle is_active status for an ad campaign"""
    return _handle_toggle_entity(campaign_id, db, user_id, ENTITY_CONFIGS["campaign"])

# Ad Group endpoints
@app.post("/ad_groups", response_model=SingleObjectResponse, status_code=201)
async def create_ad_group(
    ad_group: AdGroupCreate,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Create a new ad group"""
    return _handle_create_entity(ad_group, db, user_id, ENTITY_CONFIGS["ad_group"])

@app.get("/ad_groups", response_model=MultipleObjectsResponse)
async def list_ad_groups(
    ad_campaign_id: Optional[int] = None,
    page: int = Query(DEFAULT_PAGE, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE, description=f"Items per page (max {MAX_PAGE_SIZE})"),
    search: Optional[str] = Query(None, description="Search by ad group title (case-insensitive, partial match)"),
    is_active: Optional[bool] = Query(None, description="Filter by is_active status"),
    created_after: Optional[datetime] = Query(None, description="Filter by created date (after)"),
    created_before: Optional[datetime] = Query(None, description="Filter by created date (before)"),
    updated_after: Optional[datetime] = Query(None, description="Filter by updated date (after)"),
    updated_before: Optional[datetime] = Query(None, description="Filter by updated date (before)"),
    sort_by: Optional[str] = Query("created", description="Sort by field: id, title, is_active, ad_campaign_id, created, updated"),
    sort_order: Optional[str] = Query("desc", description="Sort order: asc or desc"),
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """List all ad groups for the authenticated user with pagination, filters, and sorting"""
    return _handle_list_entities(
        db, user_id, ENTITY_CONFIGS["ad_group"], page, page_size, search, is_active,
        created_after, created_before, updated_after, updated_before,
        sort_by, sort_order, _get_ad_groups_metadata, ad_campaign_id
    )

@app.get("/ad_groups/{ad_group_id}", response_model=SingleObjectResponse)
async def get_ad_group(
    ad_group_id: int,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Get a specific ad group by ID"""
    return _handle_get_entity(ad_group_id, db, user_id, ENTITY_CONFIGS["ad_group"])

@app.post("/ad_groups/{ad_group_id}/update", response_model=SingleObjectResponse)
async def update_ad_group(
    ad_group_id: int,
    ad_group_update: AdGroupCreate,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Update an ad group"""
    return _handle_update_entity(ad_group_id, ad_group_update, db, user_id, ENTITY_CONFIGS["ad_group"])

@app.post("/ad_groups/bulk/delete", response_model=BulkDeleteResponse)
async def bulk_delete_ad_groups(
    delete_data: BulkDeleteRequest,
    batch_size: int = Query(BATCH_SIZE, ge=1, description="Batch size for processing"),
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Bulk delete ad groups"""
    return _handle_bulk_delete(delete_data, db, user_id, ENTITY_CONFIGS["ad_group"], batch_size)

@app.post("/ad_groups/{ad_group_id}/toggle", response_model=SingleObjectResponse)
async def toggle_ad_group_active(
    ad_group_id: int,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Toggle is_active status for an ad group"""
    return _handle_toggle_entity(ad_group_id, db, user_id, ENTITY_CONFIGS["ad_group"])


# Helper functions for keyword listing
def _get_active_entity_ids(db: Session, user_id: str) -> tuple[list[int], list[int], list[int]]:
    """Get IDs of all active entities for the user using a single optimized query per entity type."""
    # Use scalar subqueries to get just the IDs efficiently
    company_ids = db.query(models.Company.id).filter(
        models.Company.clerk_user_id == user_id,
        models.Company.is_active == True
    ).all()
    
    campaign_ids = db.query(models.AdCampaign.id).filter(
        models.AdCampaign.clerk_user_id == user_id,
        models.AdCampaign.is_active == True
    ).all()
    
    adgroup_ids = db.query(models.AdGroup.id).filter(
        models.AdGroup.clerk_user_id == user_id,
        models.AdGroup.is_active == True
    ).all()
    
    return (
        [c[0] for c in company_ids],
        [c[0] for c in campaign_ids],
        [a[0] for a in adgroup_ids]
    )


def _create_match_type_condition(user_id: str, match_field: str):
    """Create an EXISTS condition for a match type across all three relation tables."""
    from sqlalchemy import or_, exists
    
    return or_(
        exists().where(
            models.CompanyKeyword.keyword_id == models.Keyword.id,
            models.CompanyKeyword.clerk_user_id == user_id,
            getattr(models.CompanyKeyword, match_field) == True
        ),
        exists().where(
            models.AdCampaignKeyword.keyword_id == models.Keyword.id,
            models.AdCampaignKeyword.clerk_user_id == user_id,
            getattr(models.AdCampaignKeyword, match_field) == True
        ),
        exists().where(
            models.AdGroupKeyword.keyword_id == models.Keyword.id,
            models.AdGroupKeyword.clerk_user_id == user_id,
            getattr(models.AdGroupKeyword, match_field) == True
        )
    )


def _create_match_type_sort_expr(user_id: str, match_field: str):
    """Create a CASE expression for sorting by match type presence (returns 1 if present, 0 if not)."""
    from sqlalchemy import case
    
    condition = _create_match_type_condition(user_id, match_field)
    return case((condition, 1), else_=0)


def _format_match_types(relation) -> dict:
    """Format match types from a relation object into a dictionary."""
    return {
        "broad": relation.broad,
        "phrase": relation.phrase,
        "exact": relation.exact,
        "neg_broad": relation.neg_broad,
        "neg_phrase": relation.neg_phrase,
        "neg_exact": relation.neg_exact
    }


def _fetch_relations_bulk(
    db: Session,
    keyword_ids: list[int],
    company_id_list: list[int],
    campaign_id_list: list[int],
    adgroup_id_list: list[int]
) -> tuple[dict, dict, dict]:
    """Fetch all relations for given keywords in bulk (3 queries instead of N*M queries)."""
    # Fetch company relations
    company_relations = {}
    if company_id_list:
        relations = db.query(models.CompanyKeyword).filter(
            models.CompanyKeyword.keyword_id.in_(keyword_ids),
            models.CompanyKeyword.company_id.in_(company_id_list)
        ).all()
        for rel in relations:
            key = (rel.keyword_id, rel.company_id)
            company_relations[key] = rel
    
    # Fetch campaign relations
    campaign_relations = {}
    if campaign_id_list:
        relations = db.query(models.AdCampaignKeyword).filter(
            models.AdCampaignKeyword.keyword_id.in_(keyword_ids),
            models.AdCampaignKeyword.ad_campaign_id.in_(campaign_id_list)
        ).all()
        for rel in relations:
            key = (rel.keyword_id, rel.ad_campaign_id)
            campaign_relations[key] = rel
    
    # Fetch ad group relations
    adgroup_relations = {}
    if adgroup_id_list:
        relations = db.query(models.AdGroupKeyword).filter(
            models.AdGroupKeyword.keyword_id.in_(keyword_ids),
            models.AdGroupKeyword.ad_group_id.in_(adgroup_id_list)
        ).all()
        for rel in relations:
            key = (rel.keyword_id, rel.ad_group_id)
            adgroup_relations[key] = rel
    
    return company_relations, campaign_relations, adgroup_relations


def _build_matrix_keyword_data(
    keyword,
    company_id_list: list[int],
    campaign_id_list: list[int],
    adgroup_id_list: list[int],
    company_relations: dict,
    campaign_relations: dict,
    adgroup_relations: dict
) -> dict:
    """Build keyword data in matrix format with entity columns using pre-fetched relations."""
    keyword_data = {
        "id": keyword.id,
        "keyword": keyword.keyword,
        "created": keyword.created,
        "updated": keyword.updated,
        "relations": {
            "companies": {},
            "ad_campaigns": {},
            "ad_groups": {}
        }
    }
    
    # Add company match types as columns (lookup from pre-fetched dict)
    for company_id in company_id_list:
        relation = company_relations.get((keyword.id, company_id))
        if relation:
            keyword_data["relations"]["companies"][company_id] = _format_match_types(relation)
    
    # Add campaign match types as columns (lookup from pre-fetched dict)
    for campaign_id in campaign_id_list:
        relation = campaign_relations.get((keyword.id, campaign_id))
        if relation:
            keyword_data["relations"]["ad_campaigns"][campaign_id] = _format_match_types(relation)
    
    # Add ad group match types as columns (lookup from pre-fetched dict)
    for adgroup_id in adgroup_id_list:
        relation = adgroup_relations.get((keyword.id, adgroup_id))
        if relation:
            keyword_data["relations"]["ad_groups"][adgroup_id] = _format_match_types(relation)
    
    return keyword_data


# Keyword endpoints
@app.post("/keywords/bulk", response_model=BulkKeywordCreateResponse, status_code=201)
async def create_bulk_keywords(
    bulk_data: BulkKeywordCreate,
    batch_size: int = Query(BATCH_SIZE, ge=1, description="Batch size for processing"),
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    created_keywords = []
    existing_keywords = []
    total_relations_created = 0
    total_relations_updated = 0
    batches_processed = 0
    
    # Process keywords in batches
    for keyword_batch in process_in_batches(bulk_data.keywords, batch_size=batch_size):
        batch_created = []
        batch_existing = []
        batch_relations_created = 0
        batch_relations_updated = 0
        
        for keyword_text in keyword_batch:
            keyword_text = keyword_text.strip()
            if not keyword_text:
                continue
                
            # Try to get existing keyword or create new one
            keyword = db.query(models.Keyword).filter(
                models.Keyword.keyword == keyword_text,
                models.Keyword.clerk_user_id == user_id
            ).first()
            
            if keyword:
                batch_existing.append(keyword)
            else:
                keyword = models.Keyword(
                    keyword=keyword_text,
                    clerk_user_id=user_id
                )
                db.add(keyword)
                db.flush()  # Get the ID without committing
                batch_created.append(keyword)
            
            # Create relations using helper function
            added, updated = _create_keyword_relations(
                db=db,
                keyword=keyword,
                company_ids=bulk_data.company_ids,
                ad_campaign_ids=bulk_data.ad_campaign_ids,
                ad_group_ids=bulk_data.ad_group_ids,
                broad=bulk_data.broad,
                phrase=bulk_data.phrase,
                exact=bulk_data.exact,
                neg_broad=bulk_data.neg_broad,
                neg_phrase=bulk_data.neg_phrase,
                neg_exact=bulk_data.neg_exact,
                override_broad=bulk_data.override_broad,
                override_phrase=bulk_data.override_phrase,
                override_exact=bulk_data.override_exact,
                override_neg_broad=bulk_data.override_neg_broad,
                override_neg_phrase=bulk_data.override_neg_phrase,
                override_neg_exact=bulk_data.override_neg_exact
            )
            batch_relations_created += added
            batch_relations_updated += updated
        
        # Commit after each batch
        db.commit()
        created_keywords.extend(batch_created)
        existing_keywords.extend(batch_existing)
        total_relations_created += batch_relations_created
        total_relations_updated += batch_relations_updated
        batches_processed += 1
    
    # Calculate totals
    all_keywords = created_keywords + existing_keywords
    
    return BulkKeywordCreateResponse(
        message=f"Created {len(created_keywords)} new keywords, found {len(existing_keywords)} existing",
        objects=[Keyword.model_validate(k) for k in all_keywords],
        created=len(created_keywords),
        existing=len(existing_keywords),
        processed=len(all_keywords),
        requested=len(bulk_data.keywords),
        relations_created=total_relations_created,
        relations_updated=total_relations_updated,
        batches_processed=batches_processed,
        batch_size=batch_size
    )


def _update_relation_match_types(assoc, update_data: BulkKeywordUpdateRelations) -> bool:
    updated = False
    
    if update_data.override_broad and update_data.broad is not None:
        assoc.broad = update_data.broad
        updated = True
    if update_data.override_phrase and update_data.phrase is not None:
        assoc.phrase = update_data.phrase
        updated = True
    if update_data.override_exact and update_data.exact is not None:
        assoc.exact = update_data.exact
        updated = True
    if update_data.override_neg_broad and update_data.neg_broad is not None:
        assoc.neg_broad = update_data.neg_broad
        updated = True
    if update_data.override_neg_phrase and update_data.neg_phrase is not None:
        assoc.neg_phrase = update_data.neg_phrase
        updated = True
    if update_data.override_neg_exact and update_data.neg_exact is not None:
        assoc.neg_exact = update_data.neg_exact
        updated = True
    
    return updated


def _create_keyword_relations(
    db: Session,
    keyword,
    company_ids: list[int],
    ad_campaign_ids: list[int],
    ad_group_ids: list[int],
    broad: bool,
    phrase: bool,
    exact: bool,
    neg_broad: bool,
    neg_phrase: bool,
    neg_exact: bool,
    override_broad: bool,
    override_phrase: bool,
    override_exact: bool,
    override_neg_broad: bool,
    override_neg_phrase: bool,
    override_neg_exact: bool
) -> tuple[int, int]:
    
    # Match types are now always provided with defaults
    
    def _process_entity_relations(
        entity_ids: list[int],
        model_class,
        entity_id_field: str
    ) -> tuple[int, int]:
        added = 0
        updated = 0
        
        for entity_id in entity_ids:
            # Query for existing relation
            filter_kwargs = {
                entity_id_field: entity_id,
                'keyword_id': keyword.id
            }
            existing = db.query(model_class).filter_by(**filter_kwargs).first()
            
            if existing:
                # Update existing relation if any override flag is True
                relation_updated = False
                if override_broad:
                    existing.broad = broad
                    relation_updated = True
                if override_phrase:
                    existing.phrase = phrase
                    relation_updated = True
                if override_exact:
                    existing.exact = exact
                    relation_updated = True
                if override_neg_broad:
                    existing.neg_broad = neg_broad
                    relation_updated = True
                if override_neg_phrase:
                    existing.neg_phrase = neg_phrase
                    relation_updated = True
                if override_neg_exact:
                    existing.neg_exact = neg_exact
                    relation_updated = True
                
                if relation_updated:
                    updated += 1
            else:
                # Create new relation
                create_kwargs = {
                    entity_id_field: entity_id,
                    'keyword_id': keyword.id,
                    'clerk_user_id': keyword.clerk_user_id,
                    'broad': broad,
                    'phrase': phrase,
                    'exact': exact,
                    'neg_broad': neg_broad,
                    'neg_phrase': neg_phrase,
                    'neg_exact': neg_exact
                }
                new_relation = model_class(**create_kwargs)
                db.add(new_relation)
                added += 1
        
        return added, updated
    
    relations_created = 0
    relations_updated = 0
    
    # Handle company relations
    if company_ids:
        added, updated = _process_entity_relations(
            company_ids, 
            models.CompanyKeyword, 
            'company_id'
        )
        relations_created += added
        relations_updated += updated
    
    # Handle campaign relations
    if ad_campaign_ids:
        added, updated = _process_entity_relations(
            ad_campaign_ids,
            models.AdCampaignKeyword,
            'ad_campaign_id'
        )
        relations_created += added
        relations_updated += updated
    
    # Handle ad group relations
    if ad_group_ids:
        added, updated = _process_entity_relations(
            ad_group_ids,
            models.AdGroupKeyword,
            'ad_group_id'
        )
        relations_created += added
        relations_updated += updated
    
    return relations_created, relations_updated


@app.post("/keywords/bulk/relations/update", response_model=BulkRelationUpdateResponse)
async def bulk_update_keyword_relations(
    update_data: BulkKeywordUpdateRelations,
    batch_size: int = Query(BATCH_SIZE, ge=1, description="Batch size for processing"),
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    # Limit to MAX_KEYWORDS_PER_REQUEST keywords per request
    if len(update_data.keyword_ids) > MAX_KEYWORDS_PER_REQUEST:
        raise HTTPException(
            status_code=400,
            detail=f"Maximum {MAX_KEYWORDS_PER_REQUEST} keywords allowed per request"
        )
    
    # Get keywords that belong to user
    keywords = db.query(models.Keyword).filter(
        models.Keyword.id.in_(update_data.keyword_ids),
        models.Keyword.clerk_user_id == user_id
    ).all()
    
    relations_updated = 0
    batches_processed = 0
    
    # Process keywords in batches of DEFAULT_BATCH_SIZE
    for keyword_batch in process_in_batches(keywords, batch_size=batch_size):
        # Process each keyword in the batch
        for keyword in keyword_batch:
            # Update company relations (with ownership check)
            company_relations = db.query(models.CompanyKeyword).filter(
                models.CompanyKeyword.keyword_id == keyword.id,
                models.CompanyKeyword.clerk_user_id == user_id
            ).all()
            
            for assoc in company_relations:
                if _update_relation_match_types(assoc, update_data):
                    relations_updated += 1
            
            # Update campaign relations (with ownership check)
            campaign_relations = db.query(models.AdCampaignKeyword).filter(
                models.AdCampaignKeyword.keyword_id == keyword.id,
                models.AdCampaignKeyword.clerk_user_id == user_id
            ).all()
            
            for assoc in campaign_relations:
                if _update_relation_match_types(assoc, update_data):
                    relations_updated += 1
            
            # Update ad group relations (with ownership check)
            ad_group_relations = db.query(models.AdGroupKeyword).filter(
                models.AdGroupKeyword.keyword_id == keyword.id,
                models.AdGroupKeyword.clerk_user_id == user_id
            ).all()
            
            for assoc in ad_group_relations:
                if _update_relation_match_types(assoc, update_data):
                    relations_updated += 1
        
        # Commit after each batch
        db.commit()
        batches_processed += 1
    
    return BulkRelationUpdateResponse(
        message=f"Updated {relations_updated} relations for {len(keywords)} keywords",
        processed=len(keywords),
        requested=len(update_data.keyword_ids),
        updated=relations_updated,
        batches_processed=batches_processed,
        batch_size=batch_size
    )


@app.post("/keywords/bulk/relations", response_model=BulkRelationCreateResponse)
async def bulk_create_keyword_relations(
    create_data: BulkKeywordCreateRelations,
    batch_size: int = Query(BATCH_SIZE, ge=1, description="Batch size for processing"),
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    # Get keywords that belong to user
    keywords = db.query(models.Keyword).filter(
        models.Keyword.id.in_(create_data.keyword_ids),
        models.Keyword.clerk_user_id == user_id
    ).all()
    
    total_relations_created = 0
    total_relations_updated = 0
    batches_processed = 0
    
    # Process keywords in batches
    for keyword_batch in process_in_batches(keywords, batch_size=batch_size):
        batch_relations_created = 0
        batch_relations_updated = 0
        
        # Process each keyword in the batch
        for keyword in keyword_batch:
            added, updated = _create_keyword_relations(
                db=db,
                keyword=keyword,
                company_ids=create_data.company_ids,
                ad_campaign_ids=create_data.ad_campaign_ids,
                ad_group_ids=create_data.ad_group_ids,
                broad=create_data.broad,
                phrase=create_data.phrase,
                exact=create_data.exact,
                neg_broad=create_data.neg_broad,
                neg_phrase=create_data.neg_phrase,
                neg_exact=create_data.neg_exact,
                override_broad=create_data.override_broad,
                override_phrase=create_data.override_phrase,
                override_exact=create_data.override_exact,
                override_neg_broad=create_data.override_neg_broad,
                override_neg_phrase=create_data.override_neg_phrase,
                override_neg_exact=create_data.override_neg_exact
            )
            batch_relations_created += added
            batch_relations_updated += updated
        
        # Commit after each batch
        db.commit()
        total_relations_created += batch_relations_created
        total_relations_updated += batch_relations_updated
        batches_processed += 1
    
    return BulkRelationCreateResponse(
        message=f"Processed {len(keywords)} keywords",
        processed=len(keywords),
        requested=len(create_data.keyword_ids),
        created=total_relations_created,
        updated=total_relations_updated,
        batches_processed=batches_processed,
        batch_size=batch_size
    )


@app.get("/keywords", response_model=MultipleObjectsResponse)
async def list_keywords(
    page: int = Query(DEFAULT_PAGE, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE, description=f"Items per page (max {MAX_PAGE_SIZE})"),
    only_attached: bool = Query(False, description="Show only keywords attached to at least one entity"),
    search: Optional[str] = Query(None, description="Search by keyword text (case-insensitive, partial match)"),
    created_after: Optional[datetime] = Query(None, description="Filter by created date (after)"),
    created_before: Optional[datetime] = Query(None, description="Filter by created date (before)"),
    updated_after: Optional[datetime] = Query(None, description="Filter by updated date (after)"),
    updated_before: Optional[datetime] = Query(None, description="Filter by updated date (before)"),
    has_broad: Optional[bool] = Query(None, description="Filter keywords with at least one broad match relation"),
    has_phrase: Optional[bool] = Query(None, description="Filter keywords with at least one phrase match relation"),
    has_exact: Optional[bool] = Query(None, description="Filter keywords with at least one exact match relation"),
    has_neg_broad: Optional[bool] = Query(None, description="Filter keywords with at least one negative broad match relation"),
    has_neg_phrase: Optional[bool] = Query(None, description="Filter keywords with at least one negative phrase match relation"),
    has_neg_exact: Optional[bool] = Query(None, description="Filter keywords with at least one negative exact match relation"),
    sort_by: Optional[str] = Query("created", description="Primary sort field: id, keyword, created, updated, has_broad, has_phrase, has_exact, has_neg_broad, has_neg_phrase, has_neg_exact"),
    sort_order: Optional[str] = Query("desc", description="Primary sort order: asc or desc"),
    sort_by_2: Optional[str] = Query(None, description="Secondary sort field (same options as sort_by)"),
    sort_order_2: Optional[str] = Query(None, description="Secondary sort order: asc or desc"),
    sort_by_3: Optional[str] = Query(None, description="Tertiary sort field (same options as sort_by)"),
    sort_order_3: Optional[str] = Query(None, description="Tertiary sort order: asc or desc"),
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    from sqlalchemy import or_, exists, and_, case, func, select
    
    # Get active entity IDs efficiently (just IDs, not full objects)
    company_id_list, campaign_id_list, adgroup_id_list = _get_active_entity_ids(db, user_id)
    
    # Build base query - start with user filter
    query = db.query(models.Keyword).filter(models.Keyword.clerk_user_id == user_id)
    
    # Add search filter if provided
    if search:
        query = query.filter(models.Keyword.keyword.ilike(f"%{search}%"))
    
    # Add date filters
    if created_after:
        query = query.filter(models.Keyword.created >= created_after)
    if created_before:
        query = query.filter(models.Keyword.created <= created_before)
    if updated_after:
        query = query.filter(models.Keyword.updated >= updated_after)
    if updated_before:
        query = query.filter(models.Keyword.updated <= updated_before)
    
    # Add match type filters using helper function
    match_type_params = {
        'broad': has_broad,
        'phrase': has_phrase,
        'exact': has_exact,
        'neg_broad': has_neg_broad,
        'neg_phrase': has_neg_phrase,
        'neg_exact': has_neg_exact
    }
    
    match_type_filters = []
    for match_field, has_match in match_type_params.items():
        if has_match is not None:
            condition = _create_match_type_condition(user_id, match_field)
            match_type_filters.append(condition if has_match else ~condition)
    
    # Apply match type filters (AND condition - all must be satisfied)
    if match_type_filters:
        query = query.filter(and_(*match_type_filters))
    
    # Filter keywords that have relations with active entities (OR condition)
    # Use EXISTS subqueries for optimal performance
    # Only apply if there are active entities to filter by
    if company_id_list or campaign_id_list or adgroup_id_list:
        filters = []
        
        if company_id_list:
            filters.append(
                exists().where(
                    models.CompanyKeyword.keyword_id == models.Keyword.id,
                    models.CompanyKeyword.company_id.in_(company_id_list)
                )
            )
        
        if campaign_id_list:
            filters.append(
                exists().where(
                    models.AdCampaignKeyword.keyword_id == models.Keyword.id,
                    models.AdCampaignKeyword.ad_campaign_id.in_(campaign_id_list)
                )
            )
        
        if adgroup_id_list:
            filters.append(
                exists().where(
                    models.AdGroupKeyword.keyword_id == models.Keyword.id,
                    models.AdGroupKeyword.ad_group_id.in_(adgroup_id_list)
                )
            )
        
        query = query.filter(or_(*filters))
    
    # If only_attached is True, add filter for keywords with at least one relation
    if only_attached:
        # Use EXISTS subqueries for all three relation types (OR condition)
        query = query.filter(
            or_(
                exists().where(
                    models.CompanyKeyword.keyword_id == models.Keyword.id,
                    models.CompanyKeyword.clerk_user_id == user_id
                ),
                exists().where(
                    models.AdCampaignKeyword.keyword_id == models.Keyword.id,
                    models.AdCampaignKeyword.clerk_user_id == user_id
                ),
                exists().where(
                    models.AdGroupKeyword.keyword_id == models.Keyword.id,
                    models.AdGroupKeyword.clerk_user_id == user_id
                )
            )
        )
    
    # Helper function to create match type sorting expressions
    def _get_sort_column(field_name: str):
        """Get the column or expression for sorting."""
        field_name = field_name.lower()
        
        # Simple field mappings
        simple_fields = {
            "id": models.Keyword.id,
            "keyword": models.Keyword.keyword,
            "created": models.Keyword.created,
            "updated": models.Keyword.updated
        }
        
        if field_name in simple_fields:
            return simple_fields[field_name]
        
        # Match type fields - use helper function
        match_type_map = {
            "has_broad": "broad",
            "has_phrase": "phrase",
            "has_exact": "exact",
            "has_neg_broad": "neg_broad",
            "has_neg_phrase": "neg_phrase",
            "has_neg_exact": "neg_exact"
        }
        
        if field_name in match_type_map:
            return _create_match_type_sort_expr(user_id, match_type_map[field_name])
        
        return None
    
    # Add sorting (up to 3 levels)
    sort_configs = [
        (sort_by, sort_order),
        (sort_by_2, sort_order_2),
        (sort_by_3, sort_order_3)
    ]
    
    order_columns = []
    for sort_field, sort_dir in sort_configs:
        if sort_field:
            sort_column = _get_sort_column(sort_field)
            if sort_column is not None:
                direction = (sort_dir or "desc").lower()
                if direction == "asc":
                    order_columns.append(sort_column.asc())
                else:
                    order_columns.append(sort_column.desc())
    
    # Apply sorting or default to created desc
    if order_columns:
        query = query.order_by(*order_columns)
    else:
        query = query.order_by(models.Keyword.created.desc())
    
    # Apply pagination AFTER all filters and sorting
    keywords, total_count, total_pages = paginate_query(query, page, page_size)
    
    # Always use matrix format - fetch all relations in bulk (3 queries instead of N*M queries)
    # When there are no active entities, the lists are empty and relations will be empty dicts
    keyword_ids = [k.id for k in keywords]
    company_relations, campaign_relations, adgroup_relations = _fetch_relations_bulk(
        db, keyword_ids, company_id_list, campaign_id_list, adgroup_id_list
    )
    
    # Build keyword data using pre-fetched relations (or empty dicts if no active entities)
    result_objects = [
        _build_matrix_keyword_data(
            keyword, company_id_list, campaign_id_list, adgroup_id_list,
            company_relations, campaign_relations, adgroup_relations
        )
        for keyword in keywords
    ]
    
    filters, sorting, special_features = _get_keywords_metadata()
    
    return MultipleObjectsResponse(
        message=f"Retrieved {total_count} keywords",
        objects=result_objects,
        pagination={
            "total": total_count,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages
        },
        filters=filters,
        sorting=sorting,
        special_features=special_features
    )


@app.get("/keywords/{keyword_id}", response_model=SingleObjectResponse)
async def get_keyword(
    keyword_id: int,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Get a specific keyword by ID"""
    return _get_entity_by_id(
        db=db,
        user_id=user_id,
        entity_id=keyword_id,
        model_class=models.Keyword,
        schema_class=Keyword,
        entity_name="keyword"
    )


@app.post("/keywords/{keyword_id}/update", response_model=SingleObjectResponse)
async def update_keyword(
    keyword_id: int,
    keyword_update: KeywordCreate,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Update a keyword"""
    return _update_simple_entity(
        db=db,
        user_id=user_id,
        entity_id=keyword_id,
        entity_update=keyword_update,
        model_class=models.Keyword,
        schema_class=Keyword,
        entity_name="keyword",
        update_fields={"keyword": keyword_update.keyword}
    )


@app.post("/keywords/bulk/delete", response_model=BulkDeleteResponse)
async def bulk_delete_keywords(
    delete_data: BulkDeleteRequest,
    batch_size: int = Query(BATCH_SIZE, ge=1, description="Batch size for processing"),
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Bulk delete keywords"""
    return _bulk_delete_with_batches(
        db=db,
        user_id=user_id,
        ids=delete_data.ids,
        model_class=models.Keyword,
        ownership_field="clerk_user_id",
        message_template="Deleted {0} keywords",
        batch_size=batch_size
    )


@app.post("/relations/ad_company_keyword/bulk/delete", response_model=BulkDeleteResponse)
async def bulk_delete_company_keyword_relations(
    delete_data: BulkDeleteRequest,
    batch_size: int = Query(BATCH_SIZE, ge=1, description="Batch size for processing"),
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Bulk delete ad_company_keyword relations"""
    return _bulk_delete_with_batches(
        db=db,
        user_id=user_id,
        ids=delete_data.ids,
        model_class=models.CompanyKeyword,
        ownership_field="clerk_user_id",
        message_template="Deleted {0} ad_company_keyword relations",
        batch_size=batch_size
    )


@app.post("/relations/ad_campaign_keyword/bulk/delete", response_model=BulkDeleteResponse)
async def bulk_delete_campaign_keyword_relations(
    delete_data: BulkDeleteRequest,
    batch_size: int = Query(BATCH_SIZE, ge=1, description="Batch size for processing"),
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Bulk delete ad_campaign_keyword relations"""
    return _bulk_delete_with_batches(
        db=db,
        user_id=user_id,
        ids=delete_data.ids,
        model_class=models.AdCampaignKeyword,
        ownership_field="clerk_user_id",
        message_template="Deleted {0} ad_campaign_keyword relations",
        batch_size=batch_size
    )


@app.post("/relations/ad_group_keyword/bulk/delete", response_model=BulkDeleteResponse)
async def bulk_delete_adgroup_keyword_relations(
    delete_data: BulkDeleteRequest,
    batch_size: int = Query(BATCH_SIZE, ge=1, description="Batch size for processing"),
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Bulk delete ad_group_keyword relations"""
    return _bulk_delete_with_batches(
        db=db,
        user_id=user_id,
        ids=delete_data.ids,
        model_class=models.AdGroupKeyword,
        ownership_field="clerk_user_id",
        message_template="Deleted {0} ad_group_keyword relations",
        batch_size=batch_size
    )
