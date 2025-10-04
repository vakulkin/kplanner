from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import Optional
from datetime import datetime
import math
from ..core.settings import DEFAULT_PAGE, PAGE_SIZE, MAX_PAGE_SIZE, BATCH_SIZE
from ..schemas.schemas import SingleObjectResponse, MultipleObjectsResponse, BulkDeleteResponse, BulkDeleteRequest


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
def apply_date_filters(query, model_class, created_after, created_before, updated_after, updated_before):
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
def apply_sorting(query, model_class, sort_by, sort_order, sort_fields_map, default_field="created"):
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
def generate_metadata(entity_type, parent_field=None, additional_sort_fields=None):
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
def get_entity_sort_fields(model_class, parent_field: str = None):
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
def get_companies_metadata():
    """Get metadata for companies endpoint including available filters and sorting."""
    return generate_metadata("company")


def get_ad_campaigns_metadata():
    """Get metadata for ad campaigns endpoint including available filters and sorting."""
    return generate_metadata("campaign", parent_field="company_id")


def get_ad_groups_metadata():
    """Get metadata for ad groups endpoint including available filters and sorting."""
    return generate_metadata("ad group", parent_field="ad_campaign_id")


def get_keywords_metadata():
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

    return filters, sorting


# Helper function for batch processing
def process_in_batches(items: list, batch_size: int = BATCH_SIZE):
    batch_size = max(1, batch_size)  # Ensure at least 1

    for i in range(0, len(items), batch_size):
        yield items[i:i + batch_size]


def bulk_delete_with_batches(
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


def check_active_limit(
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


def toggle_entity_active(
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
        can_activate, limit_message = check_active_limit(
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


def validate_parent_entity(db: Session, user_id: str, parent_id: int, parent_model, parent_name: str):
    """Validate that a parent entity exists and belongs to the user."""
    parent = db.query(parent_model).filter(
        parent_model.id == parent_id,
        getattr(parent_model, 'clerk_user_id') == user_id
    ).first()
    if not parent:
        raise HTTPException(status_code=404, detail=f"{parent_name.capitalize()} not found")
    return parent


def get_entity_by_id(
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


def update_simple_entity(
    db: Session,
    user_id: str,
    entity_id: int,
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


def list_entities_with_filters(
    db: Session,
    user_id: str,
    model_class,
    schema_class,
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
    query = apply_date_filters(query, model_class, created_after, created_before, updated_after, updated_before)

    # Apply sorting
    query = apply_sorting(query, model_class, sort_by, sort_order, sort_fields_map)

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


def create_entity_with_limit(
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
        validate_parent_entity(db, user_id, parent_id, parent_model, parent_name)

    # Check if trying to create as active and limit is reached
    is_active = entity_data.is_active
    limit_message = None

    if is_active and active_limit:
        can_activate, limit_msg = check_active_limit(
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


def update_entity_with_limit(
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
        validate_parent_entity(db, user_id, parent_id, parent_model, parent_name)

    # Check if trying to activate and limit is reached
    is_active = entity_update.is_active
    limit_message = None

    if is_active and not entity.is_active and active_limit:  # Trying to activate
        can_activate, limit_msg = check_active_limit(
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


# Generic endpoint handler functions
def handle_create_entity(entity_data, db: Session, user_id: str, config: dict):
    """Generic handler for entity creation."""
    return create_entity_with_limit(
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


def handle_list_entities(
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
    
    return list_entities_with_filters(
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
        sort_fields_map=get_entity_sort_fields(config["model_class"], config["parent_field"]),
        metadata_func=metadata_func,
        parent_filter=parent_filter
    )


def handle_get_entity(entity_id: int, db: Session, user_id: str, config: dict):
    """Generic handler for getting a single entity."""
    return get_entity_by_id(
        db=db,
        user_id=user_id,
        entity_id=entity_id,
        model_class=config["model_class"],
        schema_class=config["schema_class"],
        entity_name=config["entity_name"]
    )


def handle_update_entity(entity_id: int, entity_update, db: Session, user_id: str, config: dict):
    """Generic handler for entity updates."""
    return update_entity_with_limit(
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


def handle_toggle_entity(entity_id: int, db: Session, user_id: str, config: dict):
    """Generic handler for toggling entity active status."""
    return toggle_entity_active(
        db=db,
        entity_id=entity_id,
        user_id=user_id,
        model_class=config["model_class"],
        schema_class=config["schema_class"],
        entity_name=config["entity_name"],
        active_limit=config["active_limit"]
    )


def handle_bulk_delete(delete_data: BulkDeleteRequest, db: Session, user_id: str, config: dict, batch_size: int):
    """Generic handler for bulk delete operations."""
    return bulk_delete_with_batches(
        db=db,
        user_id=user_id,
        ids=delete_data.ids,
        model_class=config["model_class"],
        ownership_field="clerk_user_id",
        message_template=f"Deleted {{0}} {config['entity_name_plural']}",
        batch_size=batch_size
    )