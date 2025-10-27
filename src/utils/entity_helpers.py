"""
Entity helper functions for KPlanner API.

This module contains utility functions for entity CRUD operations,
including creation, retrieval, updating, and listing with filtering.
"""

from datetime import datetime
from typing import Optional
from sqlalchemy.orm import Session
from fastapi import HTTPException

from src.schemas.schemas import SingleObjectResponse, MultipleObjectsResponse, BulkDeleteRequest
from src.utils.database_helpers import paginate_query, apply_date_filters, apply_sorting
from src.utils.bulk_helpers import bulk_delete_with_batches


def get_entity_sort_fields(parent_field: str = None):
    """Generate sort fields map for an entity based on its model class."""
    base_fields = {
        "id": "id",
        "title": "title",
        "created": "created",
        "updated": "updated"
    }

    # Add parent field if exists
    if parent_field:
        base_fields[parent_field] = parent_field

    return base_fields


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
    entity_name: str,
    entity_name_plural: str,
    page: int,
    page_size: int,
    search: Optional[str],
    is_active: Optional[bool],  # Kept for backward compatibility but ignored
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

    # is_active filter removed - all entities are now visible

    # Apply date filters
    query = apply_date_filters(query, model_class, created_after, created_before, updated_after, updated_before)

    # Apply sorting
    query = apply_sorting(query, model_class, sort_by, sort_order, sort_fields_map)

    # Paginate
    entities, total_count, total_pages = paginate_query(query, page, page_size)

    # Get metadata
    filters, sorting = metadata_func()

    # Build response
    response_objects = []
    for entity in entities:
        entity_dict = schema_class.model_validate(entity).model_dump()
        response_objects.append(entity_dict)

    return MultipleObjectsResponse(
        message=f"Retrieved {total_count} {entity_name_plural}",
        objects=response_objects,
        pagination={
            "total": total_count,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages
        },
        filters=filters,
        sorting=sorting
    )


def create_entity(
    db: Session,
    user_id: str,
    entity_data,
    model_class,
    schema_class,
    entity_name: str,
    parent_field: str = None,
    parent_model = None,
    parent_name: str = None,
    **extra_fields
) -> SingleObjectResponse:
    """Generic helper for creating entities with optional parent validation."""
    # Validate parent if required
    if parent_field and parent_model:
        parent_id = getattr(entity_data, parent_field)
        validate_parent_entity(db, user_id, parent_id, parent_model, parent_name)

    # Build entity data
    entity_dict = {
        'title': entity_data.title,
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
    """Generic helper for updating entities with optional parent validation."""
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

    # Update entity
    entity.title = entity_update.title
    if parent_field:
        setattr(entity, parent_field, getattr(entity_update, parent_field))

    db.commit()
    db.refresh(entity)

    return SingleObjectResponse(
        message=f"{entity_name.capitalize()} updated successfully",
        object=schema_class.model_validate(entity)
    )


# Generic endpoint handler functions
def handle_create_entity(entity_data, db: Session, user_id: str, config: dict):
    """Generic handler for entity creation."""
    return create_entity(
        db=db,
        user_id=user_id,
        entity_data=entity_data,
        model_class=config["model_class"],
        schema_class=config["schema_class"],
        entity_name=config["entity_name"],
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
        sort_fields_map=get_entity_sort_fields(config["parent_field"]),
        metadata_func=metadata_func,
        parent_filter=parent_filter
    )


def handle_bulk_delete(delete_data: BulkDeleteRequest, db: Session, user_id: str, config: dict):
    """Generic handler for bulk delete operations."""
    return bulk_delete_with_batches(
        db=db,
        user_id=user_id,
        ids=delete_data.ids,
        model_class=config["model_class"],
        ownership_field="clerk_user_id",
        message_template=f"Deleted {{0}} {config['entity_name_plural']}",
    )


def handle_get_entity(entity_id: int, db: Session, user_id: str, config: dict):
    """Generic handler for getting a single entity by ID."""
    return get_entity_by_id(
        db=db,
        user_id=user_id,
        entity_id=entity_id,
        model_class=config["model_class"],
        schema_class=config["schema_class"],
        entity_name=config["entity_name"]
    )


def handle_update_entity(entity_id: int, entity_update, db: Session, user_id: str, config: dict):
    """Generic handler for updating entities."""
    return update_entity_with_limit(
        db=db,
        user_id=user_id,
        entity_id=entity_id,
        entity_update=entity_update,
        model_class=config["model_class"],
        schema_class=config["schema_class"],
        entity_name=config["entity_name"],
        parent_field=config.get("parent_field"),
        parent_model=config.get("parent_model"),
        parent_name=config.get("parent_name")
    )