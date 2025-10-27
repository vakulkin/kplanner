"""
Database helper functions for KPlanner API.

This module contains utility functions for database operations,
including pagination, filtering, and sorting.
"""

from sqlalchemy import desc, asc

from src.core.settings import DEFAULT_PAGE, PAGE_SIZE


def paginate_query(query, page: int = DEFAULT_PAGE, page_size: int = PAGE_SIZE):
    """Paginate a SQLAlchemy query and return entities, total_count, and total_pages."""
    # Get total count before pagination
    total_count = query.count()

    # Calculate total pages
    total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1

    # Apply pagination
    offset = (page - 1) * page_size
    entities = query.offset(offset).limit(page_size).all()

    return entities, total_count, total_pages


def apply_date_filters(query, model_class, created_after, created_before, updated_after, updated_before):
    """Apply date range filters to a query."""
    if created_after:
        query = query.filter(model_class.created >= created_after)
    if created_before:
        query = query.filter(model_class.created <= created_before)
    if updated_after:
        query = query.filter(model_class.updated >= updated_after)
    if updated_before:
        query = query.filter(model_class.updated <= updated_before)
    return query


def apply_sorting(query, model_class, sort_by, sort_order, sort_fields_map, default_field="created"):
    """Apply sorting to a query based on sort_by and sort_order."""
    # Get the actual field from the map
    sort_field = sort_fields_map.get(sort_by, default_field)

    # Apply sorting
    if sort_order.lower() == "desc":
        query = query.order_by(desc(getattr(model_class, sort_field)))
    else:
        query = query.order_by(asc(getattr(model_class, sort_field)))

    return query