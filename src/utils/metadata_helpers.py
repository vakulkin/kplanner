"""
Metadata helper functions for KPlanner API.

This module contains utility functions for generating API metadata,
including filters and sorting options for different endpoints.
"""


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
            "description": "Filter keywords with at least one broad match relation (True=positive, False=negative)",
            "available_values": [True, False]
        },
        "has_phrase": {
            "type": "boolean",
            "description": "Filter keywords with at least one phrase match relation (True=positive, False=negative)",
            "available_values": [True, False]
        },
        "has_exact": {
            "type": "boolean",
            "description": "Filter keywords with at least one exact match relation (True=positive, False=negative)",
            "available_values": [True, False]
        },
        "trash": {
            "type": "boolean",
            "description": "Filter by trash status (True=trashed, False=not trashed, None=all)",
            "available_values": [True, False]
        }
    }

    sorting = {
        "sort_by": {
            "type": "string",
            "description": "Primary sort field",
            "available_values": ["id", "keyword", "created", "updated", "has_broad", "has_phrase", "has_exact", "trash"],
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
            "available_values": ["id", "keyword", "created", "updated", "has_broad", "has_phrase", "has_exact", "trash"]
        },
        "sort_order_2": {
            "type": "string",
            "description": "Secondary sort direction",
            "available_values": ["asc", "desc"]
        },
        "sort_by_3": {
            "type": "string",
            "description": "Tertiary sort field (optional)",
            "available_values": ["id", "keyword", "created", "updated", "has_broad", "has_phrase", "has_exact", "trash"]
        },
        "sort_order_3": {
            "type": "string",
            "description": "Tertiary sort direction",
            "available_values": ["asc", "desc"]
        }
    }

    return filters, sorting
