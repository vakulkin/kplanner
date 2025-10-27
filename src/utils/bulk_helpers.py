"""
Bulk operation helper functions for KPlanner API.

This module contains utility functions for bulk operations,
including batching and bulk delete/create operations.
"""

from sqlalchemy.orm import Session
from fastapi import HTTPException

from src.core.settings import BATCH_SIZE
from src.schemas.schemas import BulkDeleteResponse


def process_in_batches(items: list):
    """Process items in batches of specified size."""
    batch_size = max(1, BATCH_SIZE)
    for i in range(0, len(items), batch_size):
        yield items[i:i + batch_size]


def bulk_delete_with_batches(
    db: Session,
    user_id: str,
    ids: list[int],
    model_class,
    ownership_field: str,
    message_template: str,
) -> BulkDeleteResponse:
    """Generic helper for bulk delete operations with batching and ownership validation."""
    if not ids:
        raise HTTPException(status_code=400, detail="ids is required")

    deleted_count = 0
    batches_processed = 0

    # Process deletions in batches
    for id_batch in process_in_batches(ids):
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
    )