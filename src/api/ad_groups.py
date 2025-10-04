from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from src.core.database import get_db
from src.core.settings import (
    AD_GROUP_ACTIVE_LIMIT,
    BATCH_SIZE,
    DEFAULT_PAGE,
    MAX_PAGE_SIZE,
    PAGE_SIZE,
)
from src.models.models import AdCampaign, AdGroup
from src.schemas.schemas import (
    AdGroup as AdGroupSchema,
    AdGroupCreate,
    BulkDeleteRequest,
    BulkDeleteResponse,
    MultipleObjectsResponse,
    SingleObjectResponse,
)
from src.utils.helpers import (
    get_ad_groups_metadata,
    handle_bulk_delete,
    handle_create_entity,
    handle_get_entity,
    handle_list_entities,
    handle_toggle_entity,
    handle_update_entity,
)
from src.utils.auth import get_current_user_id

router = APIRouter()


router = APIRouter()

ad_group_config = {
    "model_class": AdGroup,
    "schema_class": AdGroupSchema,
    "create_schema": AdGroupCreate,
    "entity_name": "ad group",
    "entity_name_plural": "ad groups",
    "active_limit": AD_GROUP_ACTIVE_LIMIT,
    "id_param": "ad_group_id",
    "parent_field": "ad_campaign_id",
    "parent_model": AdCampaign,
    "parent_name": "ad campaign",
}

@router.post("/ad_groups", response_model=SingleObjectResponse, status_code=201)
async def create_ad_group(
    ad_group: AdGroupCreate,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Create a new ad group"""
    return handle_create_entity(ad_group, db, user_id, ad_group_config)

@router.get("/ad_groups", response_model=MultipleObjectsResponse)
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
    return handle_list_entities(
        db, user_id, ad_group_config, page, page_size, search, is_active,
        created_after, created_before, updated_after, updated_before,
        sort_by, sort_order, get_ad_groups_metadata, ad_campaign_id
    )

@router.get("/ad_groups/{ad_group_id}", response_model=SingleObjectResponse)
async def get_ad_group(
    ad_group_id: int,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Get a specific ad group by ID"""
    return handle_get_entity(ad_group_id, db, user_id, ad_group_config)

@router.post("/ad_groups/{ad_group_id}/update", response_model=SingleObjectResponse)
async def update_ad_group(
    ad_group_id: int,
    ad_group_update: AdGroupCreate,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Update an ad group"""
    return handle_update_entity(ad_group_id, ad_group_update, db, user_id, ad_group_config)

@router.post("/ad_groups/bulk/delete", response_model=BulkDeleteResponse)
async def bulk_delete_ad_groups(
    delete_data: BulkDeleteRequest,
    batch_size: int = Query(BATCH_SIZE, ge=1, description="Batch size for processing"),
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Bulk delete ad groups"""
    return handle_bulk_delete(delete_data, db, user_id, ad_group_config, batch_size)

@router.post("/ad_groups/{ad_group_id}/toggle", response_model=SingleObjectResponse)
async def toggle_ad_group_active(
    ad_group_id: int,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Toggle is_active status for an ad group"""
    return handle_toggle_entity(ad_group_id, db, user_id, ad_group_config)
