from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from src.core.database import get_db
from src.core.settings import (
    AD_CAMPAIGN_ACTIVE_LIMIT,
    BATCH_SIZE,
    DEFAULT_PAGE,
    MAX_PAGE_SIZE,
    PAGE_SIZE,
)
from src.models.models import AdCampaign, Company
from src.schemas.schemas import (
    AdCampaign as AdCampaignSchema,
    AdCampaignCreate,
    BulkDeleteRequest,
    BulkDeleteResponse,
    MultipleObjectsResponse,
    SingleObjectResponse,
)
from src.utils.helpers import (
    get_ad_campaigns_metadata,
    handle_bulk_delete,
    handle_create_entity,
    handle_get_entity,
    handle_list_entities,
    handle_toggle_entity,
    handle_update_entity,
)
from src.utils.auth import get_current_user_id

router = APIRouter()

campaign_config = {
    "model_class": AdCampaign,
    "schema_class": AdCampaignSchema,
    "create_schema": AdCampaignCreate,
    "entity_name": "campaign",
    "entity_name_plural": "campaigns",
    "active_limit": AD_CAMPAIGN_ACTIVE_LIMIT,
    "id_param": "campaign_id",
    "parent_field": "company_id",
    "parent_model": Company,
    "parent_name": "company",
}

@router.post("/ad_campaigns", response_model=SingleObjectResponse, status_code=201)
async def create_ad_campaign(
    campaign: AdCampaignCreate,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Create a new ad campaign"""
    return handle_create_entity(campaign, db, user_id, campaign_config)

@router.get("/ad_campaigns", response_model=MultipleObjectsResponse)
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
    return handle_list_entities(
        db, user_id, campaign_config, page, page_size, search, is_active,
        created_after, created_before, updated_after, updated_before,
        sort_by, sort_order, get_ad_campaigns_metadata, company_id
    )

@router.get("/ad_campaigns/{campaign_id}", response_model=SingleObjectResponse)
async def get_ad_campaign(
    campaign_id: int,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Get a specific ad campaign by ID"""
    return handle_get_entity(campaign_id, db, user_id, campaign_config)

@router.post("/ad_campaigns/{campaign_id}/update", response_model=SingleObjectResponse)
async def update_ad_campaign(
    campaign_id: int,
    campaign_update: AdCampaignCreate,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Update an ad campaign"""
    return handle_update_entity(campaign_id, campaign_update, db, user_id, campaign_config)

@router.post("/ad_campaigns/bulk/delete", response_model=BulkDeleteResponse)
async def bulk_delete_ad_campaigns(
    delete_data: BulkDeleteRequest,
    batch_size: int = Query(BATCH_SIZE, ge=1, description="Batch size for processing"),
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Bulk delete ad campaigns"""
    return handle_bulk_delete(delete_data, db, user_id, campaign_config, batch_size)

@router.post("/ad_campaigns/{campaign_id}/toggle", response_model=SingleObjectResponse)
async def toggle_ad_campaign_active(
    campaign_id: int,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Toggle is_active status for an ad campaign"""
    return handle_toggle_entity(campaign_id, db, user_id, campaign_config)
