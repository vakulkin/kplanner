from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from src.core.database import get_db
from src.core.settings import (
    DEFAULT_PAGE,
    MAX_PAGE_SIZE,
    PAGE_SIZE,
)
from src.models.models import Company
from src.schemas.schemas import (
    BulkDeleteRequest,
    BulkDeleteResponse,
    Company as CompanySchema,
    CompanyCreate,
    MultipleObjectsResponse,
    SingleObjectResponse,
)
from src.utils.entity_helpers import (
    handle_bulk_delete,
    handle_create_entity,
    handle_get_entity,
    handle_list_entities,
    handle_update_entity,
)
from src.utils.metadata_helpers import get_companies_metadata
from src.utils.auth import get_current_user_id

router = APIRouter()

company_config = {
    "model_class": Company,
    "schema_class": CompanySchema,
    "create_schema": CompanyCreate,
    "entity_name": "company",
    "entity_name_plural": "companies",
    "id_param": "company_id",
    "parent_field": None,
    "parent_model": None,
    "parent_name": None,
}

@router.post("/companies", response_model=SingleObjectResponse, status_code=201)
async def create_company(
    company: CompanyCreate,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Create a new company"""
    return handle_create_entity(company, db, user_id, company_config)

@router.get("/companies", response_model=MultipleObjectsResponse)
async def list_companies(
    page: int = Query(DEFAULT_PAGE, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE, description=f"Items per page (max {MAX_PAGE_SIZE})"),
    search: Optional[str] = Query(None, description="Search by company title (case-insensitive, partial match)"),
    created_after: Optional[datetime] = Query(None, description="Filter by created date (after)"),
    created_before: Optional[datetime] = Query(None, description="Filter by created date (before)"),
    updated_after: Optional[datetime] = Query(None, description="Filter by updated date (after)"),
    updated_before: Optional[datetime] = Query(None, description="Filter by updated date (before)"),
    sort_by: Optional[str] = Query("created", description="Sort by field: id, title, created, updated"),
    sort_order: Optional[str] = Query("desc", description="Sort order: asc or desc"),
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """List all companies for the authenticated user with pagination, filters, and sorting"""
    return handle_list_entities(
        db, user_id, company_config, page, page_size, search, None,
        created_after, created_before, updated_after, updated_before,
        sort_by, sort_order, get_companies_metadata
    )

@router.get("/companies/{company_id}", response_model=SingleObjectResponse)
async def get_company(
    company_id: int,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Get a specific company by ID"""
    return handle_get_entity(company_id, db, user_id, company_config)

@router.post("/companies/{company_id}/update", response_model=SingleObjectResponse)
async def update_company(
    company_id: int,
    company_update: CompanyCreate,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Update a company"""
    return handle_update_entity(company_id, company_update, db, user_id, company_config)

@router.post("/companies/bulk/delete", response_model=BulkDeleteResponse)
async def bulk_delete_companies(
    delete_data: BulkDeleteRequest,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Bulk delete companies"""
    return handle_bulk_delete(delete_data, db, user_id, company_config)
