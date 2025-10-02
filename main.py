from fastapi import FastAPI, Depends, HTTPException, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import Optional
import os
import httpx
from clerk_backend_api import Clerk
from clerk_backend_api.security.types import AuthenticateRequestOptions
import models
from database import engine, get_db
from schemas import (
    Company, AdCampaign, AdGroup, Keyword, Filter,
    BulkKeywordCreate, BulkKeywordUpdateRelations, BulkKeywordCreateRelations,
    BulkFilterCreate, BulkFilterUpdateRelations, BulkFilterCreateRelations,
    BulkDeleteRequest, MatchTypes,
    SingleObjectResponse, MultipleObjectsResponse, BulkOperationResponse
)
import math

# Create tables
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

# Initialize Clerk SDK (only if not in dev mode)
clerk_sdk = None
if not DEV_MODE:
    clerk_secret_key = os.getenv("CLERK_SECRET_KEY")
    if not clerk_secret_key:
        raise ValueError("CLERK_SECRET_KEY environment variable is required when DEV_MODE is not enabled")
    clerk_sdk = Clerk(bearer_auth=clerk_secret_key)


# Dependency to get authenticated user ID from Clerk
async def get_current_user_id(request: Request) -> str:
    """
    Authenticate request using Clerk and extract user ID from token.
    In DEV_MODE, always returns 'clerk_demo_user' without requiring authentication.
    Raises HTTPException if authentication fails (production mode).
    """
    # In dev mode, skip authentication and use demo user
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
def paginate_query(query, page: int = 1, page_size: int = 100):
    """
    Apply pagination to a SQLAlchemy query.
    
    Args:
        query: SQLAlchemy query object
        page: Page number (1-indexed)
        page_size: Number of items per page (max 100)
    
    Returns:
        Tuple of (paginated_items, total_count, total_pages)
    """
    # Validate and limit page_size
    page_size = min(max(1, page_size), 100)  # Between 1 and 100
    page = max(1, page)  # At least page 1
    
    # Get total count
    total_count = query.count()
    
    # Calculate total pages
    total_pages = math.ceil(total_count / page_size) if total_count > 0 else 1
    
    # Apply pagination
    offset = (page - 1) * page_size
    items = query.offset(offset).limit(page_size).all()
    
    return items, total_count, total_pages


# Helper function for batch processing
def process_in_batches(items: list, batch_size: int = 25):
    """
    Split a list into batches for processing.
    
    Args:
        items: List of items to process
        batch_size: Maximum size of each batch (default 25, max 100)
    
    Yields:
        Batches of items
    """
    batch_size = min(max(1, batch_size), 100)  # Between 1 and 100
    
    for i in range(0, len(items), batch_size):
        yield items[i:i + batch_size]


@app.get("/")
async def root():
    return {
        "message": "Welcome to KPlanner API",
        "mode": "development" if DEV_MODE else "production",
        "demo_user": DEMO_USER_ID if DEV_MODE else None
    }

# Company endpoints
@app.post("/companies", response_model=SingleObjectResponse, status_code=201)
async def create_company(
    company: Company,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Create a new company"""
    db_company = models.Company(
        title=company.title,
        clerk_user_id=user_id
    )
    db.add(db_company)
    db.commit()
    db.refresh(db_company)
    return SingleObjectResponse(
        status="success",
        message="Company created successfully",
        object=Company.model_validate(db_company)
    )

@app.get("/companies", response_model=MultipleObjectsResponse)
async def list_companies(
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(100, ge=1, le=100, description="Items per page (max 100)"),
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """List all companies for the authenticated user with pagination"""
    query = db.query(models.Company).filter(models.Company.clerk_user_id == user_id)
    companies, total_count, total_pages = paginate_query(query, page, page_size)
    
    return MultipleObjectsResponse(
        status="success",
        objects=[Company.model_validate(c) for c in companies],
        total=total_count,
        page=page,
        page_size=page_size,
        total_pages=total_pages
    )

@app.get("/companies/{company_id}", response_model=SingleObjectResponse)
async def get_company(
    company_id: int,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Get a specific company by ID"""
    company = db.query(models.Company).filter(
        models.Company.id == company_id,
        models.Company.clerk_user_id == user_id
    ).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    return SingleObjectResponse(
        status="success",
        object=Company.model_validate(company),
        id=company.id
    )


@app.post("/companies/{company_id}/update", response_model=BulkOperationResponse)
async def update_company(
    company_id: int,
    company_update: Company,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Update a company"""
    company = db.query(models.Company).filter(
        models.Company.id == company_id,
        models.Company.clerk_user_id == user_id
    ).first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    
    company.title = company_update.title
    db.commit()
    db.refresh(company)
    return BulkOperationResponse(
        status="success",
        message="Company updated successfully",
        object={
            "id": company.id,
            "title": company.title,
            "clerk_user_id": company.clerk_user_id,
            "created": company.created.isoformat(),
            "updated": company.updated.isoformat()
        },
        id=company.id
    )


@app.post("/companies/bulk/delete", response_model=BulkOperationResponse)
async def bulk_delete_companies(
    delete_data: BulkDeleteRequest,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Bulk delete companies"""
    if not delete_data.ids:
        raise HTTPException(status_code=400, detail="ids is required")
    
    deleted_count = db.query(models.Company).filter(
        models.Company.id.in_(delete_data.ids),
        models.Company.clerk_user_id == user_id
    ).delete(synchronize_session=False)
    
    db.commit()
    
    return BulkOperationResponse(
        status="success",
        message=f"Deleted {deleted_count} companies",
        deleted=deleted_count,
        requested=len(delete_data.ids)
    )

# Ad Campaign endpoints
@app.post("/ad_campaigns", response_model=SingleObjectResponse, status_code=201)
async def create_ad_campaign(
    campaign: AdCampaign,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Create a new ad campaign"""
    # Validate company_id if provided
    if campaign.company_id:
        company = db.query(models.Company).filter(
            models.Company.id == campaign.company_id,
            models.Company.clerk_user_id == user_id
        ).first()
        if not company:
            raise HTTPException(status_code=404, detail="Company not found")
    
    db_campaign = models.AdCampaign(
        title=campaign.title,
        clerk_user_id=user_id,
        company_id=campaign.company_id
    )
    db.add(db_campaign)
    db.commit()
    db.refresh(db_campaign)
    return SingleObjectResponse(
        status="success",
        message="Campaign created successfully",
        object=AdCampaign.model_validate(db_campaign)
    )

@app.get("/ad_campaigns", response_model=MultipleObjectsResponse)
async def list_ad_campaigns(
    company_id: Optional[int] = None,
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(100, ge=1, le=100, description="Items per page (max 100)"),
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """List all ad campaigns for the authenticated user with pagination"""
    filters = [models.AdCampaign.clerk_user_id == user_id]
    if company_id is not None:
        filters.append(models.AdCampaign.company_id == company_id)
    query = db.query(models.AdCampaign).filter(*filters)
    campaigns, total_count, total_pages = paginate_query(query, page, page_size)
    
    return MultipleObjectsResponse(
        status="success",
        objects=[AdCampaign.model_validate(c) for c in campaigns],
        total=total_count,
        page=page,
        page_size=page_size,
        total_pages=total_pages
    )

@app.get("/ad_campaigns/{campaign_id}", response_model=SingleObjectResponse)
async def get_ad_campaign(
    campaign_id: int,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Get a specific ad campaign by ID"""
    campaign = db.query(models.AdCampaign).filter(
        models.AdCampaign.id == campaign_id,
        models.AdCampaign.clerk_user_id == user_id
    ).first()
    if not campaign:
        raise HTTPException(status_code=404, detail="Ad campaign not found")
    return SingleObjectResponse(
        status="success",
        object=AdCampaign.model_validate(campaign),
        id=campaign.id
    )


@app.post("/ad_campaigns/{campaign_id}/update", response_model=BulkOperationResponse)
async def update_ad_campaign(
    campaign_id: int,
    campaign_update: AdCampaign,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Update an ad campaign"""
    campaign = db.query(models.AdCampaign).filter(
        models.AdCampaign.id == campaign_id,
        models.AdCampaign.clerk_user_id == user_id
    ).first()
    if not campaign:
        raise HTTPException(status_code=404, detail="Ad campaign not found")
    
    # Validate company_id if provided
    if campaign_update.company_id:
        company = db.query(models.Company).filter(
            models.Company.id == campaign_update.company_id,
            models.Company.clerk_user_id == user_id
        ).first()
        if not company:
            raise HTTPException(status_code=404, detail="Company not found")
    
    campaign.title = campaign_update.title
    campaign.company_id = campaign_update.company_id
    db.commit()
    db.refresh(campaign)
    return BulkOperationResponse(
        status="success",
        message="Campaign updated successfully",
        object={
            "id": campaign.id,
            "title": campaign.title,
            "clerk_user_id": campaign.clerk_user_id,
            "company_id": campaign.company_id,
            "created": campaign.created.isoformat(),
            "updated": campaign.updated.isoformat()
        },
        id=campaign.id
    )


@app.post("/ad_campaigns/bulk/delete", response_model=BulkOperationResponse)
async def bulk_delete_ad_campaigns(
    delete_data: BulkDeleteRequest,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Bulk delete ad campaigns"""
    if not delete_data.ids:
        raise HTTPException(status_code=400, detail="ids is required")
    
    deleted_count = db.query(models.AdCampaign).filter(
        models.AdCampaign.id.in_(delete_data.ids),
        models.AdCampaign.clerk_user_id == user_id
    ).delete(synchronize_session=False)
    
    db.commit()
    
    return BulkOperationResponse(
        status="success",
        message=f"Deleted {deleted_count} campaigns",
        deleted=deleted_count,
        requested=len(delete_data.ids)
    )

# Ad Group endpoints
@app.post("/ad_groups", response_model=SingleObjectResponse, status_code=201)
async def create_ad_group(
    ad_group: AdGroup,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Create a new ad group"""
    # Validate ad_campaign_id if provided
    if ad_group.ad_campaign_id:
        campaign = db.query(models.AdCampaign).filter(
            models.AdCampaign.id == ad_group.ad_campaign_id,
            models.AdCampaign.clerk_user_id == user_id
        ).first()
        if not campaign:
            raise HTTPException(status_code=404, detail="Ad campaign not found")
    
    db_ad_group = models.AdGroup(
        title=ad_group.title,
        clerk_user_id=user_id,
        ad_campaign_id=ad_group.ad_campaign_id
    )
    db.add(db_ad_group)
    db.commit()
    db.refresh(db_ad_group)
    return SingleObjectResponse(
        status="success",
        message="Ad group created successfully",
        object=AdGroup.model_validate(db_ad_group)
    )

@app.get("/ad_groups", response_model=MultipleObjectsResponse)
async def list_ad_groups(
    ad_campaign_id: Optional[int] = None,
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(100, ge=1, le=100, description="Items per page (max 100)"),
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """List all ad groups for the authenticated user with pagination"""
    filters = [models.AdGroup.clerk_user_id == user_id]
    if ad_campaign_id is not None:
        filters.append(models.AdGroup.ad_campaign_id == ad_campaign_id)
    query = db.query(models.AdGroup).filter(*filters)
    ad_groups, total_count, total_pages = paginate_query(query, page, page_size)
    
    return MultipleObjectsResponse(
        status="success",
        objects=[AdGroup.model_validate(g) for g in ad_groups],
        total=total_count,
        page=page,
        page_size=page_size,
        total_pages=total_pages
    )

@app.get("/ad_groups/{ad_group_id}", response_model=SingleObjectResponse)
async def get_ad_group(
    ad_group_id: int,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Get a specific ad group by ID"""
    ad_group = db.query(models.AdGroup).filter(
        models.AdGroup.id == ad_group_id,
        models.AdGroup.clerk_user_id == user_id
    ).first()
    if not ad_group:
        raise HTTPException(status_code=404, detail="Ad group not found")
    return SingleObjectResponse(
        status="success",
        object=AdGroup.model_validate(ad_group),
        id=ad_group.id
    )


@app.post("/ad_groups/{ad_group_id}/update", response_model=BulkOperationResponse)
async def update_ad_group(
    ad_group_id: int,
    ad_group_update: AdGroup,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Update an ad group"""
    ad_group = db.query(models.AdGroup).filter(
        models.AdGroup.id == ad_group_id,
        models.AdGroup.clerk_user_id == user_id
    ).first()
    if not ad_group:
        raise HTTPException(status_code=404, detail="Ad group not found")
    
    # Validate ad_campaign_id if provided
    if ad_group_update.ad_campaign_id:
        campaign = db.query(models.AdCampaign).filter(
            models.AdCampaign.id == ad_group_update.ad_campaign_id,
            models.AdCampaign.clerk_user_id == user_id
        ).first()
        if not campaign:
            raise HTTPException(status_code=404, detail="Ad campaign not found")
    
    ad_group.title = ad_group_update.title
    ad_group.ad_campaign_id = ad_group_update.ad_campaign_id
    db.commit()
    db.refresh(ad_group)
    return BulkOperationResponse(
        status="success",
        message="Ad group updated successfully",
        object={
            "id": ad_group.id,
            "title": ad_group.title,
            "clerk_user_id": ad_group.clerk_user_id,
            "ad_campaign_id": ad_group.ad_campaign_id,
            "created": ad_group.created.isoformat(),
            "updated": ad_group.updated.isoformat()
        },
        id=ad_group.id
    )


@app.post("/ad_groups/bulk/delete", response_model=BulkOperationResponse)
async def bulk_delete_ad_groups(
    delete_data: BulkDeleteRequest,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Bulk delete ad groups"""
    if not delete_data.ids:
        raise HTTPException(status_code=400, detail="ids is required")
    
    deleted_count = db.query(models.AdGroup).filter(
        models.AdGroup.id.in_(delete_data.ids),
        models.AdGroup.clerk_user_id == user_id
    ).delete(synchronize_session=False)
    
    db.commit()
    
    return BulkOperationResponse(
        status="success",
        message=f"Deleted {deleted_count} ad groups",
        deleted=deleted_count,
        requested=len(delete_data.ids)
    )


# Helper functions for keyword operations
def _validate_entity_ownership(
    db: Session,
    user_id: str,
    company_ids: Optional[list[int]] = None,
    ad_campaign_ids: Optional[list[int]] = None,
    ad_group_ids: Optional[list[int]] = None
) -> None:
    """
    Validate that all provided entity IDs belong to the user.
    Raises HTTPException if validation fails.
    """
    # Validate ownership of companies if provided
    if company_ids:
        companies = db.query(models.Company).filter(
            models.Company.id.in_(company_ids),
            models.Company.clerk_user_id == user_id
        ).all()
        if len(companies) != len(company_ids):
            raise HTTPException(
                status_code=404,
                detail="One or more companies not found or not owned by user"
            )
    
    # Validate ownership of campaigns if provided
    if ad_campaign_ids:
        campaigns = db.query(models.AdCampaign).filter(
            models.AdCampaign.id.in_(ad_campaign_ids),
            models.AdCampaign.clerk_user_id == user_id
        ).all()
        if len(campaigns) != len(ad_campaign_ids):
            raise HTTPException(
                status_code=404,
                detail="One or more campaigns not found or not owned by user"
            )
    
    # Validate ownership of ad groups if provided
    if ad_group_ids:
        ad_groups = db.query(models.AdGroup).filter(
            models.AdGroup.id.in_(ad_group_ids),
            models.AdGroup.clerk_user_id == user_id
        ).all()
        if len(ad_groups) != len(ad_group_ids):
            raise HTTPException(
                status_code=404,
                detail="One or more ad groups not found or not owned by user"
            )


def _validate_keywords_ownership(
    db: Session,
    user_id: str,
    keyword_ids: list[int]
) -> list:
    """
    Validate that all provided keyword IDs belong to the user.
    Returns list of keyword objects.
    Raises HTTPException if validation fails.
    """
    keywords = db.query(models.Keyword).filter(
        models.Keyword.id.in_(keyword_ids),
        models.Keyword.clerk_user_id == user_id
    ).all()
    
    if len(keywords) != len(keyword_ids):
        raise HTTPException(
            status_code=404,
            detail="One or more keywords not found or not owned by user"
        )
    
    return keywords


# Keyword endpoints
@app.post("/keywords/bulk", response_model=BulkOperationResponse, status_code=201)
async def create_bulk_keywords(
    bulk_data: BulkKeywordCreate,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """
    Bulk create keywords with optional relations to companies, campaigns, and ad groups.
    
    - Keywords are created (or retrieved if they already exist)
    - Optionally associate with companies, campaigns, and/or ad groups
    - Apply same match types to all relations
    """
    
    # Validate ownership of all entities
    _validate_entity_ownership(
        db=db,
        user_id=user_id,
        company_ids=bulk_data.company_ids,
        ad_campaign_ids=bulk_data.ad_campaign_ids,
        ad_group_ids=bulk_data.ad_group_ids
    )
    
    # Default match types if not provided
    match_types = bulk_data.match_types or MatchTypes()
    
    created_keywords = []
    existing_keywords = []
    
    for keyword_text in bulk_data.keywords:
        keyword_text = keyword_text.strip()
        if not keyword_text:
            continue
            
        # Try to get existing keyword or create new one
        keyword = db.query(models.Keyword).filter(
            models.Keyword.keyword == keyword_text,
            models.Keyword.clerk_user_id == user_id
        ).first()
        
        if keyword:
            existing_keywords.append(keyword)
        else:
            keyword = models.Keyword(
                keyword=keyword_text,
                clerk_user_id=user_id
            )
            db.add(keyword)
            db.flush()  # Get the ID without committing
            created_keywords.append(keyword)
        
        # Create relations using helper function
        _create_keyword_relations(
            db=db,
            keyword=keyword,
            company_ids=bulk_data.company_ids,
            ad_campaign_ids=bulk_data.ad_campaign_ids,
            ad_group_ids=bulk_data.ad_group_ids,
            match_types=match_types,
            override_broad=bulk_data.override_broad,
            override_phrase=bulk_data.override_phrase,
            override_exact=bulk_data.override_exact,
            override_neg_broad=bulk_data.override_neg_broad,
            override_neg_phrase=bulk_data.override_neg_phrase,
            override_neg_exact=bulk_data.override_neg_exact
        )
    
    # Commit all changes
    db.commit()
    
    # Calculate totals
    all_keywords = created_keywords + existing_keywords
    
    return BulkOperationResponse(
        status="success",
        message=f"Created {len(created_keywords)} new keywords, found {len(existing_keywords)} existing",
        created=len(created_keywords),
        existing=len(existing_keywords),
        total=len(all_keywords)
    )


def _update_relation_match_types(assoc, update_data: BulkKeywordUpdateRelations) -> bool:
    """
    Helper function to update match types on a relation object.
    Returns True if any field was updated, False otherwise.
    """
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
    company_ids: Optional[list[int]],
    ad_campaign_ids: Optional[list[int]],
    ad_group_ids: Optional[list[int]],
    match_types: MatchTypes,
    override_broad: bool = False,
    override_phrase: bool = False,
    override_exact: bool = False,
    override_neg_broad: bool = False,
    override_neg_phrase: bool = False,
    override_neg_exact: bool = False
) -> tuple[int, int]:
    """
    Helper function to create relations for a keyword.
    
    Args:
        db: Database session
        keyword: Keyword object
        company_ids: List of company IDs to associate
        ad_campaign_ids: List of campaign IDs to associate
        ad_group_ids: List of ad group IDs to associate
        match_types: Match types to apply
        override_broad: If True, update broad match type for existing relations
        override_phrase: If True, update phrase match type for existing relations
        override_exact: If True, update exact match type for existing relations
        override_neg_broad: If True, update neg_broad match type for existing relations
        override_neg_phrase: If True, update neg_phrase match type for existing relations
        override_neg_exact: If True, update neg_exact match type for existing relations
    
    Returns:
        Tuple of (relations_added, relations_updated)
    """
    
    def _process_entity_relations(
        entity_ids: list[int],
        model_class,
        entity_id_field: str
    ) -> tuple[int, int]:
        """
        Helper to process relations for a specific entity type.
        Returns (added_count, updated_count)
        """
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
                    existing.broad = match_types.broad
                    relation_updated = True
                if override_phrase:
                    existing.phrase = match_types.phrase
                    relation_updated = True
                if override_exact:
                    existing.exact = match_types.exact
                    relation_updated = True
                if override_neg_broad:
                    existing.neg_broad = match_types.neg_broad
                    relation_updated = True
                if override_neg_phrase:
                    existing.neg_phrase = match_types.neg_phrase
                    relation_updated = True
                if override_neg_exact:
                    existing.neg_exact = match_types.neg_exact
                    relation_updated = True
                
                if relation_updated:
                    updated += 1
            else:
                # Create new relation
                create_kwargs = {
                    entity_id_field: entity_id,
                    'keyword_id': keyword.id,
                    'broad': match_types.broad,
                    'phrase': match_types.phrase,
                    'exact': match_types.exact,
                    'neg_broad': match_types.neg_broad,
                    'neg_phrase': match_types.neg_phrase,
                    'neg_exact': match_types.neg_exact
                }
                new_relation = model_class(**create_kwargs)
                db.add(new_relation)
                added += 1
        
        return added, updated
    
    relations_added = 0
    relations_updated = 0
    
    # Handle company relations
    if company_ids:
        added, updated = _process_entity_relations(
            company_ids, 
            models.CompanyKeyword, 
            'company_id'
        )
        relations_added += added
        relations_updated += updated
    
    # Handle campaign relations
    if ad_campaign_ids:
        added, updated = _process_entity_relations(
            ad_campaign_ids,
            models.AdCampaignKeyword,
            'ad_campaign_id'
        )
        relations_added += added
        relations_updated += updated
    
    # Handle ad group relations
    if ad_group_ids:
        added, updated = _process_entity_relations(
            ad_group_ids,
            models.AdGroupKeyword,
            'ad_group_id'
        )
        relations_added += added
        relations_updated += updated
    
    return relations_added, relations_updated


@app.post("/keywords/bulk/relations/update", response_model=BulkOperationResponse)
async def bulk_update_keyword_relations(
    update_data: BulkKeywordUpdateRelations,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """
    Bulk update match types for existing keyword relations.
    
    Updates match types for ALL existing relations of the specified keywords
    (companies, campaigns, ad groups) based on the override flags.
    
    Processes keywords in batches of 25 to avoid memory issues.
    Maximum 100 keywords per request.
    
    For each match type field (broad, phrase, exact, neg_broad, neg_phrase, neg_exact):
    - If override_{field}=True: Update the field to the provided value
    - If override_{field}=False: Leave the field unchanged
    
    Example: To enable 'broad' and disable 'exact' for all relations:
    {
        "keyword_ids": [1, 2, 3],
        "broad": true,
        "exact": false,
        "override_broad": true,
        "override_exact": true
    }
    """
    
    # Limit to 100 keywords per request
    if len(update_data.keyword_ids) > 100:
        raise HTTPException(
            status_code=400,
            detail="Maximum 100 keywords allowed per request"
        )
    
    # Validate keywords belong to user
    keywords = _validate_keywords_ownership(db, user_id, update_data.keyword_ids)
    
    relations_updated = 0
    batches_processed = 0
    
    # Process keywords in batches of 25
    for keyword_batch in process_in_batches(keywords, batch_size=25):
        # Process each keyword in the batch
        for keyword in keyword_batch:
            # Update company relations
            company_relations = db.query(models.CompanyKeyword).filter(
                models.CompanyKeyword.keyword_id == keyword.id
            ).all()
            
            for assoc in company_relations:
                if _update_relation_match_types(assoc, update_data):
                    relations_updated += 1
            
            # Update campaign relations
            campaign_relations = db.query(models.AdCampaignKeyword).filter(
                models.AdCampaignKeyword.keyword_id == keyword.id
            ).all()
            
            for assoc in campaign_relations:
                if _update_relation_match_types(assoc, update_data):
                    relations_updated += 1
            
            # Update ad group relations
            ad_group_relations = db.query(models.AdGroupKeyword).filter(
                models.AdGroupKeyword.keyword_id == keyword.id
            ).all()
            
            for assoc in ad_group_relations:
                if _update_relation_match_types(assoc, update_data):
                    relations_updated += 1
        
        # Commit after each batch
        db.commit()
        batches_processed += 1
    
    return BulkOperationResponse(
        status="success",
        message=f"Updated {relations_updated} relations for {len(keywords)} keywords in {batches_processed} batches",
        updated=len(keywords),
        relations_added=0,
        relations_updated=relations_updated,
        batches_processed=batches_processed,
        batch_size=25
    )


@app.post("/keywords/bulk/relations", response_model=BulkOperationResponse)
async def bulk_create_keyword_relations(
    create_data: BulkKeywordCreateRelations,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """
    Create new relations for existing keywords with companies, campaigns, or ad groups.
    
    - Creates new relations if they don't exist
    - For existing relations: Updates match types only for fields where override_{field}=True
    - For fields where override_{field}=False: Keeps existing values
    
    Example: Associate keywords 1, 2, 3 with companies 5, 6 with broad and phrase match,
    and update existing relations' broad match type:
    {
        "keyword_ids": [1, 2, 3],
        "company_ids": [5, 6],
        "match_types": {
            "broad": true,
            "phrase": true
        },
        "override_broad": true
    }
    """
    
    # Validate keywords belong to user
    keywords = _validate_keywords_ownership(db, user_id, create_data.keyword_ids)
    
    # Validate ownership of all entities
    _validate_entity_ownership(
        db=db,
        user_id=user_id,
        company_ids=create_data.company_ids,
        ad_campaign_ids=create_data.ad_campaign_ids,
        ad_group_ids=create_data.ad_group_ids
    )
    
    # Default match types if not provided
    match_types = create_data.match_types or MatchTypes()
    
    total_relations_added = 0
    total_relations_updated = 0
    
    # Process each keyword
    for keyword in keywords:
        added, updated = _create_keyword_relations(
            db=db,
            keyword=keyword,
            company_ids=create_data.company_ids,
            ad_campaign_ids=create_data.ad_campaign_ids,
            ad_group_ids=create_data.ad_group_ids,
            match_types=match_types,
            override_broad=create_data.override_broad,
            override_phrase=create_data.override_phrase,
            override_exact=create_data.override_exact,
            override_neg_broad=create_data.override_neg_broad,
            override_neg_phrase=create_data.override_neg_phrase,
            override_neg_exact=create_data.override_neg_exact
        )
        total_relations_added += added
        total_relations_updated += updated
    
    # Commit all changes
    db.commit()
    
    return BulkOperationResponse(
        status="success",
        message=f"Processed {len(keywords)} keywords: added {total_relations_added} relations, updated {total_relations_updated}",
        processed=len(keywords),
        relations_added=total_relations_added,
        relations_updated=total_relations_updated
    )


@app.get("/keywords", response_model=MultipleObjectsResponse)
async def list_keywords(
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(100, ge=1, le=100, description="Items per page (max 100)"),
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """List all keywords for the authenticated user with pagination"""
    query = db.query(models.Keyword).filter(models.Keyword.clerk_user_id == user_id)
    keywords, total_count, total_pages = paginate_query(query, page, page_size)
    
    return MultipleObjectsResponse(
        status="success",
        objects=[Keyword.model_validate(k) for k in keywords],
        total=total_count,
        page=page,
        page_size=page_size,
        total_pages=total_pages
    )


@app.get("/keywords/{keyword_id}", response_model=SingleObjectResponse)
async def get_keyword(
    keyword_id: int,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Get a specific keyword by ID"""
    keyword = db.query(models.Keyword).filter(
        models.Keyword.id == keyword_id,
        models.Keyword.clerk_user_id == user_id
    ).first()
    if not keyword:
        raise HTTPException(status_code=404, detail="Keyword not found")
    return SingleObjectResponse(
        status="success",
        object=Keyword.model_validate(keyword),
        id=keyword.id
    )


@app.post("/keywords/{keyword_id}/update", response_model=BulkOperationResponse)
async def update_keyword(
    keyword_id: int,
    keyword_update: Keyword,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Update a keyword"""
    keyword = db.query(models.Keyword).filter(
        models.Keyword.id == keyword_id,
        models.Keyword.clerk_user_id == user_id
    ).first()
    if not keyword:
        raise HTTPException(status_code=404, detail="Keyword not found")
    
    keyword.keyword = keyword_update.keyword
    db.commit()
    db.refresh(keyword)
    return BulkOperationResponse(
        status="success",
        message="Keyword updated successfully",
        object={
            "id": keyword.id,
            "keyword": keyword.keyword,
            "clerk_user_id": keyword.clerk_user_id,
            "created": keyword.created.isoformat(),
            "updated": keyword.updated.isoformat()
        },
        id=keyword.id
    )


@app.post("/keywords/bulk/delete", response_model=BulkOperationResponse)
async def bulk_delete_keywords(
    delete_data: BulkDeleteRequest,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Bulk delete keywords"""
    if not delete_data.ids:
        raise HTTPException(status_code=400, detail="ids is required")
    
    deleted_count = db.query(models.Keyword).filter(
        models.Keyword.id.in_(delete_data.ids),
        models.Keyword.clerk_user_id == user_id
    ).delete(synchronize_session=False)
    
    db.commit()
    
    return BulkOperationResponse(
        status="success",
        message=f"Deleted {deleted_count} keywords",
        deleted=deleted_count,
        requested=len(delete_data.ids)
    )


@app.post("/relations/company-keyword/bulk/delete", response_model=BulkOperationResponse)
async def bulk_delete_company_keyword_relations(
    delete_data: BulkDeleteRequest,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Bulk delete company-keyword relations"""
    if not delete_data.ids:
        raise HTTPException(status_code=400, detail="ids is required")
    
    deleted_count = db.query(models.CompanyKeyword).join(
        models.Keyword
    ).filter(
        models.CompanyKeyword.id.in_(delete_data.ids),
        models.Keyword.clerk_user_id == user_id
    ).delete(synchronize_session=False)
    
    db.commit()
    
    return BulkOperationResponse(
        status="success",
        message=f"Deleted {deleted_count} company-keyword relations",
        deleted=deleted_count,
        requested=len(delete_data.ids)
    )


@app.post("/relations/campaign-keyword/bulk/delete", response_model=BulkOperationResponse)
async def bulk_delete_campaign_keyword_relations(
    delete_data: BulkDeleteRequest,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Bulk delete campaign-keyword relations"""
    if not delete_data.ids:
        raise HTTPException(status_code=400, detail="ids is required")
    
    deleted_count = db.query(models.AdCampaignKeyword).join(
        models.Keyword
    ).filter(
        models.AdCampaignKeyword.id.in_(delete_data.ids),
        models.Keyword.clerk_user_id == user_id
    ).delete(synchronize_session=False)
    
    db.commit()
    
    return BulkOperationResponse(
        status="success",
        message=f"Deleted {deleted_count} campaign-keyword relations",
        deleted=deleted_count,
        requested=len(delete_data.ids)
    )


@app.post("/relations/adgroup-keyword/bulk/delete", response_model=BulkOperationResponse)
async def bulk_delete_adgroup_keyword_relations(
    delete_data: BulkDeleteRequest,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Bulk delete ad group-keyword relations"""
    if not delete_data.ids:
        raise HTTPException(status_code=400, detail="ids is required")
    
    deleted_count = db.query(models.AdGroupKeyword).join(
        models.Keyword
    ).filter(
        models.AdGroupKeyword.id.in_(delete_data.ids),
        models.Keyword.clerk_user_id == user_id
    ).delete(synchronize_session=False)
    
    db.commit()
    
    return BulkOperationResponse(
        status="success",
        message=f"Deleted {deleted_count} adgroup-keyword relations",
        deleted=deleted_count,
        requested=len(delete_data.ids)
    )


# Filter helper functions
def _validate_filters_ownership(
    db: Session,
    user_id: str,
    filter_ids: list[int]
) -> list:
    """
    Validate that all provided filter IDs belong to the user.
    Returns list of filter objects.
    Raises HTTPException if validation fails.
    """
    filters = db.query(models.Filter).filter(
        models.Filter.id.in_(filter_ids),
        models.Filter.clerk_user_id == user_id
    ).all()
    
    if len(filters) != len(filter_ids):
        raise HTTPException(
            status_code=404,
            detail="One or more filters not found or not owned by user"
        )
    
    return filters


def _create_filter_relations(
    db: Session,
    filter_obj,
    company_ids: Optional[list[int]],
    ad_campaign_ids: Optional[list[int]],
    ad_group_ids: Optional[list[int]],
    is_negative: bool = False
) -> tuple[int, int]:
    """
    Helper function to create relations for a filter.
    
    Args:
        db: Database session
        filter_obj: Filter object
        company_ids: List of company IDs to associate
        ad_campaign_ids: List of campaign IDs to associate
        ad_group_ids: List of ad group IDs to associate
        is_negative: Whether this is a negative filter
    
    Returns:
        Tuple of (relations_added, relations_updated)
    """
    
    def _process_entity_relations(
        entity_ids: list[int],
        model_class,
        entity_id_field: str
    ) -> tuple[int, int]:
        """
        Helper to process relations for a specific entity type.
        Returns (added_count, updated_count)
        """
        added = 0
        updated = 0
        
        for entity_id in entity_ids:
            # Query for existing relation
            filter_kwargs = {
                entity_id_field: entity_id,
                'filter_id': filter_obj.id
            }
            existing = db.query(model_class).filter_by(**filter_kwargs).first()
            
            if existing:
                # Update existing relation
                if existing.is_negative != is_negative:
                    existing.is_negative = is_negative
                    updated += 1
            else:
                # Create new relation
                create_kwargs = {
                    entity_id_field: entity_id,
                    'filter_id': filter_obj.id,
                    'is_negative': is_negative
                }
                new_relation = model_class(**create_kwargs)
                db.add(new_relation)
                added += 1
        
        return added, updated
    
    relations_added = 0
    relations_updated = 0
    
    # Handle company relations
    if company_ids:
        added, updated = _process_entity_relations(
            company_ids, 
            models.CompanyFilter, 
            'company_id'
        )
        relations_added += added
        relations_updated += updated
    
    # Handle campaign relations
    if ad_campaign_ids:
        added, updated = _process_entity_relations(
            ad_campaign_ids,
            models.AdCampaignFilter,
            'ad_campaign_id'
        )
        relations_added += added
        relations_updated += updated
    
    # Handle ad group relations
    if ad_group_ids:
        added, updated = _process_entity_relations(
            ad_group_ids,
            models.AdGroupFilter,
            'ad_group_id'
        )
        relations_added += added
        relations_updated += updated
    
    return relations_added, relations_updated


# Filter endpoints
@app.post("/filters/bulk", response_model=BulkOperationResponse, status_code=201)
async def create_bulk_filters(
    bulk_data: BulkFilterCreate,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """
    Bulk create filters with optional relations to companies, campaigns, and ad groups.
    
    - Filters are created (or retrieved if they already exist)
    - Optionally associate with companies, campaigns, and/or ad groups
    - Apply same is_negative flag to all relations
    """
    
    # Validate ownership of all entities
    _validate_entity_ownership(
        db=db,
        user_id=user_id,
        company_ids=bulk_data.company_ids,
        ad_campaign_ids=bulk_data.ad_campaign_ids,
        ad_group_ids=bulk_data.ad_group_ids
    )
    
    created_filters = []
    existing_filters = []
    
    for filter_text in bulk_data.filters:
        filter_text = filter_text.strip()
        if not filter_text:
            continue
            
        # Try to get existing filter or create new one
        filter_obj = db.query(models.Filter).filter(
            models.Filter.filter == filter_text,
            models.Filter.clerk_user_id == user_id
        ).first()
        
        if filter_obj:
            existing_filters.append(filter_obj)
        else:
            filter_obj = models.Filter(
                filter=filter_text,
                clerk_user_id=user_id
            )
            db.add(filter_obj)
            db.flush()  # Get the ID without committing
            created_filters.append(filter_obj)
        
        # Create relations using helper function
        _create_filter_relations(
            db=db,
            filter_obj=filter_obj,
            company_ids=bulk_data.company_ids,
            ad_campaign_ids=bulk_data.ad_campaign_ids,
            ad_group_ids=bulk_data.ad_group_ids,
            is_negative=bulk_data.is_negative
        )
    
    # Commit all changes
    db.commit()
    
    # Calculate totals
    all_filters = created_filters + existing_filters
    
    return BulkOperationResponse(
        status="success",
        message=f"Created {len(created_filters)} new filters, found {len(existing_filters)} existing",
        created=len(created_filters),
        existing=len(existing_filters),
        total=len(all_filters)
    )


@app.post("/filters/bulk/relations/update", response_model=BulkOperationResponse)
async def bulk_update_filter_relations(
    update_data: BulkFilterUpdateRelations,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """
    Bulk update is_negative for existing filter relations.
    
    Updates is_negative for ALL existing relations of the specified filters
    (companies, campaigns, ad groups).
    
    Processes filters in batches of 25 to avoid memory issues.
    Maximum 100 filters per request.
    
    Example: To set is_negative=true for all relations:
    {
        "filter_ids": [1, 2, 3],
        "is_negative": true
    }
    """
    
    # Limit to 100 filters per request
    if len(update_data.filter_ids) > 100:
        raise HTTPException(
            status_code=400,
            detail="Maximum 100 filters allowed per request"
        )
    
    # Validate filters belong to user
    filters = _validate_filters_ownership(db, user_id, update_data.filter_ids)
    
    relations_updated = 0
    batches_processed = 0
    
    # Process filters in batches of 25
    for filter_batch in process_in_batches(filters, batch_size=25):
        # Process each filter in the batch
        for filter_obj in filter_batch:
            # Update company relations
            company_relations = db.query(models.CompanyFilter).filter(
                models.CompanyFilter.filter_id == filter_obj.id
            ).all()
            
            for assoc in company_relations:
                if assoc.is_negative != update_data.is_negative:
                    assoc.is_negative = update_data.is_negative
                    relations_updated += 1
            
            # Update campaign relations
            campaign_relations = db.query(models.AdCampaignFilter).filter(
                models.AdCampaignFilter.filter_id == filter_obj.id
            ).all()
            
            for assoc in campaign_relations:
                if assoc.is_negative != update_data.is_negative:
                    assoc.is_negative = update_data.is_negative
                    relations_updated += 1
            
            # Update ad group relations
            ad_group_relations = db.query(models.AdGroupFilter).filter(
                models.AdGroupFilter.filter_id == filter_obj.id
            ).all()
            
            for assoc in ad_group_relations:
                if assoc.is_negative != update_data.is_negative:
                    assoc.is_negative = update_data.is_negative
                    relations_updated += 1
        
        # Commit after each batch
        db.commit()
        batches_processed += 1
    
    return BulkOperationResponse(
        status="success",
        message=f"Updated {relations_updated} relations for {len(filters)} filters in {batches_processed} batches",
        updated=len(filters),
        relations_added=0,
        relations_updated=relations_updated,
        batches_processed=batches_processed,
        batch_size=25
    )


@app.post("/filters/bulk/relations", response_model=BulkOperationResponse)
async def bulk_create_filter_relations(
    create_data: BulkFilterCreateRelations,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """
    Create new relations for existing filters with companies, campaigns, or ad groups.
    
    - Creates new relations if they don't exist
    - Updates is_negative for existing relations
    
    Example: Associate filters 1, 2, 3 with companies 5, 6 as negative filters:
    {
        "filter_ids": [1, 2, 3],
        "company_ids": [5, 6],
        "is_negative": true
    }
    """
    
    # Validate filters belong to user
    filters = _validate_filters_ownership(db, user_id, create_data.filter_ids)
    
    # Validate ownership of all entities
    _validate_entity_ownership(
        db=db,
        user_id=user_id,
        company_ids=create_data.company_ids,
        ad_campaign_ids=create_data.ad_campaign_ids,
        ad_group_ids=create_data.ad_group_ids
    )
    
    total_relations_added = 0
    total_relations_updated = 0
    
    # Process each filter
    for filter_obj in filters:
        added, updated = _create_filter_relations(
            db=db,
            filter_obj=filter_obj,
            company_ids=create_data.company_ids,
            ad_campaign_ids=create_data.ad_campaign_ids,
            ad_group_ids=create_data.ad_group_ids,
            is_negative=create_data.is_negative
        )
        total_relations_added += added
        total_relations_updated += updated
    
    # Commit all changes
    db.commit()
    
    return BulkOperationResponse(
        status="success",
        message=f"Processed {len(filters)} filters: added {total_relations_added} relations, updated {total_relations_updated}",
        processed=len(filters),
        relations_added=total_relations_added,
        relations_updated=total_relations_updated
    )


@app.get("/filters", response_model=MultipleObjectsResponse)
async def list_filters(
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(100, ge=1, le=100, description="Items per page (max 100)"),
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """List all filters for the authenticated user with pagination"""
    query = db.query(models.Filter).filter(models.Filter.clerk_user_id == user_id)
    filters, total_count, total_pages = paginate_query(query, page, page_size)
    
    return MultipleObjectsResponse(
        status="success",
        objects=[Filter.model_validate(f) for f in filters],
        total=total_count,
        page=page,
        page_size=page_size,
        total_pages=total_pages
    )


@app.get("/filters/{filter_id}", response_model=SingleObjectResponse)
async def get_filter(
    filter_id: int,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Get a specific filter by ID"""
    filter_obj = db.query(models.Filter).filter(
        models.Filter.id == filter_id,
        models.Filter.clerk_user_id == user_id
    ).first()
    if not filter_obj:
        raise HTTPException(status_code=404, detail="Filter not found")
    return SingleObjectResponse(
        status="success",
        object=Filter.model_validate(filter_obj),
        id=filter_obj.id
    )


@app.post("/filters/{filter_id}/update", response_model=BulkOperationResponse)
async def update_filter(
    filter_id: int,
    filter_update: Filter,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Update a filter"""
    filter_obj = db.query(models.Filter).filter(
        models.Filter.id == filter_id,
        models.Filter.clerk_user_id == user_id
    ).first()
    if not filter_obj:
        raise HTTPException(status_code=404, detail="Filter not found")
    
    filter_obj.filter = filter_update.filter
    db.commit()
    db.refresh(filter_obj)
    return BulkOperationResponse(
        status="success",
        message="Filter updated successfully",
        object={
            "id": filter_obj.id,
            "filter": filter_obj.filter,
            "clerk_user_id": filter_obj.clerk_user_id,
            "created": filter_obj.created.isoformat(),
            "updated": filter_obj.updated.isoformat()
        },
        id=filter_obj.id
    )


@app.post("/filters/bulk/delete", response_model=BulkOperationResponse)
async def bulk_delete_filters(
    delete_data: BulkDeleteRequest,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Bulk delete filters"""
    if not delete_data.ids:
        raise HTTPException(status_code=400, detail="ids is required")
    
    deleted_count = db.query(models.Filter).filter(
        models.Filter.id.in_(delete_data.ids),
        models.Filter.clerk_user_id == user_id
    ).delete(synchronize_session=False)
    
    db.commit()
    
    return BulkOperationResponse(
        status="success",
        message=f"Deleted {deleted_count} filters",
        deleted=deleted_count,
        requested=len(delete_data.ids)
    )


@app.post("/relations/company-filter/bulk/delete", response_model=BulkOperationResponse)
async def bulk_delete_company_filter_relations(
    delete_data: BulkDeleteRequest,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Bulk delete company-filter relations"""
    if not delete_data.ids:
        raise HTTPException(status_code=400, detail="ids is required")
    
    deleted_count = db.query(models.CompanyFilter).join(
        models.Filter
    ).filter(
        models.CompanyFilter.id.in_(delete_data.ids),
        models.Filter.clerk_user_id == user_id
    ).delete(synchronize_session=False)
    
    db.commit()
    
    return BulkOperationResponse(
        status="success",
        message=f"Deleted {deleted_count} company-filter relations",
        deleted=deleted_count,
        requested=len(delete_data.ids)
    )


@app.post("/relations/campaign-filter/bulk/delete", response_model=BulkOperationResponse)
async def bulk_delete_campaign_filter_relations(
    delete_data: BulkDeleteRequest,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Bulk delete campaign-filter relations"""
    if not delete_data.ids:
        raise HTTPException(status_code=400, detail="ids is required")
    
    deleted_count = db.query(models.AdCampaignFilter).join(
        models.Filter
    ).filter(
        models.AdCampaignFilter.id.in_(delete_data.ids),
        models.Filter.clerk_user_id == user_id
    ).delete(synchronize_session=False)
    
    db.commit()
    
    return BulkOperationResponse(
        status="success",
        message=f"Deleted {deleted_count} campaign-filter relations",
        deleted=deleted_count,
        requested=len(delete_data.ids)
    )


@app.post("/relations/adgroup-filter/bulk/delete", response_model=BulkOperationResponse)
async def bulk_delete_adgroup_filter_relations(
    delete_data: BulkDeleteRequest,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Bulk delete ad group-filter relations"""
    if not delete_data.ids:
        raise HTTPException(status_code=400, detail="ids is required")
    
    deleted_count = db.query(models.AdGroupFilter).join(
        models.Filter
    ).filter(
        models.AdGroupFilter.id.in_(delete_data.ids),
        models.Filter.clerk_user_id == user_id
    ).delete(synchronize_session=False)
    
    db.commit()
    
    return BulkOperationResponse(
        status="success",
        message=f"Deleted {deleted_count} adgroup-filter relations",
        deleted=deleted_count,
        requested=len(delete_data.ids)
    )