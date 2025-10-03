from pydantic import BaseModel, ConfigDict, Field, field_validator
from typing import List, Any, Optional
from datetime import datetime


# ==================== Base Entity Schemas ====================

# Base entity data model (for responses only)
class EntityResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True, exclude_none=True)
    
    id: int
    created: datetime
    updated: datetime


# ==================== Base Schemas for Reusability ====================

class TitleActiveBase(BaseModel):
    """Base schema for entities with title and is_active fields"""
    title: str = Field(min_length=1, max_length=255)
    is_active: bool

    @field_validator('title')
    @classmethod
    def title_must_not_be_blank(cls, v):
        if not v.strip():
            raise ValueError('Title must not be empty or contain only whitespace')
        return v.strip()


# ==================== Create/Update Schemas ====================

class CompanyCreate(TitleActiveBase):
    """Schema for creating/updating companies. For create: omit id. For update: id ignored."""
    pass


class AdCampaignCreate(TitleActiveBase):
    """Schema for creating/updating ad campaigns. For create: omit id. For update: id ignored."""
    company_id: int


class AdGroupCreate(TitleActiveBase):
    """Schema for creating/updating ad groups. For create: omit id. For update: id ignored."""
    ad_campaign_id: int


class KeywordCreate(BaseModel):
    keyword: str


# ==================== Response Schemas (Output - includes all fields) ====================

class Company(EntityResponse, CompanyCreate):
    pass


class AdCampaign(EntityResponse, AdCampaignCreate):
    company_id: int


class AdGroup(EntityResponse, AdGroupCreate):
    ad_campaign_id: int


class Keyword(EntityResponse, KeywordCreate):
    pass


# ==================== Response Wrappers (All include) ====================

class SingleObjectResponse(BaseModel):
    """Response for single entity operations (create, update, get)"""
    model_config = ConfigDict(exclude_none=True)

    message: str
    object: Any


class PaginationInfo(BaseModel):
    """Pagination information"""
    total: int
    page: int
    page_size: int
    total_pages: int


class MultipleObjectsResponse(BaseModel):
    """Response for list operations with pagination"""
    model_config = ConfigDict(exclude_none=True)
    
    message: str
    objects: List[Any]
    pagination: PaginationInfo
    filters: Optional[dict] = None
    sorting: Optional[dict] = None
    special_features: Optional[dict] = None


class BulkOperationResponse(BaseModel):
    """Base response for bulk operations"""
    model_config = ConfigDict(exclude_none=True)
    
    message: str
    processed: int
    requested: int
    batches_processed: int
    batch_size: int


class BulkDeleteResponse(BulkOperationResponse):
    """Response for bulk delete operations"""
    deleted: int


class BulkCreateResponse(BulkOperationResponse):
    """Response for bulk create operations"""
    objects: List[Any]
    created: int
    existing: int

class BulkRelationUpdateResponse(BulkOperationResponse):
    """Response for bulk relation updates"""
    updated: int


class BulkRelationCreateResponse(BulkOperationResponse):
    """Response for bulk relation creation"""
    created: int
    updated: int

class BulkKeywordCreateResponse(BulkCreateResponse):
    """Response for bulk keyword creation with relations"""
    relations_created: int
    relations_updated: int

# ==================== Bulk Keyword Schemas ====================

class MatchTypeFlagsBase(BaseModel):
    broad: bool
    phrase: bool
    exact: bool
    neg_broad: bool
    neg_phrase: bool
    neg_exact: bool


class OverrideFlagsBase(BaseModel):
    """Base schema for override boolean flags"""
    override_broad: bool
    override_phrase: bool
    override_exact: bool
    override_neg_broad: bool
    override_neg_phrase: bool
    override_neg_exact: bool


class KeywordMatchTypesBase(MatchTypeFlagsBase, OverrideFlagsBase):
    """Combined schema for all keyword match type and override flags"""
    pass


class EntityIdsBase(BaseModel):
    """Base schema for entity ID associations"""
    company_ids: List[int]
    ad_campaign_ids: List[int]
    ad_group_ids: List[int]


# ==================== Request Schemas for Bulk Operations ====================

class BulkDeleteRequest(BaseModel):
    """Request schema for bulk delete operations"""
    ids: List[int]


class BulkKeywordCreate(KeywordMatchTypesBase, EntityIdsBase):
    """Request schema for bulk keyword creation with relations"""
    keywords: List[str]


class BulkKeywordUpdateRelations(KeywordMatchTypesBase):
    """Request schema for bulk updating keyword relations"""
    keyword_ids: List[int]


class BulkKeywordCreateRelations(KeywordMatchTypesBase, EntityIdsBase):
    """Request schema for bulk creating keyword relations"""
    keyword_ids: List[int]
