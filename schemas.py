from pydantic import BaseModel, ConfigDict
from typing import Optional, List, Any
from datetime import datetime


# ==================== Base Entity Schemas ====================

# Match Types Schema (used for keyword associations)
class MatchTypes(BaseModel):
    broad: bool = False
    phrase: bool = False
    exact: bool = False
    neg_broad: bool = False
    neg_phrase: bool = False
    neg_exact: bool = False


# Base entity data model (for responses)
class EntityData(BaseModel):
    model_config = ConfigDict(from_attributes=True, exclude_none=True)
    
    id: Optional[int] = None
    clerk_user_id: Optional[str] = None
    created: Optional[datetime] = None
    updated: Optional[datetime] = None


# ==================== Entity Schemas (Used for both Create and Response) ====================

class Company(EntityData):
    title: str


class AdCampaign(EntityData):
    title: str
    company_id: Optional[int] = None


class AdGroup(EntityData):
    title: str
    ad_campaign_id: Optional[int] = None


class Keyword(EntityData):
    keyword: str


class Filter(EntityData):
    filter: str


# ==================== Response Wrappers (All include status) ====================

class SingleObjectResponse(BaseModel):
    """Response for single entity operations (create, update, get)"""
    model_config = ConfigDict(exclude_none=True)
    
    status: str  # "success" or "error"
    message: Optional[str] = None
    object: Any  # The entity data
    id: Optional[int] = None  # Quick access to entity ID


class MultipleObjectsResponse(BaseModel):
    """Response for list operations with pagination"""
    model_config = ConfigDict(exclude_none=True)
    
    status: str  # "success" or "error"
    message: Optional[str] = None
    objects: List[Any]  # List of entities
    total: Optional[int] = None  # Total count of all entities
    page: Optional[int] = None  # Current page number (1-indexed)
    page_size: Optional[int] = None  # Number of items per page
    total_pages: Optional[int] = None  # Total number of pages


class BulkOperationResponse(BaseModel):
    """Response for bulk operations (create, update, delete)"""
    model_config = ConfigDict(exclude_none=True)
    
    status: str  # "success" or "error"
    message: Optional[str] = None
    created: Optional[int] = None
    updated: Optional[int] = None
    deleted: Optional[int] = None
    existing: Optional[int] = None
    processed: Optional[int] = None
    total: Optional[int] = None
    requested: Optional[int] = None
    relations_added: Optional[int] = None
    relations_updated: Optional[int] = None
    batches_processed: Optional[int] = None  # Number of batches processed
    batch_size: Optional[int] = None  # Size of each batch


# ==================== Bulk Keyword Schemas ====================

class BulkKeywordUpdateRelations(BaseModel):
    """Base schema for bulk updating keyword match types for existing relations"""
    keyword_ids: List[int]  # List of keyword IDs to update
    
    # Match type values to set (if corresponding override flag is True)
    broad: Optional[bool] = None
    phrase: Optional[bool] = None
    exact: Optional[bool] = None
    neg_broad: Optional[bool] = None
    neg_phrase: Optional[bool] = None
    neg_exact: Optional[bool] = None
    
    # Override flags: If True, update the corresponding match type
    # If False or not provided, leave the match type unchanged
    override_broad: bool = False
    override_phrase: bool = False
    override_exact: bool = False
    override_neg_broad: bool = False
    override_neg_phrase: bool = False
    override_neg_exact: bool = False


class BulkKeywordCreateRelations(BulkKeywordUpdateRelations):
    """Schema for creating new relations for existing keywords"""
    # Match types to apply to new relations
    match_types: Optional[MatchTypes] = None
    
    # Optional: Associate with companies (apply same match_types to all)
    company_ids: Optional[List[int]] = None
    
    # Optional: Associate with ad campaigns (apply same match_types to all)
    ad_campaign_ids: Optional[List[int]] = None
    
    # Optional: Associate with ad groups (apply same match_types to all)
    ad_group_ids: Optional[List[int]] = None


class BulkKeywordCreate(BulkKeywordCreateRelations):
    """Schema for bulk keyword creation with optional relations"""
    keywords: List[str]  # List of keyword strings to create
    
    # Override keyword_ids from parent - not needed for creation
    keyword_ids: Optional[List[int]] = None  # Not used in this endpoint


# ==================== Bulk Filter Schemas ====================

class BulkFilterUpdateRelations(BaseModel):
    """Schema for bulk updating filter is_negative for existing relations"""
    filter_ids: List[int]  # List of filter IDs to update
    is_negative: bool  # Value to set for is_negative field


class BulkFilterCreateRelations(BaseModel):
    """Schema for creating new relations for existing filters"""
    filter_ids: List[int]  # List of filter IDs to associate
    is_negative: bool = False  # Whether this is a negative filter
    
    # Optional: Associate with companies
    company_ids: Optional[List[int]] = None
    
    # Optional: Associate with ad campaigns
    ad_campaign_ids: Optional[List[int]] = None
    
    # Optional: Associate with ad groups
    ad_group_ids: Optional[List[int]] = None


class BulkFilterCreate(BulkFilterCreateRelations):
    """Schema for bulk filter creation with optional relations"""
    filters: List[str]  # List of filter strings to create
    
    # Override filter_ids from parent - not needed for creation
    filter_ids: Optional[List[int]] = None  # Not used in this endpoint


# ==================== Delete Schemas ====================

class BulkDeleteRequest(BaseModel):
    """Request schema for bulk delete operations - simple list of IDs"""
    ids: List[int]

