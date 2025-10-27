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
    trash: Optional[bool] = None


# ==================== Column Mapping Schemas ====================

class ColumnMappingToggleRequest(BaseModel):
    """Schema for column mapping operations (create or remove)"""
    
    action: str = Field(description="'create' or 'remove'")
    
    # Source entity - provide exactly ONE of these approaches:
    # Option 1: Specific entity ID fields (legacy)
    source_company_id: Optional[int] = Field(None, gt=0, description="Source company ID")
    source_ad_campaign_id: Optional[int] = Field(None, gt=0, description="Source campaign ID")
    source_ad_group_id: Optional[int] = Field(None, gt=0, description="Source ad group ID")
    
    # Option 2: Generic entity type + ID (new)
    source_entity_type: Optional[str] = Field(None, description="Source entity type: 'company', 'ad_campaign', or 'ad_group'")
    source_entity_id: Optional[int] = Field(None, gt=0, description="Source entity ID")
    
    source_match_type: str = Field(description="'broad', 'phrase', 'exact', 'neg_broad', 'neg_phrase', or 'neg_exact'")
    
    # Target entity - provide exactly ONE of these approaches:
    # Option 1: Specific entity ID fields (legacy)
    target_company_id: Optional[int] = Field(None, gt=0, description="Target company ID")
    target_ad_campaign_id: Optional[int] = Field(None, gt=0, description="Target campaign ID")
    target_ad_group_id: Optional[int] = Field(None, gt=0, description="Target ad group ID")
    
    # Option 2: Generic entity type + ID (new)
    target_entity_type: Optional[str] = Field(None, description="Target entity type: 'company', 'ad_campaign', or 'ad_group'")
    target_entity_id: Optional[int] = Field(None, gt=0, description="Target entity ID")
    
    target_match_type: str = Field(description="'broad', 'phrase', 'exact', 'neg_broad', 'neg_phrase', or 'neg_exact'")
    
    @field_validator('action')
    @classmethod
    def validate_action(cls, v):
        valid_actions = ['create', 'remove']
        if v not in valid_actions:
            raise ValueError(f"Action must be one of: {', '.join(valid_actions)}")
        return v
    
    @field_validator('source_entity_type', 'target_entity_type')
    @classmethod
    def validate_entity_type(cls, v):
        if v is not None:
            valid_types = ['company', 'ad_campaign', 'ad_group']
            if v not in valid_types:
                raise ValueError(f"Entity type must be one of: {', '.join(valid_types)}")
        return v
    
    @field_validator('source_match_type', 'target_match_type')
    @classmethod
    def validate_match_type(cls, v):
        valid_types = ['broad', 'phrase', 'exact', 'neg_broad', 'neg_phrase', 'neg_exact']
        if v not in valid_types:
            raise ValueError(f"Match type must be one of: {', '.join(valid_types)}")
        return v
    
    def model_post_init(self, __context):
        """Validate that exactly one source and one target entity is provided, and convert generic format to specific format"""
        
        # Convert generic source format to specific format
        if self.source_entity_type and self.source_entity_id:
            if self.source_entity_type == 'company':
                self.source_company_id = self.source_entity_id
            elif self.source_entity_type == 'ad_campaign':
                self.source_ad_campaign_id = self.source_entity_id
            elif self.source_entity_type == 'ad_group':
                self.source_ad_group_id = self.source_entity_id
            # Clear the generic fields
            self.source_entity_type = None
            self.source_entity_id = None
        
        # Convert generic target format to specific format
        if self.target_entity_type and self.target_entity_id:
            if self.target_entity_type == 'company':
                self.target_company_id = self.target_entity_id
            elif self.target_entity_type == 'ad_campaign':
                self.target_ad_campaign_id = self.target_entity_id
            elif self.target_entity_type == 'ad_group':
                self.target_ad_group_id = self.target_entity_id
            # Clear the generic fields
            self.target_entity_type = None
            self.target_entity_id = None
        
        # Validate that exactly one source entity is provided
        source_count = sum([
            self.source_company_id is not None,
            self.source_ad_campaign_id is not None,
            self.source_ad_group_id is not None
        ])
        
        # Validate that exactly one target entity is provided
        target_count = sum([
            self.target_company_id is not None,
            self.target_ad_campaign_id is not None,
            self.target_ad_group_id is not None
        ])
        
        if source_count != 1:
            raise ValueError("Exactly one source entity (company, campaign, or ad group) must be provided")
        if target_count != 1:
            raise ValueError("Exactly one target entity (company, campaign, or ad group) must be provided")


class ColumnMappingResponse(EntityResponse):
    """Response schema for column mapping with denormalized entity info"""
    clerk_user_id: str
    
    # Source entity info
    source_company_id: Optional[int]
    source_ad_campaign_id: Optional[int]
    source_ad_group_id: Optional[int]
    source_match_type: str
    
    # Target entity info
    target_company_id: Optional[int]
    target_ad_campaign_id: Optional[int]
    target_ad_group_id: Optional[int]
    target_match_type: str


# ==================== Response Schemas (Output - includes all fields) ====================

class Company(EntityResponse, CompanyCreate):
    """Company response"""
    pass


class AdCampaign(EntityResponse, AdCampaignCreate):
    """Ad campaign response"""
    pass


class AdGroup(EntityResponse, AdGroupCreate):
    """Ad group response"""
    pass


# ==================== Response Schemas (Output - includes all fields) ====================

class Company(EntityResponse, CompanyCreate):
    """Company response"""
    pass


class AdCampaign(EntityResponse, AdCampaignCreate):
    """Ad campaign response"""
    pass


class AdGroup(EntityResponse, AdGroupCreate):
    """Ad group response"""
    pass


class Keyword(EntityResponse, KeywordCreate):
    trash: Optional[bool] = None


# ==================== Relation Schemas ====================

class CompanyKeywordRelation(BaseModel):
    """Schema for company-keyword relations"""
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    company_id: int
    keyword_id: int
    # None = not set, True = positive match, False = negative match
    broad: Optional[bool]
    phrase: Optional[bool]
    exact: Optional[bool]
    # None = not paused, 1 = paused
    pause: Optional[bool]


class AdCampaignKeywordRelation(BaseModel):
    """Schema for ad campaign-keyword relations"""
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    ad_campaign_id: int
    keyword_id: int
    # None = not set, True = positive match, False = negative match
    broad: Optional[bool]
    phrase: Optional[bool]
    exact: Optional[bool]
    # None = not paused, 1 = paused
    pause: Optional[bool]


class AdGroupKeywordRelation(BaseModel):
    """Schema for ad group-keyword relations"""
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    ad_group_id: int
    keyword_id: int
    # None = not set, True = positive match, False = negative match
    broad: Optional[bool]
    phrase: Optional[bool]
    exact: Optional[bool]
    # None = not paused, 1 = paused
    pause: Optional[bool]


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


class BulkOperationResponse(BaseModel):
    """Base response for bulk operations"""
    model_config = ConfigDict(exclude_none=True)
    
    message: str
    processed: int
    requested: int
    batches_processed: Optional[int] = None
    batch_size: Optional[int] = None


class BulkDeleteResponse(BulkOperationResponse):
    """Response for bulk delete operations"""
    deleted: int


class BulkCreateResponse(BulkOperationResponse):
    """Response for bulk create operations"""
    objects: List[Any]
    created: int
    existing: int

class BulkRelationCreateResponse(BulkOperationResponse):
    """Response for bulk relation creation/updating/deletion"""
    created: int = 0
    updated: int = 0
    deleted: int = 0
    relations: List[Any] = []  # List of created/updated relations (CompanyKeywordRelation, AdCampaignKeywordRelation, or AdGroupKeywordRelation)

class BulkKeywordCreateResponse(BulkCreateResponse):
    """Response for bulk keyword creation with relations"""
    relations_created: int
    relations_updated: int

# ==================== Bulk Keyword Schemas ====================

class MatchTypeFlagsBase(BaseModel):
    """Base schema for match types: None = not set, True = positive, False = negative"""
    broad: Optional[bool] = None
    phrase: Optional[bool] = None
    exact: Optional[bool] = None
    # None = not paused, 1 = paused
    pause: Optional[bool] = None


class OverrideFlagsBase(BaseModel):
    """Base schema for override boolean flags"""
    override_broad: Optional[bool] = None
    override_phrase: Optional[bool] = None
    override_exact: Optional[bool] = None
    override_pause: Optional[bool] = None


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


class BulkTrashRequest(BaseModel):
    """Request schema for bulk trash operations"""
    ids: List[int]
    trash: bool


class BulkKeywordCreate(KeywordMatchTypesBase, EntityIdsBase):
    """Request schema for bulk keyword creation with relations"""
    keywords: List[str]


class BulkKeywordCreateRelations(KeywordMatchTypesBase, EntityIdsBase):
    """Request schema for bulk creating/updating/deleting keyword relations"""
    keyword_ids: List[int]


# ==================== Column Mapping Schemas ====================

class ColumnMappingCreate(BaseModel):
    """Schema for creating a column mapping rule with proper entity references"""
    name: str = Field(min_length=1, max_length=255, description="User-friendly name for the mapping")
    description: Optional[str] = Field(None, max_length=500, description="Optional description")
    
    # Source entity - provide exactly ONE of these
    source_company_id: Optional[int] = Field(None, gt=0, description="Source company ID (if source is a company)")
    source_ad_campaign_id: Optional[int] = Field(None, gt=0, description="Source campaign ID (if source is a campaign)")
    source_ad_group_id: Optional[int] = Field(None, gt=0, description="Source ad group ID (if source is an ad group)")
    source_match_type: str = Field(description="'broad', 'phrase', or 'exact'")
    
    # Target entity - provide exactly ONE of these
    target_company_id: Optional[int] = Field(None, gt=0, description="Target company ID (if target is a company)")
    target_ad_campaign_id: Optional[int] = Field(None, gt=0, description="Target campaign ID (if target is a campaign)")
    target_ad_group_id: Optional[int] = Field(None, gt=0, description="Target ad group ID (if target is an ad group)")
    target_match_type: str = Field(description="'broad', 'phrase', or 'exact'")
    
    # Transformation rules
    invert_match: bool = Field(default=False, description="Convert positive to negative and vice versa")
    only_positive: bool = Field(default=False, description="Only copy positive matches (true)")
    only_negative: bool = Field(default=False, description="Only copy negative matches (false)")
    
    is_active: bool = Field(default=True, description="Enable/disable this mapping")
    
    @field_validator('source_match_type', 'target_match_type')
    @classmethod
    def validate_match_type(cls, v):
        valid_types = ['broad', 'phrase', 'exact']
        if v not in valid_types:
            raise ValueError(f"Match type must be one of: {', '.join(valid_types)}")
        return v
    
    @field_validator('name')
    @classmethod
    def name_must_not_be_blank(cls, v):
        if not v.strip():
            raise ValueError('Name must not be empty or contain only whitespace')
        return v.strip()
    
    def model_post_init(self, __context):
        """Validate that exactly one source and one target entity is provided"""
        source_count = sum([
            self.source_company_id is not None,
            self.source_ad_campaign_id is not None,
            self.source_ad_group_id is not None
        ])
        target_count = sum([
            self.target_company_id is not None,
            self.target_ad_campaign_id is not None,
            self.target_ad_group_id is not None
        ])
        
        if source_count != 1:
            raise ValueError("Exactly one source entity (company, campaign, or ad group) must be provided")
        if target_count != 1:
            raise ValueError("Exactly one target entity (company, campaign, or ad group) must be provided")


class ColumnMappingUpdate(BaseModel):
    """Schema for updating a column mapping rule (all fields optional)"""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=500)
    
    source_company_id: Optional[int] = Field(None, gt=0)
    source_ad_campaign_id: Optional[int] = Field(None, gt=0)
    source_ad_group_id: Optional[int] = Field(None, gt=0)
    source_match_type: Optional[str] = None
    
    target_company_id: Optional[int] = Field(None, gt=0)
    target_ad_campaign_id: Optional[int] = Field(None, gt=0)
    target_ad_group_id: Optional[int] = Field(None, gt=0)
    target_match_type: Optional[str] = None
    
    invert_match: Optional[bool] = None
    only_positive: Optional[bool] = None
    only_negative: Optional[bool] = None
    
    is_active: Optional[bool] = None
    
    @field_validator('source_match_type', 'target_match_type')
    @classmethod
    def validate_match_type(cls, v):
        if v is not None:
            valid_types = ['broad', 'phrase', 'exact']
            if v not in valid_types:
                raise ValueError(f"Match type must be one of: {', '.join(valid_types)}")
        return v
    
    @field_validator('name')
    @classmethod
    def name_must_not_be_blank(cls, v):
        if v is not None and not v.strip():
            raise ValueError('Name must not be empty or contain only whitespace')
        return v.strip() if v else v


class ColumnMappingResponse(EntityResponse):
    """Response schema for column mapping with denormalized entity info"""
    clerk_user_id: str
    name: str
    description: Optional[str]
    
    # Source entity info
    source_company_id: Optional[int]
    source_ad_campaign_id: Optional[int]
    source_ad_group_id: Optional[int]
    source_match_type: str
    
    # Target entity info
    target_company_id: Optional[int]
    target_ad_campaign_id: Optional[int]
    target_ad_group_id: Optional[int]
    target_match_type: str
    
    invert_match: bool
    only_positive: bool
    only_negative: bool
    
    is_active: bool


class ColumnMappingListResponse(BaseModel):
    """Response schema for listing column mappings with pagination"""
    objects: List[ColumnMappingResponse]
    pagination: PaginationInfo

