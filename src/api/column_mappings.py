"""
Simplified Column Mappings API - Toggle functionality and active mappings retrieval.
Column mappings are accessed via dedicated endpoints, not mixed with entity responses.
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..core.database import get_db
from ..models.models import ColumnMapping, Company, AdCampaign, AdGroup
from ..schemas.schemas import ColumnMappingToggleRequest
from ..utils.auth import get_current_user_id


def column_mapping_to_dict(mapping: ColumnMapping) -> dict:
    """Convert ColumnMapping SQLAlchemy object to dictionary for JSON serialization."""
    return {
        'id': mapping.id,
        'source_company_id': mapping.source_company_id,
        'source_ad_campaign_id': mapping.source_ad_campaign_id,
        'source_ad_group_id': mapping.source_ad_group_id,
        'source_match_type': mapping.source_match_type,
        'target_company_id': mapping.target_company_id,
        'target_ad_campaign_id': mapping.target_ad_campaign_id,
        'target_ad_group_id': mapping.target_ad_group_id,
        'target_match_type': mapping.target_match_type,
        'created': mapping.created.isoformat(),
        'updated': mapping.updated.isoformat()
    }


router = APIRouter(prefix="/column-mappings", tags=["column_mappings"])


@router.post("/toggle")
def toggle_column_mapping(
    request: ColumnMappingToggleRequest,
    db: Session = Depends(get_db),
    clerk_user_id: str = Depends(get_current_user_id)
):
    """Create or remove column mapping based on action parameter ('create' or 'remove')"""
    
    # Validate that source entities exist and belong to the user
    if request.source_company_id:
        source_entity = db.query(Company).filter(
            Company.id == request.source_company_id,
            Company.clerk_user_id == clerk_user_id
        ).first()
        if not source_entity:
            raise HTTPException(status_code=404, detail="Source company not found")
    
    elif request.source_ad_campaign_id:
        source_entity = db.query(AdCampaign).filter(
            AdCampaign.id == request.source_ad_campaign_id,
            AdCampaign.clerk_user_id == clerk_user_id
        ).first()
        if not source_entity:
            raise HTTPException(status_code=404, detail="Source campaign not found")
    
    elif request.source_ad_group_id:
        source_entity = db.query(AdGroup).filter(
            AdGroup.id == request.source_ad_group_id,
            AdGroup.clerk_user_id == clerk_user_id
        ).first()
        if not source_entity:
            raise HTTPException(status_code=404, detail="Source ad group not found")
    
    # Validate that target entities exist and belong to the user
    if request.target_company_id:
        target_entity = db.query(Company).filter(
            Company.id == request.target_company_id,
            Company.clerk_user_id == clerk_user_id
        ).first()
        if not target_entity:
            raise HTTPException(status_code=404, detail="Target company not found")
    
    elif request.target_ad_campaign_id:
        target_entity = db.query(AdCampaign).filter(
            AdCampaign.id == request.target_ad_campaign_id,
            AdCampaign.clerk_user_id == clerk_user_id
        ).first()
        if not target_entity:
            raise HTTPException(status_code=404, detail="Target campaign not found")
    
    elif request.target_ad_group_id:
        target_entity = db.query(AdGroup).filter(
            AdGroup.id == request.target_ad_group_id,
            AdGroup.clerk_user_id == clerk_user_id
        ).first()
        if not target_entity:
            raise HTTPException(status_code=404, detail="Target ad group not found")
    
    # Handle create action
    if request.action == "create":
        # Check if mapping already exists
        existing_mapping = db.query(ColumnMapping).filter(
            ColumnMapping.clerk_user_id == clerk_user_id,
            ColumnMapping.source_company_id == request.source_company_id,
            ColumnMapping.source_ad_campaign_id == request.source_ad_campaign_id,
            ColumnMapping.source_ad_group_id == request.source_ad_group_id,
            ColumnMapping.source_match_type == request.source_match_type,
            ColumnMapping.target_company_id == request.target_company_id,
            ColumnMapping.target_ad_campaign_id == request.target_ad_campaign_id,
            ColumnMapping.target_ad_group_id == request.target_ad_group_id,
            ColumnMapping.target_match_type == request.target_match_type
        ).first()
        
        if existing_mapping:
            return {"action": "already_exists", "mapping_id": existing_mapping.id}
        else:
            # Create new mapping
            db_mapping = ColumnMapping(
                clerk_user_id=clerk_user_id,
                source_company_id=request.source_company_id,
                source_ad_campaign_id=request.source_ad_campaign_id,
                source_ad_group_id=request.source_ad_group_id,
                source_match_type=request.source_match_type,
                target_company_id=request.target_company_id,
                target_ad_campaign_id=request.target_ad_campaign_id,
                target_ad_group_id=request.target_ad_group_id,
                target_match_type=request.target_match_type
            )
            db.add(db_mapping)
            db.commit()
            db.refresh(db_mapping)
            return {"action": "created", "mapping_id": db_mapping.id}
    
    # Handle remove action
    elif request.action == "remove":
        # Find and delete the mapping
        existing_mapping = db.query(ColumnMapping).filter(
            ColumnMapping.clerk_user_id == clerk_user_id,
            ColumnMapping.source_company_id == request.source_company_id,
            ColumnMapping.source_ad_campaign_id == request.source_ad_campaign_id,
            ColumnMapping.source_ad_group_id == request.source_ad_group_id,
            ColumnMapping.source_match_type == request.source_match_type,
            ColumnMapping.target_company_id == request.target_company_id,
            ColumnMapping.target_ad_campaign_id == request.target_ad_campaign_id,
            ColumnMapping.target_ad_group_id == request.target_ad_group_id,
            ColumnMapping.target_match_type == request.target_match_type
        ).first()
        
        if existing_mapping:
            db.delete(existing_mapping)
            db.commit()
            return {"action": "removed", "mapping_id": existing_mapping.id}
        else:
            return {"action": "not_found", "message": "Mapping not found"}


@router.get("/active")
def get_active_column_mappings(
    db: Session = Depends(get_db),
    clerk_user_id: str = Depends(get_current_user_id)
):
    """Get all active column mappings for the authenticated user where both source and target entities are active"""
    from sqlalchemy import exists, and_, or_
    
    # Build condition for source entity being active
    source_active_condition = or_(
        and_(
            ColumnMapping.source_company_id.isnot(None),
            exists().where(
                and_(
                    Company.id == ColumnMapping.source_company_id,
                    Company.clerk_user_id == clerk_user_id,
                    Company.is_active == True
                )
            )
        ),
        and_(
            ColumnMapping.source_ad_campaign_id.isnot(None),
            exists().where(
                and_(
                    AdCampaign.id == ColumnMapping.source_ad_campaign_id,
                    AdCampaign.clerk_user_id == clerk_user_id,
                    AdCampaign.is_active == True
                )
            )
        ),
        and_(
            ColumnMapping.source_ad_group_id.isnot(None),
            exists().where(
                and_(
                    AdGroup.id == ColumnMapping.source_ad_group_id,
                    AdGroup.clerk_user_id == clerk_user_id,
                    AdGroup.is_active == True
                )
            )
        )
    )
    
    # Build condition for target entity being active
    target_active_condition = or_(
        and_(
            ColumnMapping.target_company_id.isnot(None),
            exists().where(
                and_(
                    Company.id == ColumnMapping.target_company_id,
                    Company.clerk_user_id == clerk_user_id,
                    Company.is_active == True
                )
            )
        ),
        and_(
            ColumnMapping.target_ad_campaign_id.isnot(None),
            exists().where(
                and_(
                    AdCampaign.id == ColumnMapping.target_ad_campaign_id,
                    AdCampaign.clerk_user_id == clerk_user_id,
                    AdCampaign.is_active == True
                )
            )
        ),
        and_(
            ColumnMapping.target_ad_group_id.isnot(None),
            exists().where(
                and_(
                    AdGroup.id == ColumnMapping.target_ad_group_id,
                    AdGroup.clerk_user_id == clerk_user_id,
                    AdGroup.is_active == True
                )
            )
        )
    )
    
    # Query mappings where both source AND target entities are active
    mappings = db.query(ColumnMapping).filter(
        ColumnMapping.clerk_user_id == clerk_user_id,
        source_active_condition,
        target_active_condition
    ).all()
    
    return {
        "message": "Active column mappings retrieved successfully",
        "objects": [column_mapping_to_dict(mapping) for mapping in mappings]
    }
