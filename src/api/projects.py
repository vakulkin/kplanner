from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from src.core.database import get_db
from src.core.settings import (
    DEFAULT_PAGE,
    MAX_PAGE_SIZE,
    PAGE_SIZE,
)
from src.models.models import (
    Project,
    ProjectCompany,
    ProjectAdCampaign,
    ProjectAdGroup,
    Company,
    AdCampaign,
    AdGroup,
)
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from src.core.database import get_db
from src.core.settings import (
    DEFAULT_PAGE,
    MAX_PAGE_SIZE,
    PAGE_SIZE,
)
from src.models.models import (
    Project,
    ProjectCompany,
    ProjectAdCampaign,
    ProjectAdGroup,
    Company,
    AdCampaign,
    AdGroup,
)
from src.schemas.schemas import (
    BulkDeleteRequest,
    BulkDeleteResponse,
    Project as ProjectSchema,
    ProjectWithEntities,
    ProjectCreate,
    ProjectEntityUpdate,
    MultipleObjectsResponse,
    SingleObjectResponse,
)
from src.utils.database_helpers import paginate_query
from src.utils.entity_helpers import update_simple_entity, validate_parent_entity
from src.utils.auth import get_current_user_id

router = APIRouter()


@router.post("/projects", response_model=SingleObjectResponse, status_code=201)
async def create_project(
    project_data: ProjectCreate,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Create a new project"""
    project = Project(
        title=project_data.name,
        clerk_user_id=user_id
    )
    db.add(project)
    db.commit()
    db.refresh(project)
    
    return {
        "message": "Project created successfully",
        "object": ProjectSchema.model_validate(project)
    }


@router.get("/projects", response_model=MultipleObjectsResponse)
async def list_projects(
    page: int = Query(DEFAULT_PAGE, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE, description=f"Items per page (max {MAX_PAGE_SIZE})"),
    search: Optional[str] = Query(None, description="Search by project title (case-insensitive, partial match)"),
    created_after: Optional[datetime] = Query(None, description="Filter by created date (after)"),
    created_before: Optional[datetime] = Query(None, description="Filter by created date (before)"),
    updated_after: Optional[datetime] = Query(None, description="Filter by updated date (after)"),
    updated_before: Optional[datetime] = Query(None, description="Filter by updated date (before)"),
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """List projects with pagination and filtering"""
    query = db.query(Project).filter(Project.clerk_user_id == user_id)
    
    # Apply filters
    if search:
        query = query.filter(Project.title.ilike(f"%{search}%"))
    if created_after:
        query = query.filter(Project.created >= created_after)
    if created_before:
        query = query.filter(Project.created <= created_before)
    if updated_after:
        query = query.filter(Project.updated >= updated_after)
    if updated_before:
        query = query.filter(Project.updated <= updated_before)
    
    # Order by creation date (newest first)
    query = query.order_by(Project.created.desc())
    
    # Paginate
    total_count = query.count()
    projects, _, _ = paginate_query(query, page, page_size)
    
    return {
        "message": f"Retrieved {total_count} projects",
        "objects": [ProjectSchema.model_validate(project) for project in projects],
        "pagination": {
            "total": total_count,
            "page": page,
            "page_size": page_size,
            "total_pages": (total_count + page_size - 1) // page_size
        },
        "filters": {
            "search": search,
            "created_after": created_after,
            "created_before": created_before,
            "updated_after": updated_after,
            "updated_before": updated_before
        },
        "sorting": {
            "sort_by": "created",
            "sort_order": "desc"
        }
    }


@router.get("/projects/{project_id}", response_model=SingleObjectResponse)
async def get_project(
    project_id: int,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Get a specific project with its attached entities"""
    project = validate_parent_entity(
        db=db,
        user_id=user_id,
        parent_id=project_id,
        parent_model=Project,
        parent_name="project"
    )
    
    return {
        "message": "Project retrieved successfully",
        "object": ProjectWithEntities.model_validate(project)
    }


@router.post("/projects/{project_id}/update", response_model=SingleObjectResponse)
async def update_project(
    project_id: int,
    project_update: ProjectCreate,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Update a project"""
    return update_simple_entity(
        db=db,
        user_id=user_id,
        entity_id=project_id,
        model_class=Project,
        schema_class=ProjectSchema,
        entity_name="project",
        update_fields={"title": project_update.name}
    )


@router.post("/projects/{project_id}/entities", response_model=SingleObjectResponse)
async def update_project_entities(
    project_id: int,
    entity_update: ProjectEntityUpdate,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Update project entities (companies, campaigns, ad groups)"""
    # Verify project exists and belongs to user
    project = validate_parent_entity(
        db=db,
        user_id=user_id,
        parent_id=project_id,
        parent_model=Project,
        parent_name="project"
    )
    
    # Update companies
    if entity_update.company_ids is not None:
        # Remove existing company associations
        db.query(ProjectCompany).filter(ProjectCompany.project_id == project_id).delete()
        # Add new associations
        for company_id in entity_update.company_ids:
            validate_parent_entity(
                db=db,
                user_id=user_id,
                parent_id=company_id,
                parent_model=Company,
                parent_name="company"
            )
            db.add(ProjectCompany(project_id=project_id, company_id=company_id, clerk_user_id=user_id))
    
    # Update ad campaigns
    if entity_update.ad_campaign_ids is not None:
        # Remove existing campaign associations
        db.query(ProjectAdCampaign).filter(ProjectAdCampaign.project_id == project_id).delete()
        # Add new associations
        for campaign_id in entity_update.ad_campaign_ids:
            validate_parent_entity(
                db=db,
                user_id=user_id,
                parent_id=campaign_id,
                parent_model=AdCampaign,
                parent_name="ad campaign"
            )
            db.add(ProjectAdCampaign(project_id=project_id, ad_campaign_id=campaign_id, clerk_user_id=user_id))
    
    # Update ad groups
    if entity_update.ad_group_ids is not None:
        # Remove existing ad group associations
        db.query(ProjectAdGroup).filter(ProjectAdGroup.project_id == project_id).delete()
        # Add new associations
        for ad_group_id in entity_update.ad_group_ids:
            validate_parent_entity(
                db=db,
                user_id=user_id,
                parent_id=ad_group_id,
                parent_model=AdGroup,
                parent_name="ad group"
            )
            db.add(ProjectAdGroup(project_id=project_id, ad_group_id=ad_group_id, clerk_user_id=user_id))
    
    db.commit()
    db.refresh(project)
    
    return {
        "message": "Project entities updated successfully",
        "object": ProjectWithEntities.model_validate(project)
    }


@router.post("/projects/{project_id}/delete", response_model=SingleObjectResponse)
async def delete_project(
    project_id: int,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Delete a project"""
    # Verify project exists and belongs to user
    project = validate_parent_entity(
        db=db,
        user_id=user_id,
        parent_id=project_id,
        parent_model=Project,
        parent_name="project"
    )
    
    # Delete associated entities
    db.query(ProjectCompany).filter(ProjectCompany.project_id == project_id).delete()
    db.query(ProjectAdCampaign).filter(ProjectAdCampaign.project_id == project_id).delete()
    db.query(ProjectAdGroup).filter(ProjectAdGroup.project_id == project_id).delete()
    
    # Delete project
    db.delete(project)
    db.commit()
    
    return {
        "message": "Project deleted successfully",
        "object": {"id": project_id}
    }


@router.post("/projects/bulk-delete", response_model=BulkDeleteResponse)
async def bulk_delete_projects(
    request: BulkDeleteRequest,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Bulk delete projects"""
    if not request.ids:
        raise HTTPException(status_code=400, detail="No project IDs provided")
    
    # Verify all projects exist and belong to user
    projects = db.query(Project).filter(
        Project.id.in_(request.ids),
        Project.clerk_user_id == user_id
    ).all()
    
    if len(projects) != len(request.ids):
        raise HTTPException(status_code=404, detail="Some projects not found or access denied")
    
    # Delete associated entities first
    for project_id in request.ids:
        db.query(ProjectCompany).filter(ProjectCompany.project_id == project_id).delete()
        db.query(ProjectAdCampaign).filter(ProjectAdCampaign.project_id == project_id).delete()
        db.query(ProjectAdGroup).filter(ProjectAdGroup.project_id == project_id).delete()
    
    # Delete projects
    deleted_count = db.query(Project).filter(
        Project.id.in_(request.ids),
        Project.clerk_user_id == user_id
    ).delete()
    
    db.commit()
    
    return {
        "message": f"Successfully deleted {deleted_count} projects",
        "processed": deleted_count,
        "requested": len(request.ids),
        "deleted": deleted_count
    }
    
