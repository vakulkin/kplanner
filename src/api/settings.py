from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from src.core.database import get_db
from src.core.settings import (
    DEFAULT_PAGE,
    MAX_PAGE_SIZE,
    PAGE_SIZE,
)
from src.models.models import Settings
from src.schemas.schemas import (
    BulkDeleteRequest,
    BulkDeleteResponse,
    Setting as SettingSchema,
    SettingCreate,
    MultipleObjectsResponse,
    SingleObjectResponse,
)
from src.utils.database_helpers import paginate_query
from src.utils.entity_helpers import get_entity_by_id, update_simple_entity
from src.utils.auth import get_current_user_id

router = APIRouter()


@router.post("/settings", response_model=SingleObjectResponse, status_code=201)
async def create_setting(
    setting_data: SettingCreate,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Create or update a setting"""
    # Check if setting already exists
    existing_setting = db.query(Settings).filter(
        Settings.clerk_user_id == user_id,
        Settings.key == setting_data.key
    ).first()
    
    if existing_setting:
        # Update existing
        existing_setting.value = setting_data.value
        db.commit()
        db.refresh(existing_setting)
        message = "Setting updated successfully"
        setting = existing_setting
    else:
        # Create new
        setting = Settings(
            clerk_user_id=user_id,
            key=setting_data.key,
            value=setting_data.value
        )
        db.add(setting)
        db.commit()
        db.refresh(setting)
        message = "Setting created successfully"
    
    return {
        "message": message,
        "object": SettingSchema.model_validate(setting)
    }


@router.get("/settings", response_model=MultipleObjectsResponse)
async def list_settings(
    page: int = Query(DEFAULT_PAGE, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE, description=f"Items per page (max {MAX_PAGE_SIZE})"),
    key_filter: Optional[str] = Query(None, description="Filter by setting key (exact match)"),
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """List settings with pagination and filtering"""
    query = db.query(Settings).filter(Settings.clerk_user_id == user_id)
    
    # Apply filters
    if key_filter:
        query = query.filter(Settings.key == key_filter)
    
    # Order by key
    query = query.order_by(Settings.key)
    
    # Paginate
    total_count = query.count()
    settings, _, _ = paginate_query(query, page, page_size)
    
    return {
        "message": f"Retrieved {total_count} settings",
        "objects": [SettingSchema.model_validate(setting) for setting in settings],
        "pagination": {
            "total": total_count,
            "page": page,
            "page_size": page_size,
            "total_pages": (total_count + page_size - 1) // page_size
        },
        "filters": {
            "key_filter": key_filter,
        },
        "sorting": {
            "sort_by": "key",
            "sort_order": "asc"
        }
    }


@router.get("/settings/{setting_id}", response_model=SingleObjectResponse)
async def get_setting(
    setting_id: int,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Get a specific setting"""
    return get_entity_by_id(db, user_id, setting_id, Settings, SettingSchema, "setting")


@router.post("/settings/{setting_id}/update", response_model=SingleObjectResponse)
async def update_setting(
    setting_id: int,
    setting_update: SettingCreate,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Update a setting"""
    setting = db.query(Settings).filter(
        Settings.id == setting_id,
        Settings.clerk_user_id == user_id
    ).first()
    if not setting:
        raise HTTPException(status_code=404, detail="Setting not found")
    
    # Check if another setting with the same key exists (excluding current)
    if setting_update.key != setting.key:
        existing = db.query(Settings).filter(
            Settings.clerk_user_id == user_id,
            Settings.key == setting_update.key,
            Settings.id != setting_id
        ).first()
        if existing:
            raise HTTPException(status_code=400, detail="Setting with this key already exists")
    
    setting.key = setting_update.key
    setting.value = setting_update.value
    
    db.commit()
    db.refresh(setting)
    
    return {
        "message": "Setting updated successfully",
        "object": SettingSchema.model_validate(setting)
    }


@router.post("/settings/bulk/delete", response_model=BulkDeleteResponse)
async def bulk_delete_settings(
    delete_data: BulkDeleteRequest,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Bulk delete settings"""
    deleted_count = 0
    
    for setting_id in delete_data.ids:
        try:
            setting = db.query(Settings).filter(
                Settings.id == setting_id,
                Settings.clerk_user_id == user_id
            ).first()
            if setting:
                db.delete(setting)
                deleted_count += 1
        except Exception:
            continue  # Skip if any error occurs
    
    db.commit()
    
    return {
        "message": f"Deleted {deleted_count} settings",
        "processed": len(delete_data.ids),
        "requested": len(delete_data.ids),
        "deleted": deleted_count
    }


@router.get("/settings/key/{key}", response_model=SingleObjectResponse)
async def get_setting_by_key(
    key: str,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Get a setting by key"""
    setting = db.query(Settings).filter(
        Settings.clerk_user_id == user_id,
        Settings.key == key
    ).first()
    
    if not setting:
        raise HTTPException(status_code=404, detail="Setting not found")
    
    return {
        "message": "Setting retrieved successfully",
        "object": SettingSchema.model_validate(setting)
    }


@router.post("/settings/key/{key}", response_model=SingleObjectResponse)
async def set_setting_by_key(
    key: str,
    setting_data: SettingCreate,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Create or update a setting by key"""
    # Ensure the key in the URL matches the key in the data
    if setting_data.key != key:
        raise HTTPException(status_code=400, detail="Key in URL must match key in request body")
    
    # Check if setting already exists
    existing_setting = db.query(Settings).filter(
        Settings.clerk_user_id == user_id,
        Settings.key == key
    ).first()
    
    if existing_setting:
        # Update existing
        existing_setting.value = setting_data.value
        db.commit()
        db.refresh(existing_setting)
        message = "Setting updated successfully"
        setting = existing_setting
    else:
        # Create new
        setting = Settings(
            clerk_user_id=user_id,
            key=key,
            value=setting_data.value
        )
        db.add(setting)
        db.commit()
        db.refresh(setting)
        message = "Setting created successfully"
    
    return {
        "message": message,
        "object": SettingSchema.model_validate(setting)
    }