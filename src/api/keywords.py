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
    AdCampaign,
    AdCampaignKeyword,
    AdGroup,
    AdGroupKeyword,
    Company,
    CompanyKeyword,
    Keyword,
)
from src.schemas.schemas import (
    BulkDeleteRequest,
    BulkDeleteResponse,
    BulkKeywordCreate,
    BulkKeywordCreateRelations,
    BulkKeywordCreateResponse,
    BulkRelationCreateResponse,
    BulkTrashRequest,
    CompanyKeywordRelation,
    AdCampaignKeywordRelation,
    AdGroupKeywordRelation,
    Keyword as KeywordSchema,
    KeywordCreate,
    MultipleObjectsResponse,
    SingleObjectResponse,
)
from src.utils.bulk_helpers import bulk_delete_with_batches, process_in_batches
from src.utils.database_helpers import paginate_query
from src.utils.entity_helpers import get_entity_by_id, update_simple_entity
from src.utils.metadata_helpers import get_keywords_metadata
from src.utils.auth import get_current_user_id

router = APIRouter()


# Helper functions for keyword listing
def _get_project_entity_ids(db: Session, user_id: str, project_id: Optional[int] = None) -> tuple[list[int], list[int], list[int]]:
    """Get IDs of entities attached to a specific project, or all entities if no project specified."""
    if project_id is None:
        # Return all entities for the user
        from src.models.models import Company, AdCampaign, AdGroup
        
        company_ids = db.query(Company.id).filter(Company.clerk_user_id == user_id).all()
        campaign_ids = db.query(AdCampaign.id).filter(AdCampaign.clerk_user_id == user_id).all()
        adgroup_ids = db.query(AdGroup.id).filter(AdGroup.clerk_user_id == user_id).all()
        
        return (
            [c[0] for c in company_ids],
            [c[0] for c in campaign_ids],
            [a[0] for a in adgroup_ids]
        )
    
    # Get entities attached to the specified project
    from src.models.models import ProjectCompany, ProjectAdCampaign, ProjectAdGroup
    
    company_ids = db.query(ProjectCompany.company_id).filter(
        ProjectCompany.project_id == project_id
    ).all()
    
    campaign_ids = db.query(ProjectAdCampaign.ad_campaign_id).filter(
        ProjectAdCampaign.project_id == project_id
    ).all()
    
    adgroup_ids = db.query(ProjectAdGroup.ad_group_id).filter(
        ProjectAdGroup.project_id == project_id
    ).all()
    
    return (
        [c[0] for c in company_ids],
        [c[0] for c in campaign_ids],
        [a[0] for a in adgroup_ids]
    )


def _create_match_type_condition(user_id: str, match_field: str, match_value: bool):
    """Create an EXISTS condition for a match type across all three relation tables.
    match_value: True = positive match, False = negative match"""
    from sqlalchemy import or_, exists

    return or_(
        exists().where(
            CompanyKeyword.keyword_id == Keyword.id,
            CompanyKeyword.clerk_user_id == user_id,
            getattr(CompanyKeyword, match_field) == match_value
        ),
        exists().where(
            AdCampaignKeyword.keyword_id == Keyword.id,
            AdCampaignKeyword.clerk_user_id == user_id,
            getattr(AdCampaignKeyword, match_field) == match_value
        ),
        exists().where(
            AdGroupKeyword.keyword_id == Keyword.id,
            AdGroupKeyword.clerk_user_id == user_id,
            getattr(AdGroupKeyword, match_field) == match_value
        )
    )


def _create_match_type_sort_expr(user_id: str, match_field: str, match_value: bool = True):
    """Create a CASE expression for sorting by match type presence (returns 1 if present, 0 if not).
    match_value: True = positive match, False = negative match"""
    from sqlalchemy import case

    condition = _create_match_type_condition(user_id, match_field, match_value)
    return case((condition, 1), else_=0)


def _format_match_types(relation) -> dict:
    """Format match types from a relation object into a dictionary.
    None = not set, True = positive match, False = negative match"""
    return {
        "broad": relation.broad,
        "phrase": relation.phrase,
        "exact": relation.exact,
        "pause": relation.pause
    }


def _fetch_relations_bulk(
    db: Session,
    keyword_ids: list[int],
    company_id_list: list[int],
    campaign_id_list: list[int],
    adgroup_id_list: list[int]
) -> tuple[dict, dict, dict]:
    """Fetch all relations for given keywords in bulk (3 queries instead of N*M queries)."""
    # Fetch company relations
    company_relations = {}
    if company_id_list:
        relations = db.query(CompanyKeyword).filter(
            CompanyKeyword.keyword_id.in_(keyword_ids),
            CompanyKeyword.company_id.in_(company_id_list)
        ).all()
        for rel in relations:
            key = (rel.keyword_id, rel.company_id)
            company_relations[key] = rel

    # Fetch campaign relations
    campaign_relations = {}
    if campaign_id_list:
        relations = db.query(AdCampaignKeyword).filter(
            AdCampaignKeyword.keyword_id.in_(keyword_ids),
            AdCampaignKeyword.ad_campaign_id.in_(campaign_id_list)
        ).all()
        for rel in relations:
            key = (rel.keyword_id, rel.ad_campaign_id)
            campaign_relations[key] = rel

    # Fetch ad group relations
    adgroup_relations = {}
    if adgroup_id_list:
        relations = db.query(AdGroupKeyword).filter(
            AdGroupKeyword.keyword_id.in_(keyword_ids),
            AdGroupKeyword.ad_group_id.in_(adgroup_id_list)
        ).all()
        for rel in relations:
            key = (rel.keyword_id, rel.ad_group_id)
            adgroup_relations[key] = rel

    return company_relations, campaign_relations, adgroup_relations


def _build_matrix_keyword_data(
    keyword,
    company_id_list: list[int],
    campaign_id_list: list[int],
    adgroup_id_list: list[int],
    company_relations: dict,
    campaign_relations: dict,
    adgroup_relations: dict
) -> dict:
    """Build keyword data in matrix format with entity columns using pre-fetched relations."""
    keyword_data = {
        "id": keyword.id,
        "keyword": keyword.keyword,
        "trash": keyword.trash,
        "created": keyword.created,
        "updated": keyword.updated,
        "relations": {
            "companies": {},
            "ad_campaigns": {},
            "ad_groups": {}
        }
    }

    # Add company match types as columns (lookup from pre-fetched dict)
    for company_id in company_id_list:
        relation = company_relations.get((keyword.id, company_id))
        if relation:
            keyword_data["relations"]["companies"][company_id] = _format_match_types(relation)

    # Add campaign match types as columns (lookup from pre-fetched dict)
    for campaign_id in campaign_id_list:
        relation = campaign_relations.get((keyword.id, campaign_id))
        if relation:
            keyword_data["relations"]["ad_campaigns"][campaign_id] = _format_match_types(relation)

    # Add ad group match types as columns (lookup from pre-fetched dict)
    for adgroup_id in adgroup_id_list:
        relation = adgroup_relations.get((keyword.id, adgroup_id))
        if relation:
            keyword_data["relations"]["ad_groups"][adgroup_id] = _format_match_types(relation)

    return keyword_data


def _create_keyword_relations(
    db: Session,
    keyword,
    company_ids: list[int],
    ad_campaign_ids: list[int],
    ad_group_ids: list[int],
    broad: Optional[bool],
    phrase: Optional[bool],
    exact: Optional[bool],
    pause: Optional[int],
    override_broad: Optional[bool],
    override_phrase: Optional[bool],
    override_exact: Optional[bool],
    override_pause: Optional[bool]
) -> tuple[int, int, int, list]:
    """
    Create/update/delete keyword relations.
    Match types: None = not set, True = positive match, False = negative match
    Pause: None = not paused, 1 = paused
    A relation is deleted if all match types AND pause become None
    """

    def _process_entity_relations(
        entity_ids: list[int],
        model_class,
        entity_id_field: str
    ) -> tuple[int, int, int, list]:
        added = 0
        updated = 0
        deleted = 0
        relations = []

        for entity_id in entity_ids:
            # Query for existing relation
            filter_kwargs = {
                entity_id_field: entity_id,
                'keyword_id': keyword.id,
                'clerk_user_id': keyword.clerk_user_id
            }
            existing = db.query(model_class).filter_by(**filter_kwargs).first()

            if existing:
                # Update existing relation - allow setting values if override=true OR the field is currently null
                relation_updated = False
                if (override_broad is True or existing.broad is None) and existing.broad != broad:
                    existing.broad = broad
                    relation_updated = True
                    
                if (override_phrase is True or existing.phrase is None) and existing.phrase != phrase:
                    existing.phrase = phrase
                    relation_updated = True
                    
                if (override_exact is True or existing.exact is None) and existing.exact != exact:
                    existing.exact = exact
                    relation_updated = True

                if (override_pause is True or existing.pause is None) and existing.pause != pause:
                    existing.pause = pause
                    relation_updated = True

                if relation_updated:
                    # Check if all match types are None after update
                    if existing.broad is None and existing.phrase is None and existing.exact is None and existing.pause is None:
                        # Delete the relation since all match types are None
                        db.delete(existing)
                        deleted += 1
                        # Return the deleted relation info to frontend with None values
                        class DeletedRelation:
                            def __init__(self, original):
                                # Copy all attributes from the original relation
                                for attr in dir(original):
                                    if not attr.startswith('_') and not callable(getattr(original, attr)):
                                        try:
                                            setattr(self, attr, getattr(original, attr))
                                        except:
                                            pass
                                # Set all match types to None to indicate deletion
                                self.broad = None
                                self.phrase = None
                                self.exact = None
                                self.pause = None
                        
                        deleted_relation = DeletedRelation(existing)
                        relations.append(deleted_relation)
                    else:
                        updated += 1
                        relations.append(existing)
            else:
                # Only create new relation if at least one match type or pause is not None
                if broad is not None or phrase is not None or exact is not None or pause is not None:
                    create_kwargs = {
                        entity_id_field: entity_id,
                        'keyword_id': keyword.id,
                        'clerk_user_id': keyword.clerk_user_id,
                        'broad': broad,
                        'phrase': phrase,
                        'exact': exact,
                        'pause': pause
                    }
                    new_relation = model_class(**create_kwargs)
                    db.add(new_relation)
                    added += 1
                    relations.append(new_relation)

        return added, updated, deleted, relations

    relations_created = 0
    relations_updated = 0
    relations_deleted = 0
    all_relations = []

    # Handle company relations
    if company_ids:
        added, updated, deleted, relations = _process_entity_relations(
            company_ids,
            CompanyKeyword,
            'company_id'
        )
        relations_created += added
        relations_updated += updated
        relations_deleted += deleted
        all_relations.extend(relations)

    # Handle campaign relations
    if ad_campaign_ids:
        added, updated, deleted, relations = _process_entity_relations(
            ad_campaign_ids,
            AdCampaignKeyword,
            'ad_campaign_id'
        )
        relations_created += added
        relations_updated += updated
        relations_deleted += deleted
        all_relations.extend(relations)

    # Handle ad group relations
    if ad_group_ids:
        added, updated, deleted, relations = _process_entity_relations(
            ad_group_ids,
            AdGroupKeyword,
            'ad_group_id'
        )
        relations_created += added
        relations_updated += updated
        relations_deleted += deleted
        all_relations.extend(relations)

    return relations_created, relations_updated, relations_deleted, all_relations


@router.post("/keywords/bulk", response_model=BulkKeywordCreateResponse, status_code=201)
async def create_bulk_keywords(
    bulk_data: BulkKeywordCreate,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    created_keywords = []
    existing_keywords = []
    total_relations_created = 0
    total_relations_updated = 0
    batches_processed = 0

    # Process keywords in batches
    for keyword_batch in process_in_batches(bulk_data.keywords):
        batch_created = []
        batch_existing = []
        batch_relations_created = 0
        batch_relations_updated = 0

        for keyword_text in keyword_batch:
            keyword_text = keyword_text.strip()
            if not keyword_text:
                continue

            # Try to get existing keyword or create new one
            keyword = db.query(Keyword).filter(
                Keyword.keyword == keyword_text,
                Keyword.clerk_user_id == user_id
            ).first()

            if keyword:
                batch_existing.append(keyword)
            else:
                keyword = Keyword(
                    keyword=keyword_text,
                    clerk_user_id=user_id
                )
                db.add(keyword)
                db.flush()  # Get the ID without committing
                batch_created.append(keyword)

            # Create relations using helper function
            added, updated, deleted, _ = _create_keyword_relations(
                db=db,
                keyword=keyword,
                company_ids=bulk_data.company_ids,
                ad_campaign_ids=bulk_data.ad_campaign_ids,
                ad_group_ids=bulk_data.ad_group_ids,
                broad=bulk_data.broad,
                phrase=bulk_data.phrase,
                exact=bulk_data.exact,
                pause=bulk_data.pause,
                override_broad=bulk_data.override_broad,
                override_phrase=bulk_data.override_phrase,
                override_exact=bulk_data.override_exact,
                override_pause=bulk_data.override_pause
            )
            batch_relations_created += added
            batch_relations_updated += updated

        # Commit after each batch
        db.commit()
        created_keywords.extend(batch_created)
        existing_keywords.extend(batch_existing)
        total_relations_created += batch_relations_created
        total_relations_updated += batch_relations_updated
        batches_processed += 1

    # Calculate totals
    all_keywords = created_keywords + existing_keywords

    return BulkKeywordCreateResponse(
        message=f"Created {len(created_keywords)} new keywords, found {len(existing_keywords)} existing",
        objects=[KeywordSchema.model_validate(k) for k in all_keywords],
        created=len(created_keywords),
        existing=len(existing_keywords),
        processed=len(all_keywords),
        requested=len(bulk_data.keywords),
        relations_created=total_relations_created,
        relations_updated=total_relations_updated,
    )


def _delete_keyword_relations(
    db: Session,
    keyword,
    company_ids: list[int],
    ad_campaign_ids: list[int],
    ad_group_ids: list[int],
    user_id: str
) -> int:
    """Delete keyword relations for specified entities."""
    deleted_count = 0

    # Delete company relations
    if company_ids:
        deleted = db.query(CompanyKeyword).filter(
            CompanyKeyword.keyword_id == keyword.id,
            CompanyKeyword.company_id.in_(company_ids),
            CompanyKeyword.clerk_user_id == user_id
        ).delete()
        deleted_count += deleted

    # Delete campaign relations
    if ad_campaign_ids:
        deleted = db.query(AdCampaignKeyword).filter(
            AdCampaignKeyword.keyword_id == keyword.id,
            AdCampaignKeyword.ad_campaign_id.in_(ad_campaign_ids),
            AdCampaignKeyword.clerk_user_id == user_id
        ).delete()
        deleted_count += deleted

    # Delete ad group relations
    if ad_group_ids:
        deleted = db.query(AdGroupKeyword).filter(
            AdGroupKeyword.keyword_id == keyword.id,
            AdGroupKeyword.ad_group_id.in_(ad_group_ids),
            AdGroupKeyword.clerk_user_id == user_id
        ).delete()
        deleted_count += deleted

    return deleted_count


@router.post("/keywords/bulk/relations", response_model=BulkRelationCreateResponse)
async def bulk_upsert_keyword_relations(
    upsert_data: BulkKeywordCreateRelations,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    # Get keywords that belong to user
    keywords = db.query(Keyword).filter(
        Keyword.id.in_(upsert_data.keyword_ids),
        Keyword.clerk_user_id == user_id
    ).all()

    total_relations_created = 0
    total_relations_updated = 0
    total_relations_deleted = 0
    all_relations = []
    batches_processed = 0

    # Process keywords in batches
    for keyword_batch in process_in_batches(keywords):
        batch_relations_created = 0
        batch_relations_updated = 0
        batch_relations_deleted = 0
        batch_relations = []

        # Process each keyword in the batch
        for keyword in keyword_batch:
            # Use the values directly - they can be None, True, or False
            added, updated, deleted, relations = _create_keyword_relations(
                db=db,
                keyword=keyword,
                company_ids=upsert_data.company_ids,
                ad_campaign_ids=upsert_data.ad_campaign_ids,
                ad_group_ids=upsert_data.ad_group_ids,
                broad=upsert_data.broad,
                phrase=upsert_data.phrase,
                exact=upsert_data.exact,
                pause=upsert_data.pause,
                override_broad=upsert_data.override_broad,
                override_phrase=upsert_data.override_phrase,
                override_exact=upsert_data.override_exact,
                override_pause=upsert_data.override_pause
            )
            batch_relations_created += added
            batch_relations_updated += updated
            batch_relations_deleted += deleted
            batch_relations.extend(relations)

        # Flush to get IDs before committing
        db.flush()
        
        # Convert batch relations to response schemas before commit (only for create/update, not delete)
        for r in batch_relations:
            if hasattr(r, 'company_id'):
                all_relations.append(CompanyKeywordRelation.model_validate(r))
            elif hasattr(r, 'ad_campaign_id'):
                all_relations.append(AdCampaignKeywordRelation.model_validate(r))
            elif hasattr(r, 'ad_group_id'):
                all_relations.append(AdGroupKeywordRelation.model_validate(r))
        
        # Commit after each batch
        db.commit()
        total_relations_created += batch_relations_created
        total_relations_updated += batch_relations_updated
        total_relations_deleted += batch_relations_deleted
        batches_processed += 1

    return BulkRelationCreateResponse(
        message=f"Processed {len(keywords)} keywords",
        processed=len(keywords),
        requested=len(upsert_data.keyword_ids),
        created=total_relations_created,
        updated=total_relations_updated,
        deleted=total_relations_deleted,
        relations=all_relations,
    )


@router.get("/keywords", response_model=MultipleObjectsResponse)
async def list_keywords(
    project_id: Optional[int] = Query(None, description="Filter keywords by project (show only keywords attached to entities in this project). If not provided, show all keywords."),
    page: int = Query(DEFAULT_PAGE, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE, description=f"Items per page (max {MAX_PAGE_SIZE})"),
    only_attached: bool = Query(False, description="Show only keywords attached to at least one entity"),
    search: Optional[str] = Query(None, description="Search by keyword text (case-insensitive, partial match)"),
    created_after: Optional[datetime] = Query(None, description="Filter by created date (after)"),
    created_before: Optional[datetime] = Query(None, description="Filter by created date (before)"),
    updated_after: Optional[datetime] = Query(None, description="Filter by updated date (after)"),
    updated_before: Optional[datetime] = Query(None, description="Filter by updated date (before)"),
    has_broad: Optional[bool] = Query(None, description="Filter keywords with at least one broad match relation (True=positive, False=negative)"),
    has_phrase: Optional[bool] = Query(None, description="Filter keywords with at least one phrase match relation (True=positive, False=negative)"),
    has_exact: Optional[bool] = Query(None, description="Filter keywords with at least one exact match relation (True=positive, False=negative)"),
    trash: Optional[bool] = Query(None, description="Filter by trash status (True=trashed, False=not trashed, None=all)"),
    sort_by: Optional[str] = Query("created", description="Primary sort field: id, keyword, created, updated, has_broad, has_phrase, has_exact, trash"),
    sort_order: Optional[str] = Query("desc", description="Primary sort order: asc or desc"),
    sort_by_2: Optional[str] = Query(None, description="Secondary sort field (same options as sort_by)"),
    sort_order_2: Optional[str] = Query(None, description="Secondary sort order: asc or desc"),
    sort_by_3: Optional[str] = Query(None, description="Tertiary sort field (same options as sort_by)"),
    sort_order_3: Optional[str] = Query(None, description="Tertiary sort order: asc or desc"),
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    from sqlalchemy import or_, exists, and_, case, func, select

    # Get entity IDs based on project filter (or all entities if no project specified)
    company_id_list, campaign_id_list, adgroup_id_list = _get_project_entity_ids(db, user_id, project_id)

    # Build base query - start with user filter
    query = db.query(Keyword).filter(Keyword.clerk_user_id == user_id)

    # If project_id is specified, only include keywords that have relations to the project's entities
    if project_id:
        query = query.filter(
            or_(
                exists().where(
                    CompanyKeyword.keyword_id == Keyword.id,
                    CompanyKeyword.company_id.in_(company_id_list) if company_id_list else False
                ),
                exists().where(
                    AdCampaignKeyword.keyword_id == Keyword.id,
                    AdCampaignKeyword.ad_campaign_id.in_(campaign_id_list) if campaign_id_list else False
                ),
                exists().where(
                    AdGroupKeyword.keyword_id == Keyword.id,
                    AdGroupKeyword.ad_group_id.in_(adgroup_id_list) if adgroup_id_list else False
                )
            )
        )

    # Add search filter if provided
    if search:
        query = query.filter(Keyword.keyword.ilike(f"%{search}%"))

    # Add date filters
    if created_after:
        query = query.filter(Keyword.created >= created_after)
    if created_before:
        query = query.filter(Keyword.created <= created_before)
    if updated_after:
        query = query.filter(Keyword.updated >= updated_after)
    if updated_before:
        query = query.filter(Keyword.updated <= updated_before)

    # Add match type filters using helper function
    # Now: has_broad=True means positive broad, has_broad=False means negative broad
    match_type_params = {
        'broad': has_broad,
        'phrase': has_phrase,
        'exact': has_exact
    }

    match_type_filters = []
    for match_field, has_match in match_type_params.items():
        if has_match is not None:
            # has_match True/False now directly maps to positive/negative match
            condition = _create_match_type_condition(user_id, match_field, has_match)
            match_type_filters.append(condition)

    # Apply match type filters (AND condition - all must be satisfied)
    if match_type_filters:
        query = query.filter(and_(*match_type_filters))

    # Add trash filter
    if trash is not None:
        if trash:
            # Only trashed keywords (trash = True)
            query = query.filter(Keyword.trash == True)
        else:
            # Only not trashed keywords (trash IS NULL OR trash = False)
            query = query.filter((Keyword.trash == False) | (Keyword.trash.is_(None)))

    # If only_attached is True, add filter for keywords with at least one relation
    if only_attached:
        # Use EXISTS subqueries for all three relation types (OR condition)
        query = query.filter(
            or_(
                exists().where(
                    CompanyKeyword.keyword_id == Keyword.id,
                    CompanyKeyword.clerk_user_id == user_id
                ),
                exists().where(
                    AdCampaignKeyword.keyword_id == Keyword.id,
                    AdCampaignKeyword.clerk_user_id == user_id
                ),
                exists().where(
                    AdGroupKeyword.keyword_id == Keyword.id,
                    AdGroupKeyword.clerk_user_id == user_id
                )
            )
        )

    # Helper function to create match type sorting expressions
    def _get_sort_column(field_name: str):
        """Get the column or expression for sorting."""
        field_name = field_name.lower()

        # Simple field mappings
        simple_fields = {
            "id": Keyword.id,
            "keyword": Keyword.keyword,
            "created": Keyword.created,
            "updated": Keyword.updated,
            "trash": Keyword.trash
        }

        if field_name in simple_fields:
            return simple_fields[field_name]

        # Match type fields - use helper function (sorting by positive matches by default)
        match_type_map = {
            "has_broad": "broad",
            "has_phrase": "phrase",
            "has_exact": "exact"
        }

        if field_name in match_type_map:
            return _create_match_type_sort_expr(user_id, match_type_map[field_name], True)

        return None

    # Add sorting (up to 3 levels)
    sort_configs = [
        (sort_by, sort_order),
        (sort_by_2, sort_order_2),
        (sort_by_3, sort_order_3)
    ]

    order_columns = []
    for sort_field, sort_dir in sort_configs:
        if sort_field:
            sort_column = _get_sort_column(sort_field)
            if sort_column is not None:
                direction = (sort_dir or "desc").lower()
                if direction == "asc":
                    order_columns.append(sort_column.asc())
                else:
                    order_columns.append(sort_column.desc())

    # Apply sorting or default to created desc
    if order_columns:
        query = query.order_by(*order_columns)
    else:
        query = query.order_by(Keyword.created.desc())

    # Apply pagination AFTER all filters and sorting
    keywords, total_count, total_pages = paginate_query(query, page, page_size)

    # Always use matrix format - fetch all relations in bulk (3 queries instead of N*M queries)
    # When there are no active entities, the lists are empty and relations will be empty dicts
    keyword_ids = [k.id for k in keywords]
    company_relations, campaign_relations, adgroup_relations = _fetch_relations_bulk(
        db, keyword_ids, company_id_list, campaign_id_list, adgroup_id_list
    )

    # Build keyword data using pre-fetched relations (or empty dicts if no active entities)
    result_objects = [
        _build_matrix_keyword_data(
            keyword, company_id_list, campaign_id_list, adgroup_id_list,
            company_relations, campaign_relations, adgroup_relations
        )
        for keyword in keywords
    ]

    filters, sorting = get_keywords_metadata()

    # Update filters to include project info
    filters["project_id"] = project_id

    message = f"Retrieved {total_count} keywords"
    if project_id:
        message += f" for project {project_id}"

    return MultipleObjectsResponse(
        message=message,
        objects=result_objects,
        pagination={
            "total": total_count,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages
        },
        filters=filters,
        sorting=sorting,
    )


@router.get("/keywords/{keyword_id}", response_model=SingleObjectResponse)
async def get_keyword(
    keyword_id: int,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Get a specific keyword by ID"""
    return get_entity_by_id(
        db=db,
        user_id=user_id,
        entity_id=keyword_id,
        model_class=Keyword,
        schema_class=KeywordSchema,
        entity_name="keyword"
    )


@router.post("/keywords/{keyword_id}/update", response_model=SingleObjectResponse)
async def update_keyword(
    keyword_id: int,
    keyword_update: KeywordCreate,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Update a keyword"""
    update_fields = {"keyword": keyword_update.keyword}
    if keyword_update.trash is not None:
        update_fields["trash"] = keyword_update.trash
    
    return update_simple_entity(
        db=db,
        user_id=user_id,
        entity_id=keyword_id,
        model_class=Keyword,
        schema_class=KeywordSchema,
        entity_name="keyword",
        update_fields=update_fields
    )


@router.post("/keywords/bulk/trash", response_model=BulkDeleteResponse)
async def bulk_trash_keywords(
    trash_data: BulkTrashRequest,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Bulk trash/untrash keywords"""
    # Get keywords that belong to user
    keywords = db.query(Keyword).filter(
        Keyword.id.in_(trash_data.ids),
        Keyword.clerk_user_id == user_id
    ).all()

    updated_count = 0
    for keyword in keywords:
        keyword.trash = trash_data.trash
        updated_count += 1

    db.commit()

    action = "trashed" if trash_data.trash else "untrashed"
    return BulkDeleteResponse(
        message=f"Successfully {action} {updated_count} keywords",
        processed=len(keywords),
        requested=len(trash_data.ids),
        deleted=updated_count,  # Using deleted field for consistency with other bulk responses
    )


@router.post("/keywords/bulk/delete", response_model=BulkDeleteResponse)
async def bulk_delete_keywords(
    delete_data: BulkDeleteRequest,
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Bulk delete keywords"""
    return bulk_delete_with_batches(
        db=db,
        user_id=user_id,
        ids=delete_data.ids,
        model_class=Keyword,
        ownership_field="clerk_user_id",
        message_template="Deleted {0} keywords",
    )
