from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from src.core.database import get_db
from src.core.settings import (
    BATCH_SIZE,
    DEFAULT_PAGE,
    MAX_KEYWORDS_PER_REQUEST,
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
    BulkKeywordUpdateRelations,
    BulkRelationCreateResponse,
    BulkRelationUpdateResponse,
    Keyword as KeywordSchema,
    KeywordCreate,
    MultipleObjectsResponse,
    SingleObjectResponse,
)
from src.utils.helpers import (
    bulk_delete_with_batches,
    get_entity_by_id,
    get_keywords_metadata,
    update_simple_entity,
    paginate_query,
    process_in_batches,
)
from src.utils.auth import get_current_user_id

router = APIRouter()


# Helper functions for keyword listing
def _get_active_entity_ids(db: Session, user_id: str) -> tuple[list[int], list[int], list[int]]:
    """Get IDs of all active entities for the user using a single optimized query per entity type."""
    # Use scalar subqueries to get just the IDs efficiently
    company_ids = db.query(Company.id).filter(
        Company.clerk_user_id == user_id,
        Company.is_active == True
    ).all()

    campaign_ids = db.query(AdCampaign.id).filter(
        AdCampaign.clerk_user_id == user_id,
        AdCampaign.is_active == True
    ).all()

    adgroup_ids = db.query(AdGroup.id).filter(
        AdGroup.clerk_user_id == user_id,
        AdGroup.is_active == True
    ).all()

    return (
        [c[0] for c in company_ids],
        [c[0] for c in campaign_ids],
        [a[0] for a in adgroup_ids]
    )


def _create_match_type_condition(user_id: str, match_field: str):
    """Create an EXISTS condition for a match type across all three relation tables."""
    from sqlalchemy import or_, exists

    return or_(
        exists().where(
            CompanyKeyword.keyword_id == Keyword.id,
            CompanyKeyword.clerk_user_id == user_id,
            getattr(CompanyKeyword, match_field) == True
        ),
        exists().where(
            AdCampaignKeyword.keyword_id == Keyword.id,
            AdCampaignKeyword.clerk_user_id == user_id,
            getattr(AdCampaignKeyword, match_field) == True
        ),
        exists().where(
            AdGroupKeyword.keyword_id == Keyword.id,
            AdGroupKeyword.clerk_user_id == user_id,
            getattr(AdGroupKeyword, match_field) == True
        )
    )


def _create_match_type_sort_expr(user_id: str, match_field: str):
    """Create a CASE expression for sorting by match type presence (returns 1 if present, 0 if not)."""
    from sqlalchemy import case

    condition = _create_match_type_condition(user_id, match_field)
    return case((condition, 1), else_=0)


def _format_match_types(relation) -> dict:
    """Format match types from a relation object into a dictionary."""
    return {
        "broad": relation.broad,
        "phrase": relation.phrase,
        "exact": relation.exact,
        "neg_broad": relation.neg_broad,
        "neg_phrase": relation.neg_phrase,
        "neg_exact": relation.neg_exact
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


def _update_relation_match_types(assoc, update_data: BulkKeywordUpdateRelations) -> bool:
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
    company_ids: list[int],
    ad_campaign_ids: list[int],
    ad_group_ids: list[int],
    broad: bool,
    phrase: bool,
    exact: bool,
    neg_broad: bool,
    neg_phrase: bool,
    neg_exact: bool,
    override_broad: bool,
    override_phrase: bool,
    override_exact: bool,
    override_neg_broad: bool,
    override_neg_phrase: bool,
    override_neg_exact: bool
) -> tuple[int, int]:

    # Match types are now always provided with defaults

    def _process_entity_relations(
        entity_ids: list[int],
        model_class,
        entity_id_field: str
    ) -> tuple[int, int]:
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
                    existing.broad = broad
                    relation_updated = True
                if override_phrase:
                    existing.phrase = phrase
                    relation_updated = True
                if override_exact:
                    existing.exact = exact
                    relation_updated = True
                if override_neg_broad:
                    existing.neg_broad = neg_broad
                    relation_updated = True
                if override_neg_phrase:
                    existing.neg_phrase = neg_phrase
                    relation_updated = True
                if override_neg_exact:
                    existing.neg_exact = neg_exact
                    relation_updated = True

                if relation_updated:
                    updated += 1
            else:
                # Create new relation
                create_kwargs = {
                    entity_id_field: entity_id,
                    'keyword_id': keyword.id,
                    'clerk_user_id': keyword.clerk_user_id,
                    'broad': broad,
                    'phrase': phrase,
                    'exact': exact,
                    'neg_broad': neg_broad,
                    'neg_phrase': neg_phrase,
                    'neg_exact': neg_exact
                }
                new_relation = model_class(**create_kwargs)
                db.add(new_relation)
                added += 1

        return added, updated

    relations_created = 0
    relations_updated = 0

    # Handle company relations
    if company_ids:
        added, updated = _process_entity_relations(
            company_ids,
            CompanyKeyword,
            'company_id'
        )
        relations_created += added
        relations_updated += updated

    # Handle campaign relations
    if ad_campaign_ids:
        added, updated = _process_entity_relations(
            ad_campaign_ids,
            AdCampaignKeyword,
            'ad_campaign_id'
        )
        relations_created += added
        relations_updated += updated

    # Handle ad group relations
    if ad_group_ids:
        added, updated = _process_entity_relations(
            ad_group_ids,
            AdGroupKeyword,
            'ad_group_id'
        )
        relations_created += added
        relations_updated += updated

    return relations_created, relations_updated


@router.post("/keywords/bulk", response_model=BulkKeywordCreateResponse, status_code=201)
async def create_bulk_keywords(
    bulk_data: BulkKeywordCreate,
    batch_size: int = Query(BATCH_SIZE, ge=1, description="Batch size for processing"),
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    created_keywords = []
    existing_keywords = []
    total_relations_created = 0
    total_relations_updated = 0
    batches_processed = 0

    # Process keywords in batches
    for keyword_batch in process_in_batches(bulk_data.keywords, batch_size=batch_size):
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
            added, updated = _create_keyword_relations(
                db=db,
                keyword=keyword,
                company_ids=bulk_data.company_ids,
                ad_campaign_ids=bulk_data.ad_campaign_ids,
                ad_group_ids=bulk_data.ad_group_ids,
                broad=bulk_data.broad,
                phrase=bulk_data.phrase,
                exact=bulk_data.exact,
                neg_broad=bulk_data.neg_broad,
                neg_phrase=bulk_data.neg_phrase,
                neg_exact=bulk_data.neg_exact,
                override_broad=bulk_data.override_broad,
                override_phrase=bulk_data.override_phrase,
                override_exact=bulk_data.override_exact,
                override_neg_broad=bulk_data.override_neg_broad,
                override_neg_phrase=bulk_data.override_neg_phrase,
                override_neg_exact=bulk_data.override_neg_exact
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
        objects=[KeywordSchema.from_orm(k) for k in all_keywords],
        created=len(created_keywords),
        existing=len(existing_keywords),
        processed=len(all_keywords),
        requested=len(bulk_data.keywords),
        relations_created=total_relations_created,
        relations_updated=total_relations_updated,
        batches_processed=batches_processed,
        batch_size=batch_size
    )


@router.post("/keywords/bulk/relations/update", response_model=BulkRelationUpdateResponse)
async def bulk_update_keyword_relations(
    update_data: BulkKeywordUpdateRelations,
    batch_size: int = Query(BATCH_SIZE, ge=1, description="Batch size for processing"),
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    # Limit to MAX_KEYWORDS_PER_REQUEST keywords per request
    if len(update_data.keyword_ids) > MAX_KEYWORDS_PER_REQUEST:
        raise HTTPException(
            status_code=400,
            detail=f"Maximum {MAX_KEYWORDS_PER_REQUEST} keywords allowed per request"
        )

    # Get keywords that belong to user
    keywords = db.query(Keyword).filter(
        Keyword.id.in_(update_data.keyword_ids),
        Keyword.clerk_user_id == user_id
    ).all()

    relations_updated = 0
    batches_processed = 0

    # Process keywords in batches of DEFAULT_BATCH_SIZE
    for keyword_batch in process_in_batches(keywords, batch_size=batch_size):
        # Process each keyword in the batch
        for keyword in keyword_batch:
            # Update company relations (with ownership check)
            company_relations = db.query(CompanyKeyword).filter(
                CompanyKeyword.keyword_id == keyword.id,
                CompanyKeyword.clerk_user_id == user_id
            ).all()

            for assoc in company_relations:
                if _update_relation_match_types(assoc, update_data):
                    relations_updated += 1

            # Update campaign relations (with ownership check)
            campaign_relations = db.query(AdCampaignKeyword).filter(
                AdCampaignKeyword.keyword_id == keyword.id,
                AdCampaignKeyword.clerk_user_id == user_id
            ).all()

            for assoc in campaign_relations:
                if _update_relation_match_types(assoc, update_data):
                    relations_updated += 1

            # Update ad group relations (with ownership check)
            ad_group_relations = db.query(AdGroupKeyword).filter(
                AdGroupKeyword.keyword_id == keyword.id,
                AdGroupKeyword.clerk_user_id == user_id
            ).all()

            for assoc in ad_group_relations:
                if _update_relation_match_types(assoc, update_data):
                    relations_updated += 1

        # Commit after each batch
        db.commit()
        batches_processed += 1

    return BulkRelationUpdateResponse(
        message=f"Updated {relations_updated} relations for {len(keywords)} keywords",
        processed=len(keywords),
        requested=len(update_data.keyword_ids),
        updated=relations_updated,
        batches_processed=batches_processed,
        batch_size=batch_size
    )


@router.post("/keywords/bulk/relations", response_model=BulkRelationCreateResponse)
async def bulk_create_keyword_relations(
    create_data: BulkKeywordCreateRelations,
    batch_size: int = Query(BATCH_SIZE, ge=1, description="Batch size for processing"),
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    # Get keywords that belong to user
    keywords = db.query(Keyword).filter(
        Keyword.id.in_(create_data.keyword_ids),
        Keyword.clerk_user_id == user_id
    ).all()

    total_relations_created = 0
    total_relations_updated = 0
    batches_processed = 0

    # Process keywords in batches
    for keyword_batch in process_in_batches(keywords, batch_size=batch_size):
        batch_relations_created = 0
        batch_relations_updated = 0

        # Process each keyword in the batch
        for keyword in keyword_batch:
            added, updated = _create_keyword_relations(
                db=db,
                keyword=keyword,
                company_ids=create_data.company_ids,
                ad_campaign_ids=create_data.ad_campaign_ids,
                ad_group_ids=create_data.ad_group_ids,
                broad=create_data.broad,
                phrase=create_data.phrase,
                exact=create_data.exact,
                neg_broad=create_data.neg_broad,
                neg_phrase=create_data.neg_phrase,
                neg_exact=create_data.neg_exact,
                override_broad=create_data.override_broad,
                override_phrase=create_data.override_phrase,
                override_exact=create_data.override_exact,
                override_neg_broad=create_data.override_neg_broad,
                override_neg_phrase=create_data.override_neg_phrase,
                override_neg_exact=create_data.override_neg_exact
            )
            batch_relations_created += added
            batch_relations_updated += updated

        # Commit after each batch
        db.commit()
        total_relations_created += batch_relations_created
        total_relations_updated += batch_relations_updated
        batches_processed += 1

    return BulkRelationCreateResponse(
        message=f"Processed {len(keywords)} keywords",
        processed=len(keywords),
        requested=len(create_data.keyword_ids),
        created=total_relations_created,
        updated=total_relations_updated,
        batches_processed=batches_processed,
        batch_size=batch_size
    )


@router.get("/keywords", response_model=MultipleObjectsResponse)
async def list_keywords(
    page: int = Query(DEFAULT_PAGE, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE, description=f"Items per page (max {MAX_PAGE_SIZE})"),
    only_attached: bool = Query(False, description="Show only keywords attached to at least one entity"),
    search: Optional[str] = Query(None, description="Search by keyword text (case-insensitive, partial match)"),
    created_after: Optional[datetime] = Query(None, description="Filter by created date (after)"),
    created_before: Optional[datetime] = Query(None, description="Filter by created date (before)"),
    updated_after: Optional[datetime] = Query(None, description="Filter by updated date (after)"),
    updated_before: Optional[datetime] = Query(None, description="Filter by updated date (before)"),
    has_broad: Optional[bool] = Query(None, description="Filter keywords with at least one broad match relation"),
    has_phrase: Optional[bool] = Query(None, description="Filter keywords with at least one phrase match relation"),
    has_exact: Optional[bool] = Query(None, description="Filter keywords with at least one exact match relation"),
    has_neg_broad: Optional[bool] = Query(None, description="Filter keywords with at least one negative broad match relation"),
    has_neg_phrase: Optional[bool] = Query(None, description="Filter keywords with at least one negative phrase match relation"),
    has_neg_exact: Optional[bool] = Query(None, description="Filter keywords with at least one negative exact match relation"),
    sort_by: Optional[str] = Query("created", description="Primary sort field: id, keyword, created, updated, has_broad, has_phrase, has_exact, has_neg_broad, has_neg_phrase, has_neg_exact"),
    sort_order: Optional[str] = Query("desc", description="Primary sort order: asc or desc"),
    sort_by_2: Optional[str] = Query(None, description="Secondary sort field (same options as sort_by)"),
    sort_order_2: Optional[str] = Query(None, description="Secondary sort order: asc or desc"),
    sort_by_3: Optional[str] = Query(None, description="Tertiary sort field (same options as sort_by)"),
    sort_order_3: Optional[str] = Query(None, description="Tertiary sort order: asc or desc"),
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    from sqlalchemy import or_, exists, and_, case, func, select

    # Get active entity IDs efficiently (just IDs, not full objects)
    company_id_list, campaign_id_list, adgroup_id_list = _get_active_entity_ids(db, user_id)

    # Build base query - start with user filter
    query = db.query(Keyword).filter(Keyword.clerk_user_id == user_id)

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
    match_type_params = {
        'broad': has_broad,
        'phrase': has_phrase,
        'exact': has_exact,
        'neg_broad': has_neg_broad,
        'neg_phrase': has_neg_phrase,
        'neg_exact': has_neg_exact
    }

    match_type_filters = []
    for match_field, has_match in match_type_params.items():
        if has_match is not None:
            condition = _create_match_type_condition(user_id, match_field)
            match_type_filters.append(condition if has_match else ~condition)

    # Apply match type filters (AND condition - all must be satisfied)
    if match_type_filters:
        query = query.filter(and_(*match_type_filters))

    # Filter keywords that have relations with active entities (OR condition)
    # Use EXISTS subqueries for optimal performance
    # Only apply if there are active entities to filter by
    if company_id_list or campaign_id_list or adgroup_id_list:
        filters = []

        if company_id_list:
            filters.append(
                exists().where(
                    CompanyKeyword.keyword_id == Keyword.id,
                    CompanyKeyword.company_id.in_(company_id_list)
                )
            )

        if campaign_id_list:
            filters.append(
                exists().where(
                    AdCampaignKeyword.keyword_id == Keyword.id,
                    AdCampaignKeyword.ad_campaign_id.in_(campaign_id_list)
                )
            )

        if adgroup_id_list:
            filters.append(
                exists().where(
                    AdGroupKeyword.keyword_id == Keyword.id,
                    AdGroupKeyword.ad_group_id.in_(adgroup_id_list)
                )
            )

        query = query.filter(or_(*filters))

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
            "updated": Keyword.updated
        }

        if field_name in simple_fields:
            return simple_fields[field_name]

        # Match type fields - use helper function
        match_type_map = {
            "has_broad": "broad",
            "has_phrase": "phrase",
            "has_exact": "exact",
            "has_neg_broad": "neg_broad",
            "has_neg_phrase": "neg_phrase",
            "has_neg_exact": "neg_exact"
        }

        if field_name in match_type_map:
            return _create_match_type_sort_expr(user_id, match_type_map[field_name])

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

    return MultipleObjectsResponse(
        message=f"Retrieved {total_count} keywords",
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
    return update_simple_entity(
        db=db,
        user_id=user_id,
        entity_id=keyword_id,
        model_class=Keyword,
        schema_class=KeywordSchema,
        entity_name="keyword",
        update_fields={"keyword": keyword_update.keyword}
    )


@router.post("/keywords/bulk/delete", response_model=BulkDeleteResponse)
async def bulk_delete_keywords(
    delete_data: BulkDeleteRequest,
    batch_size: int = Query(BATCH_SIZE, ge=1, description="Batch size for processing"),
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
        batch_size=batch_size
    )


@router.post("/relations/ad_company_keyword/bulk/delete", response_model=BulkDeleteResponse)
async def bulk_delete_company_keyword_relations(
    delete_data: BulkDeleteRequest,
    batch_size: int = Query(BATCH_SIZE, ge=1, description="Batch size for processing"),
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Bulk delete ad_company_keyword relations"""
    return bulk_delete_with_batches(
        db=db,
        user_id=user_id,
        ids=delete_data.ids,
        model_class=CompanyKeyword,
        ownership_field="clerk_user_id",
        message_template="Deleted {0} ad_company_keyword relations",
        batch_size=batch_size
    )


@router.post("/relations/ad_campaign_keyword/bulk/delete", response_model=BulkDeleteResponse)
async def bulk_delete_campaign_keyword_relations(
    delete_data: BulkDeleteRequest,
    batch_size: int = Query(BATCH_SIZE, ge=1, description="Batch size for processing"),
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Bulk delete ad_campaign_keyword relations"""
    return bulk_delete_with_batches(
        db=db,
        user_id=user_id,
        ids=delete_data.ids,
        model_class=AdCampaignKeyword,
        ownership_field="clerk_user_id",
        message_template="Deleted {0} ad_campaign_keyword relations",
        batch_size=batch_size
    )


@router.post("/relations/ad_group_keyword/bulk/delete", response_model=BulkDeleteResponse)
async def bulk_delete_adgroup_keyword_relations(
    delete_data: BulkDeleteRequest,
    batch_size: int = Query(BATCH_SIZE, ge=1, description="Batch size for processing"),
    db: Session = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Bulk delete ad_group_keyword relations"""
    return bulk_delete_with_batches(
        db=db,
        user_id=user_id,
        ids=delete_data.ids,
        model_class=AdGroupKeyword,
        ownership_field="clerk_user_id",
        message_template="Deleted {0} ad_group_keyword relations",
        batch_size=batch_size
    )
