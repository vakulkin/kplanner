from sqlalchemy import Column, Integer, String, Boolean, TIMESTAMP, ForeignKey, Index, UniqueConstraint, DDL, event
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from ..core.database import Base


def create_relation_triggers(target, connection, **kw):
    """Create database triggers for cleaning up empty keyword relations."""
    dialect_name = connection.dialect.name
    
    if dialect_name == 'mysql':
        # MySQL triggers are not needed since cleanup is handled at application level
        # to avoid self-modification issues with triggers
        pass

    elif dialect_name == 'postgresql':
        # PostgreSQL trigger syntax
        company_keyword_trigger = DDL("""
            CREATE OR REPLACE FUNCTION delete_empty_company_keyword_func()
            RETURNS TRIGGER AS $$
            BEGIN
                IF NEW.broad IS NULL AND NEW.phrase IS NULL AND NEW.exact IS NULL AND NEW.pause IS NULL THEN
                    DELETE FROM company_keyword WHERE id = NEW.id;
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;

            CREATE TRIGGER delete_empty_company_keyword
                AFTER UPDATE ON company_keyword
                FOR EACH ROW
                EXECUTE FUNCTION delete_empty_company_keyword_func();
        """)

        ad_campaign_keyword_trigger = DDL("""
            CREATE OR REPLACE FUNCTION delete_empty_ad_campaign_keyword_func()
            RETURNS TRIGGER AS $$
            BEGIN
                IF NEW.broad IS NULL AND NEW.phrase IS NULL AND NEW.exact IS NULL AND NEW.pause IS NULL THEN
                    DELETE FROM ad_campaign_keyword WHERE id = NEW.id;
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;

            CREATE TRIGGER delete_empty_ad_campaign_keyword
                AFTER UPDATE ON ad_campaign_keyword
                FOR EACH ROW
                EXECUTE FUNCTION delete_empty_ad_campaign_keyword_func();
        """)

        ad_group_keyword_trigger = DDL("""
            CREATE OR REPLACE FUNCTION delete_empty_ad_group_keyword_func()
            RETURNS TRIGGER AS $$
            BEGIN
                IF NEW.broad IS NULL AND NEW.phrase IS NULL AND NEW.exact IS NULL AND NEW.pause IS NULL THEN
                    DELETE FROM ad_group_keyword WHERE id = NEW.id;
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;

            CREATE TRIGGER delete_empty_ad_group_keyword
                AFTER UPDATE ON ad_group_keyword
                FOR EACH ROW
                EXECUTE FUNCTION delete_empty_ad_group_keyword_func();
        """)

        # Execute the triggers
        connection.execute(company_keyword_trigger)
        connection.execute(ad_campaign_keyword_trigger)
        connection.execute(ad_group_keyword_trigger)

    # For SQLite and other databases, skip trigger creation silently


# Attach the trigger creation to fire after metadata create_all
event.listen(Base.metadata, 'after_create', create_relation_triggers)


class Company(Base):
    __tablename__ = "companies"
    
    id = Column(Integer, primary_key=True)
    title = Column(String(255), nullable=False)
    clerk_user_id = Column(String(255), nullable=False, index=True)
    created = Column(TIMESTAMP, server_default=func.current_timestamp())
    updated = Column(TIMESTAMP, server_default=func.current_timestamp(), onupdate=func.current_timestamp())
    
    ad_campaigns = relationship("AdCampaign", back_populates="company", cascade="all, delete-orphan")
    keywords = relationship("Keyword", secondary="company_keyword", back_populates="companies")
    projects = relationship("Project", secondary="project_company", back_populates="companies")


class AdCampaign(Base):
    __tablename__ = "ad_campaigns"
    
    id = Column(Integer, primary_key=True)
    title = Column(String(255), nullable=False)
    clerk_user_id = Column(String(255), nullable=False, index=True)
    created = Column(TIMESTAMP, server_default=func.current_timestamp())
    updated = Column(TIMESTAMP, server_default=func.current_timestamp(), onupdate=func.current_timestamp())
    company_id = Column(Integer, ForeignKey("companies.id", ondelete="CASCADE"))
    
    company = relationship("Company", back_populates="ad_campaigns")
    ad_groups = relationship("AdGroup", back_populates="ad_campaign", cascade="all, delete-orphan")
    keywords = relationship("Keyword", secondary="ad_campaign_keyword", back_populates="ad_campaigns")
    projects = relationship("Project", secondary="project_ad_campaign", back_populates="ad_campaigns")


class AdGroup(Base):
    __tablename__ = "ad_groups"
    
    id = Column(Integer, primary_key=True)
    title = Column(String(255), nullable=False)
    clerk_user_id = Column(String(255), nullable=False, index=True)
    created = Column(TIMESTAMP, server_default=func.current_timestamp())
    updated = Column(TIMESTAMP, server_default=func.current_timestamp(), onupdate=func.current_timestamp())
    ad_campaign_id = Column(Integer, ForeignKey("ad_campaigns.id", ondelete="CASCADE"))
    
    ad_campaign = relationship("AdCampaign", back_populates="ad_groups")
    keywords = relationship("Keyword", secondary="ad_group_keyword", back_populates="ad_groups")
    projects = relationship("Project", secondary="project_ad_group", back_populates="ad_groups")


class Project(Base):
    __tablename__ = "projects"
    __table_args__ = (
        Index('idx_project_clerk_user_id', 'clerk_user_id'),
    )
    
    id = Column(Integer, primary_key=True)
    title = Column(String(255), nullable=False)
    clerk_user_id = Column(String(255), nullable=False)
    created = Column(TIMESTAMP, server_default=func.current_timestamp())
    updated = Column(TIMESTAMP, server_default=func.current_timestamp(), onupdate=func.current_timestamp())
    
    # Relationships to entities (many-to-many)
    companies = relationship("Company", secondary="project_company", back_populates="projects")
    ad_campaigns = relationship("AdCampaign", secondary="project_ad_campaign", back_populates="projects")
    ad_groups = relationship("AdGroup", secondary="project_ad_group", back_populates="projects")


class Settings(Base):
    __tablename__ = "settings"
    __table_args__ = (
        UniqueConstraint('clerk_user_id', 'key', name='unique_user_setting'),
        Index('idx_settings_clerk_user_id', 'clerk_user_id'),
    )
    
    id = Column(Integer, primary_key=True)
    clerk_user_id = Column(String(255), nullable=False)
    key = Column(String(255), nullable=False)
    value = Column(String(1000), nullable=True)  # JSON string for complex values
    created = Column(TIMESTAMP, server_default=func.current_timestamp())
    updated = Column(TIMESTAMP, server_default=func.current_timestamp(), onupdate=func.current_timestamp())


# Association tables for project-entity relationships
class ProjectCompany(Base):
    __tablename__ = "project_company"
    __table_args__ = (
        UniqueConstraint('project_id', 'company_id', name='unique_project_company'),
        Index('idx_project_company_clerk_user_id', 'clerk_user_id'),
    )
    
    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id", ondelete="CASCADE"), nullable=False)
    company_id = Column(Integer, ForeignKey("companies.id", ondelete="CASCADE"), nullable=False)
    clerk_user_id = Column(String(255), nullable=False)
    created = Column(TIMESTAMP, server_default=func.current_timestamp())


class ProjectAdCampaign(Base):
    __tablename__ = "project_ad_campaign"
    __table_args__ = (
        UniqueConstraint('project_id', 'ad_campaign_id', name='unique_project_ad_campaign'),
        Index('idx_project_ad_campaign_clerk_user_id', 'clerk_user_id'),
    )
    
    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id", ondelete="CASCADE"), nullable=False)
    ad_campaign_id = Column(Integer, ForeignKey("ad_campaigns.id", ondelete="CASCADE"), nullable=False)
    clerk_user_id = Column(String(255), nullable=False)
    created = Column(TIMESTAMP, server_default=func.current_timestamp())


class ProjectAdGroup(Base):
    __tablename__ = "project_ad_group"
    __table_args__ = (
        UniqueConstraint('project_id', 'ad_group_id', name='unique_project_ad_group'),
        Index('idx_project_ad_group_clerk_user_id', 'clerk_user_id'),
    )
    
    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id", ondelete="CASCADE"), nullable=False)
    ad_group_id = Column(Integer, ForeignKey("ad_groups.id", ondelete="CASCADE"), nullable=False)
    clerk_user_id = Column(String(255), nullable=False)
    created = Column(TIMESTAMP, server_default=func.current_timestamp())


class Keyword(Base):
    __tablename__ = "keywords"
    __table_args__ = (
        UniqueConstraint('keyword', 'clerk_user_id', name='unique_keyword_per_user'),
        Index('idx_clerk_user_id', 'clerk_user_id'),
    )
    
    id = Column(Integer, primary_key=True)
    keyword = Column(String(255), nullable=False)
    clerk_user_id = Column(String(255), nullable=False)
    trash = Column(Boolean, nullable=True)
    created = Column(TIMESTAMP, server_default=func.current_timestamp())
    updated = Column(TIMESTAMP, server_default=func.current_timestamp(), onupdate=func.current_timestamp())
    
    companies = relationship("Company", secondary="company_keyword", back_populates="keywords")
    ad_campaigns = relationship("AdCampaign", secondary="ad_campaign_keyword", back_populates="keywords")
    ad_groups = relationship("AdGroup", secondary="ad_group_keyword", back_populates="keywords")

class AdGroupKeyword(Base):

    __tablename__ = "ad_group_keyword"
    __table_args__ = (
        UniqueConstraint('ad_group_id', 'keyword_id', name='unique_ad_group_keyword'),
        Index('idx_ad_group_keyword_clerk_user_id', 'clerk_user_id'),
    )
    
    id = Column(Integer, primary_key=True)
    ad_group_id = Column(Integer, ForeignKey("ad_groups.id", ondelete="CASCADE"), nullable=False)
    keyword_id = Column(Integer, ForeignKey("keywords.id", ondelete="CASCADE"), nullable=False)
    clerk_user_id = Column(String(255), nullable=False)
    # Nullable Boolean: None = not set, True = positive match, False = negative match
    broad = Column(Boolean, nullable=True, default=None)
    phrase = Column(Boolean, nullable=True, default=None)
    exact = Column(Boolean, nullable=True, default=None)
    # Nullable Integer: None = not paused, 1 = paused
    pause = Column(Integer, nullable=True, default=None)

class CompanyKeyword(Base):
    __tablename__ = "company_keyword"
    __table_args__ = (
        UniqueConstraint('company_id', 'keyword_id', name='unique_company_keyword'),
        Index('idx_company_keyword_clerk_user_id', 'clerk_user_id'),
    )
    
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey("companies.id", ondelete="CASCADE"), nullable=False)
    keyword_id = Column(Integer, ForeignKey("keywords.id", ondelete="CASCADE"), nullable=False)
    clerk_user_id = Column(String(255), nullable=False)
    # Nullable Boolean: None = not set, True = positive match, False = negative match
    broad = Column(Boolean, nullable=True, default=None)
    phrase = Column(Boolean, nullable=True, default=None)
    exact = Column(Boolean, nullable=True, default=None)
    # Nullable Integer: None = not paused, 1 = paused
    pause = Column(Integer, nullable=True, default=None)

class AdCampaignKeyword(Base):
    __tablename__ = "ad_campaign_keyword"
    __table_args__ = (
        UniqueConstraint('ad_campaign_id', 'keyword_id', name='unique_ad_campaign_keyword'),
        Index('idx_ad_campaign_keyword_clerk_user_id', 'clerk_user_id'),
    )
    
    id = Column(Integer, primary_key=True)
    ad_campaign_id = Column(Integer, ForeignKey("ad_campaigns.id", ondelete="CASCADE"), nullable=False)
    keyword_id = Column(Integer, ForeignKey("keywords.id", ondelete="CASCADE"), nullable=False)
    clerk_user_id = Column(String(255), nullable=False)
    # Nullable Boolean: None = not set, True = positive match, False = negative match
    broad = Column(Boolean, nullable=True, default=None)
    phrase = Column(Boolean, nullable=True, default=None)
    exact = Column(Boolean, nullable=True, default=None)
    # Nullable Integer: None = not paused, 1 = paused
    pause = Column(Integer, nullable=True, default=None)


class ColumnMapping(Base):
    """
    Table for storing column mapping rules for CSV export transformations.
    Allows users to define rules like:
    - Copy Company A broad matches → Add to Company B exact
    - Apply Campaign 1 phrase → Ad Group 5 broad
    
    Uses proper foreign keys with nullable fields to support any entity type combination.
    Only one source and one target entity should be set per mapping.
    """
    __tablename__ = "column_mappings"
    __table_args__ = (
        Index('idx_column_mapping_clerk_user_id', 'clerk_user_id'),
    )
    
    id = Column(Integer, primary_key=True)
    clerk_user_id = Column(String(255), nullable=False)
    
    # Source entity - only ONE of these should be set (nullable to support any entity type)
    source_company_id = Column(Integer, ForeignKey("companies.id", ondelete="CASCADE"), nullable=True)
    source_ad_campaign_id = Column(Integer, ForeignKey("ad_campaigns.id", ondelete="CASCADE"), nullable=True)
    source_ad_group_id = Column(Integer, ForeignKey("ad_groups.id", ondelete="CASCADE"), nullable=True)
    source_match_type = Column(String(50), nullable=False)  # 'broad', 'phrase', 'exact', 'neg_broad', 'neg_phrase', 'neg_exact'
    
    # Target entity - only ONE of these should be set (nullable to support any entity type)
    target_company_id = Column(Integer, ForeignKey("companies.id", ondelete="CASCADE"), nullable=True)
    target_ad_campaign_id = Column(Integer, ForeignKey("ad_campaigns.id", ondelete="CASCADE"), nullable=True)
    target_ad_group_id = Column(Integer, ForeignKey("ad_groups.id", ondelete="CASCADE"), nullable=True)
    target_match_type = Column(String(50), nullable=False)  # 'broad', 'phrase', 'exact', 'neg_broad', 'neg_phrase', 'neg_exact'
    
    # Metadata
    created = Column(TIMESTAMP, server_default=func.current_timestamp())
    updated = Column(TIMESTAMP, server_default=func.current_timestamp(), onupdate=func.current_timestamp())
    
    # Relationships
    source_company = relationship("Company", foreign_keys=[source_company_id])
    source_ad_campaign = relationship("AdCampaign", foreign_keys=[source_ad_campaign_id])
    source_ad_group = relationship("AdGroup", foreign_keys=[source_ad_group_id])
    
    target_company = relationship("Company", foreign_keys=[target_company_id])
    target_ad_campaign = relationship("AdCampaign", foreign_keys=[target_ad_campaign_id])
    target_ad_group = relationship("AdGroup", foreign_keys=[target_ad_group_id])


# Database triggers to automatically delete relations when all match types become NULL
# These triggers ensure data integrity by removing empty relations

from sqlalchemy import event, DDL


def ensure_relation_triggers_exist(engine):
    """Ensure database triggers exist for cleaning up empty keyword relations.
    
    This function can be called on every server start to ensure triggers are present,
    regardless of whether tables were just created or already existed.
    """
    with engine.connect() as connection:
        create_relation_triggers(None, connection)


# Attach the trigger creation to fire after metadata create_all
# Note: We also call ensure_relation_triggers_exist() directly in main.py
# to ensure triggers exist on every server start, not just when tables are created
event.listen(Base.metadata, 'after_create', create_relation_triggers)
