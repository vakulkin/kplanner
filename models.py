from sqlalchemy import Column, Integer, String, Boolean, TIMESTAMP, ForeignKey, Index, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from database import Base

class Company(Base):
    __tablename__ = "companies"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(255), nullable=False)
    clerk_user_id = Column(String(255), nullable=False, index=True)
    created = Column(TIMESTAMP, server_default=func.current_timestamp())
    updated = Column(TIMESTAMP, server_default=func.current_timestamp(), onupdate=func.current_timestamp())
    
    ad_campaigns = relationship("AdCampaign", back_populates="company", cascade="all, delete-orphan")
    keywords = relationship("Keyword", secondary="company_keyword", back_populates="companies")
    filters = relationship("Filter", secondary="company_filter", back_populates="companies")

class AdCampaign(Base):
    __tablename__ = "ad_campaigns"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(255), nullable=False)
    clerk_user_id = Column(String(255), nullable=False, index=True)
    created = Column(TIMESTAMP, server_default=func.current_timestamp())
    updated = Column(TIMESTAMP, server_default=func.current_timestamp(), onupdate=func.current_timestamp())
    company_id = Column(Integer, ForeignKey("companies.id", ondelete="CASCADE"))
    
    company = relationship("Company", back_populates="ad_campaigns")
    ad_groups = relationship("AdGroup", back_populates="ad_campaign", cascade="all, delete-orphan")
    keywords = relationship("Keyword", secondary="ad_campaign_keyword", back_populates="ad_campaigns")
    filters = relationship("Filter", secondary="ad_campaign_filter", back_populates="ad_campaigns")

class AdGroup(Base):
    __tablename__ = "ad_groups"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(255), nullable=False)
    clerk_user_id = Column(String(255), nullable=False, index=True)
    created = Column(TIMESTAMP, server_default=func.current_timestamp())
    updated = Column(TIMESTAMP, server_default=func.current_timestamp(), onupdate=func.current_timestamp())
    ad_campaign_id = Column(Integer, ForeignKey("ad_campaigns.id", ondelete="CASCADE"))
    
    ad_campaign = relationship("AdCampaign", back_populates="ad_groups")
    keywords = relationship("Keyword", secondary="ad_group_keyword", back_populates="ad_groups")
    filters = relationship("Filter", secondary="ad_group_filter", back_populates="ad_groups")

class Keyword(Base):
    __tablename__ = "keywords"
    __table_args__ = (
        UniqueConstraint('keyword', 'clerk_user_id', name='unique_keyword_per_user'),
        Index('idx_clerk_user_id', 'clerk_user_id'),
    )
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    keyword = Column(String(255), nullable=False)
    clerk_user_id = Column(String(255), nullable=False)
    created = Column(TIMESTAMP, server_default=func.current_timestamp())
    updated = Column(TIMESTAMP, server_default=func.current_timestamp(), onupdate=func.current_timestamp())
    
    companies = relationship("Company", secondary="company_keyword", back_populates="keywords")
    ad_campaigns = relationship("AdCampaign", secondary="ad_campaign_keyword", back_populates="keywords")
    ad_groups = relationship("AdGroup", secondary="ad_group_keyword", back_populates="keywords")

class Filter(Base):
    __tablename__ = "filters"
    __table_args__ = (
        UniqueConstraint('filter', 'clerk_user_id', name='unique_filter_per_user'),
        Index('idx_clerk_user_id', 'clerk_user_id'),
    )
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    filter = Column(String(255), nullable=False)
    clerk_user_id = Column(String(255), nullable=False)
    created = Column(TIMESTAMP, server_default=func.current_timestamp())
    updated = Column(TIMESTAMP, server_default=func.current_timestamp(), onupdate=func.current_timestamp())
    
    companies = relationship("Company", secondary="company_filter", back_populates="filters")
    ad_campaigns = relationship("AdCampaign", secondary="ad_campaign_filter", back_populates="filters")
    ad_groups = relationship("AdGroup", secondary="ad_group_filter", back_populates="filters")

class AdGroupKeyword(Base):
    __tablename__ = "ad_group_keyword"
    __table_args__ = (
        UniqueConstraint('ad_group_id', 'keyword_id', name='unique_ad_group_keyword'),
    )
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    ad_group_id = Column(Integer, ForeignKey("ad_groups.id", ondelete="CASCADE"), nullable=False)
    keyword_id = Column(Integer, ForeignKey("keywords.id", ondelete="CASCADE"), nullable=False)
    broad = Column(Boolean, default=False)
    phrase = Column(Boolean, default=False)
    exact = Column(Boolean, default=False)
    neg_broad = Column(Boolean, default=False)
    neg_phrase = Column(Boolean, default=False)
    neg_exact = Column(Boolean, default=False)

class CompanyKeyword(Base):
    __tablename__ = "company_keyword"
    __table_args__ = (
        UniqueConstraint('company_id', 'keyword_id', name='unique_company_keyword'),
    )
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("companies.id", ondelete="CASCADE"), nullable=False)
    keyword_id = Column(Integer, ForeignKey("keywords.id", ondelete="CASCADE"), nullable=False)
    broad = Column(Boolean, default=False)
    phrase = Column(Boolean, default=False)
    exact = Column(Boolean, default=False)
    neg_broad = Column(Boolean, default=False)
    neg_phrase = Column(Boolean, default=False)
    neg_exact = Column(Boolean, default=False)

class AdCampaignKeyword(Base):
    __tablename__ = "ad_campaign_keyword"
    __table_args__ = (
        UniqueConstraint('ad_campaign_id', 'keyword_id', name='unique_ad_campaign_keyword'),
    )
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    ad_campaign_id = Column(Integer, ForeignKey("ad_campaigns.id", ondelete="CASCADE"), nullable=False)
    keyword_id = Column(Integer, ForeignKey("keywords.id", ondelete="CASCADE"), nullable=False)
    broad = Column(Boolean, default=False)
    phrase = Column(Boolean, default=False)
    exact = Column(Boolean, default=False)
    neg_broad = Column(Boolean, default=False)
    neg_phrase = Column(Boolean, default=False)
    neg_exact = Column(Boolean, default=False)

class AdGroupFilter(Base):
    __tablename__ = "ad_group_filter"
    __table_args__ = (
        UniqueConstraint('ad_group_id', 'filter_id', name='unique_ad_group_filter'),
    )
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    ad_group_id = Column(Integer, ForeignKey("ad_groups.id", ondelete="CASCADE"), nullable=False)
    filter_id = Column(Integer, ForeignKey("filters.id", ondelete="CASCADE"), nullable=False)
    is_negative = Column(Boolean, default=False)

class CompanyFilter(Base):
    __tablename__ = "company_filter"
    __table_args__ = (
        UniqueConstraint('company_id', 'filter_id', name='unique_company_filter'),
    )
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("companies.id", ondelete="CASCADE"), nullable=False)
    filter_id = Column(Integer, ForeignKey("filters.id", ondelete="CASCADE"), nullable=False)
    is_negative = Column(Boolean, default=False)

class AdCampaignFilter(Base):
    __tablename__ = "ad_campaign_filter"
    id = Column(Integer, primary_key=True, autoincrement=True)
    ad_campaign_id = Column(Integer, ForeignKey("ad_campaigns.id", ondelete="CASCADE"), primary_key=True)
    filter_id = Column(Integer, ForeignKey("filters.id", ondelete="CASCADE"), primary_key=True)
    is_negative = Column(Boolean, default=False)

