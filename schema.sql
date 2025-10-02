-- Create companies table: Represents top-level organizations or clients
CREATE TABLE companies (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    clerk_user_id VARCHAR(255) NOT NULL,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_clerk_user_id (clerk_user_id)
);

-- Create ad_campaigns table: Advertising campaigns under a company
CREATE TABLE ad_campaigns (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    clerk_user_id VARCHAR(255) NOT NULL,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    company_id INT,
    FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE,
    INDEX idx_clerk_user_id (clerk_user_id)
);

-- Create ad_groups table: Sub-groups within a campaign for organizing keywords
CREATE TABLE ad_groups (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    clerk_user_id VARCHAR(255) NOT NULL,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    ad_campaign_id INT,
    FOREIGN KEY (ad_campaign_id) REFERENCES ad_campaigns(id) ON DELETE CASCADE,
    INDEX idx_clerk_user_id (clerk_user_id)
);

-- Create keywords table: Individual keywords for matching in ads or searches
-- Made unique per user to avoid duplicates
CREATE TABLE keywords (
    id INT AUTO_INCREMENT PRIMARY KEY,
    keyword VARCHAR(255) NOT NULL,
    clerk_user_id VARCHAR(255) NOT NULL,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_keyword_per_user (keyword, clerk_user_id),
    INDEX idx_clerk_user_id (clerk_user_id)
);

-- Create ad_group_keyword junction table: Associates keywords with ad groups, including match type specifications
CREATE TABLE ad_group_keyword (
    ad_group_id INT,
    keyword_id INT,
    broad BOOLEAN DEFAULT FALSE,
    phrase BOOLEAN DEFAULT FALSE,
    exact BOOLEAN DEFAULT FALSE,
    neg_broad BOOLEAN DEFAULT FALSE,
    neg_phrase BOOLEAN DEFAULT FALSE,
    neg_exact BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (ad_group_id, keyword_id),
    FOREIGN KEY (ad_group_id) REFERENCES ad_groups(id) ON DELETE CASCADE,
    FOREIGN KEY (keyword_id) REFERENCES keywords(id) ON DELETE CASCADE
);

-- Create company_keyword junction table: Associates keywords with companies, including match types
CREATE TABLE company_keyword (
    company_id INT,
    keyword_id INT,
    broad BOOLEAN DEFAULT FALSE,
    phrase BOOLEAN DEFAULT FALSE,
    exact BOOLEAN DEFAULT FALSE,
    neg_broad BOOLEAN DEFAULT FALSE,
    neg_phrase BOOLEAN DEFAULT FALSE,
    neg_exact BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (company_id, keyword_id),
    FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE,
    FOREIGN KEY (keyword_id) REFERENCES keywords(id) ON DELETE CASCADE
);

-- Create ad_campaign_keyword junction table: Associates keywords with campaigns, including match types
CREATE TABLE ad_campaign_keyword (
    ad_campaign_id INT,
    keyword_id INT,
    broad BOOLEAN DEFAULT FALSE,
    phrase BOOLEAN DEFAULT FALSE,
    exact BOOLEAN DEFAULT FALSE,
    neg_broad BOOLEAN DEFAULT FALSE,
    neg_phrase BOOLEAN DEFAULT FALSE,
    neg_exact BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (ad_campaign_id, keyword_id),
    FOREIGN KEY (ad_campaign_id) REFERENCES ad_campaigns(id) ON DELETE CASCADE,
    FOREIGN KEY (keyword_id) REFERENCES keywords(id) ON DELETE CASCADE
);

-- Create filters table: Stores individual words used for virtual filtering/matching of keywords
-- Made unique per user to avoid duplicates
CREATE TABLE filters (
    id INT AUTO_INCREMENT PRIMARY KEY,
    word VARCHAR(255) NOT NULL,
    clerk_user_id VARCHAR(255) NOT NULL,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_word_per_user (word, clerk_user_id),
    INDEX idx_clerk_user_id (clerk_user_id)
);

-- Create company_filter junction table: Enables many-to-many association between companies and filter words
CREATE TABLE company_filter(
    company_id INT,
    filter_id INT,
    is_negative BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (company_id, filter_id),
    FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE,
    FOREIGN KEY (filter_id) REFERENCES filters(id) ON DELETE CASCADE
);

-- Create ad_campaign_filter junction table: Enables many-to-many association between campaigns and filter words
CREATE TABLE ad_campaign_filter(
    ad_campaign_id INT,
    filter_id INT,
    is_negative BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (ad_campaign_id, filter_id),
    FOREIGN KEY (ad_campaign_id) REFERENCES ad_campaigns(id) ON DELETE CASCADE,
    FOREIGN KEY (filter_id) REFERENCES filters(id) ON DELETE CASCADE
);

-- Create ad_group_filter junction table: Enables many-to-many association between ad groups and filter words for virtual grouping
CREATE TABLE ad_group_filter(
    ad_group_id INT,
    filter_id INT,
    is_negative BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (ad_group_id, filter_id),
    FOREIGN KEY (ad_group_id) REFERENCES ad_groups(id) ON DELETE CASCADE,
    FOREIGN KEY (filter_id) REFERENCES filters(id) ON DELETE CASCADE
);
