-- UK Job Market Intelligence Platform - Database Schema (SQLite Version)
-- SQLite Schema for storing job market data from Adzuna, Reed, and ONS APIs

-- Drop tables if they exist (for clean re-creation)
DROP TABLE IF EXISTS skills_extracted;
DROP TABLE IF EXISTS job_postings;
DROP TABLE IF EXISTS ons_vacancies;
DROP TABLE IF EXISTS ons_salaries;

-- ============================================================================
-- Table: job_postings
-- Description: Stores job posting data collected from Adzuna and Reed APIs
-- ============================================================================
CREATE TABLE job_postings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source VARCHAR(50) NOT NULL,                    -- API source: 'adzuna' or 'reed'
    title VARCHAR(255) NOT NULL,                    -- Job title
    company VARCHAR(255),                           -- Company name
    region VARCHAR(100),                            -- ONS region (standardized)
    city VARCHAR(100),                              -- City name
    salary_min DECIMAL(10, 2),                      -- Minimum salary (£)
    salary_max DECIMAL(10, 2),                      -- Maximum salary (£)
    salary_avg DECIMAL(10, 2),                      -- Average salary (calculated)
    contract_type VARCHAR(50),                      -- Contract type: permanent, contract, temporary
    posting_date DATE,                              -- Date job was posted
    collected_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Date data was collected
    description TEXT,                               -- Full job description
    role_category VARCHAR(100),                     -- Classified role: Data Analyst, Business Analyst, etc.
    
    -- Constraints
    CHECK (salary_min >= 0 OR salary_min IS NULL),
    CHECK (salary_max >= 0 OR salary_max IS NULL),
    CHECK (salary_avg >= 0 OR salary_avg IS NULL),
    CHECK (salary_max >= salary_min OR salary_max IS NULL OR salary_min IS NULL)
);

-- ============================================================================
-- Table: skills_extracted
-- Description: Stores skills extracted from job descriptions
-- ============================================================================
CREATE TABLE skills_extracted (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL,                        -- Foreign key to job_postings
    skill_name VARCHAR(100) NOT NULL,               -- Skill name (e.g., 'SQL', 'Python')
    skill_category VARCHAR(50),                     -- Category: 'technical', 'soft', 'tool'
    
    -- Foreign key constraint
    FOREIGN KEY (job_id) REFERENCES job_postings(id) ON DELETE CASCADE,
    
    -- Prevent duplicate skills for the same job
    UNIQUE (job_id, skill_name)
);

-- ============================================================================
-- Table: ons_vacancies
-- Description: Stores ONS vacancy data by industry and region
-- ============================================================================
CREATE TABLE ons_vacancies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    period DATE NOT NULL,                           -- Time period (month/quarter)
    region VARCHAR(100),                            -- ONS region
    industry VARCHAR(255),                          -- Industry sector
    vacancy_count INTEGER,                          -- Number of vacancies
    yoy_change_percent DECIMAL(5, 2),               -- Year-over-year change percentage
    
    -- Prevent duplicate entries for same period/region/industry
    UNIQUE (period, region, industry)
);

-- ============================================================================
-- Table: ons_salaries
-- Description: Stores ONS salary data by region and occupation
-- ============================================================================
CREATE TABLE ons_salaries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    period DATE NOT NULL,                           -- Time period (month/quarter)
    region VARCHAR(100),                            -- ONS region
    occupation VARCHAR(255),                        -- Occupation category
    avg_weekly_earnings DECIMAL(10, 2),             -- Average weekly earnings (£)
    avg_annual_salary DECIMAL(10, 2),               -- Average annual salary (£)
    
    -- Prevent duplicate entries for same period/region/occupation
    UNIQUE (period, region, occupation)
);

-- ============================================================================
-- Indexes for Performance Optimization
-- ============================================================================

-- Indexes on job_postings table (frequently queried columns)
CREATE INDEX idx_job_postings_posting_date ON job_postings(posting_date);
CREATE INDEX idx_job_postings_region ON job_postings(region);
CREATE INDEX idx_job_postings_role_category ON job_postings(role_category);
CREATE INDEX idx_job_postings_source ON job_postings(source);
CREATE INDEX idx_job_postings_collected_date ON job_postings(collected_date);

-- Composite index for common query patterns
CREATE INDEX idx_job_postings_region_role ON job_postings(region, role_category);
CREATE INDEX idx_job_postings_date_region ON job_postings(posting_date, region);

-- Indexes on skills_extracted table
CREATE INDEX idx_skills_job_id ON skills_extracted(job_id);
CREATE INDEX idx_skills_skill_name ON skills_extracted(skill_name);
CREATE INDEX idx_skills_category ON skills_extracted(skill_category);

-- Indexes on ons_vacancies table
CREATE INDEX idx_ons_vacancies_period ON ons_vacancies(period);
CREATE INDEX idx_ons_vacancies_region ON ons_vacancies(region);
CREATE INDEX idx_ons_vacancies_industry ON ons_vacancies(industry);

-- Indexes on ons_salaries table
CREATE INDEX idx_ons_salaries_period ON ons_salaries(period);
CREATE INDEX idx_ons_salaries_region ON ons_salaries(region);
CREATE INDEX idx_ons_salaries_occupation ON ons_salaries(occupation);