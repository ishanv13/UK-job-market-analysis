"""
Data Cleaning and Processing Module for UK Job Market Intelligence Platform

This module handles data validation, cleaning, and standardization of job postings
collected from Adzuna and Reed APIs. It prepares data for database storage and analysis.
"""

import logging
import pandas as pd
import numpy as np
import re
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# Region Mapping Dictionary
# Maps UK cities to ONS NUTS1 regions for standardization
# ============================================================================
REGION_MAP = {
    # London
    'london': 'London',
    'city of london': 'London',
    'westminster': 'London',
    'camden': 'London',
    'islington': 'London',
    'hackney': 'London',
    'tower hamlets': 'London',
    'greenwich': 'London',
    'lewisham': 'London',
    'southwark': 'London',
    'lambeth': 'London',
    'wandsworth': 'London',
    'hammersmith': 'London',
    'kensington': 'London',
    'chelsea': 'London',
    'richmond': 'London',
    'kingston': 'London',
    'croydon': 'London',
    'bromley': 'London',
    'bexley': 'London',
    'havering': 'London',
    'barking': 'London',
    'redbridge': 'London',
    'newham': 'London',
    'waltham forest': 'London',
    'haringey': 'London',
    'enfield': 'London',
    'barnet': 'London',
    'harrow': 'London',
    'brent': 'London',
    'ealing': 'London',
    'hounslow': 'London',
    'hillingdon': 'London',
    
    # South East
    'brighton': 'South East',
    'hove': 'South East',
    'reading': 'South East',
    'slough': 'South East',
    'oxford': 'South East',
    'milton keynes': 'South East',
    'southampton': 'South East',
    'portsmouth': 'South East',
    'basingstoke': 'South East',
    'guildford': 'South East',
    'crawley': 'South East',
    'worthing': 'South East',
    'eastbourne': 'South East',
    'canterbury': 'South East',
    'maidstone': 'South East',
    'ashford': 'South East',
    'woking': 'South East',
    'watford': 'South East',
    'luton': 'South East',
    'high wycombe': 'South East',
    'aylesbury': 'South East',
    'bracknell': 'South East',
    'maidenhead': 'South East',
    'winchester': 'South East',
    'chichester': 'South East',
    
    # South West
    'bristol': 'South West',
    'plymouth': 'South West',
    'bournemouth': 'South West',
    'poole': 'South West',
    'swindon': 'South West',
    'exeter': 'South West',
    'cheltenham': 'South West',
    'gloucester': 'South West',
    'bath': 'South West',
    'torbay': 'South West',
    'torquay': 'South West',
    'taunton': 'South West',
    'yeovil': 'South West',
    'truro': 'South West',
    'penzance': 'South West',
    
    # West Midlands
    'birmingham': 'West Midlands',
    'coventry': 'West Midlands',
    'wolverhampton': 'West Midlands',
    'solihull': 'West Midlands',
    'dudley': 'West Midlands',
    'walsall': 'West Midlands',
    'stoke': 'West Midlands',
    'stoke-on-trent': 'West Midlands',
    'telford': 'West Midlands',
    'worcester': 'West Midlands',
    'hereford': 'West Midlands',
    'shrewsbury': 'West Midlands',
    'stafford': 'West Midlands',
    'cannock': 'West Midlands',
    'tamworth': 'West Midlands',
    'nuneaton': 'West Midlands',
    'rugby': 'West Midlands',
    'redditch': 'West Midlands',
    
    # East Midlands
    'nottingham': 'East Midlands',
    'leicester': 'East Midlands',
    'derby': 'East Midlands',
    'northampton': 'East Midlands',
    'lincoln': 'East Midlands',
    'mansfield': 'East Midlands',
    'chesterfield': 'East Midlands',
    'kettering': 'East Midlands',
    'corby': 'East Midlands',
    'wellingborough': 'East Midlands',
    'loughborough': 'East Midlands',
    'grantham': 'East Midlands',
    'boston': 'East Midlands',
    
    # East of England
    'cambridge': 'East of England',
    'peterborough': 'East of England',
    'ipswich': 'East of England',
    'norwich': 'East of England',
    'colchester': 'East of England',
    'chelmsford': 'East of England',
    'southend': 'East of England',
    'southend-on-sea': 'East of England',
    'basildon': 'East of England',
    'harlow': 'East of England',
    'stevenage': 'East of England',
    'hemel hempstead': 'East of England',
    'st albans': 'East of England',
    'hatfield': 'East of England',
    'bedford': 'East of England',
    'kings lynn': 'East of England',
    'great yarmouth': 'East of England',
    'lowestoft': 'East of England',
    'bury st edmunds': 'East of England',
    
    # North West
    'manchester': 'North West',
    'liverpool': 'North West',
    'preston': 'North West',
    'blackpool': 'North West',
    'bolton': 'North West',
    'warrington': 'North West',
    'stockport': 'North West',
    'oldham': 'North West',
    'rochdale': 'North West',
    'salford': 'North West',
    'wigan': 'North West',
    'blackburn': 'North West',
    'burnley': 'North West',
    'chester': 'North West',
    'crewe': 'North West',
    'macclesfield': 'North West',
    'carlisle': 'North West',
    'barrow': 'North West',
    'lancaster': 'North West',
    'kendal': 'North West',
    
    # North East
    'newcastle': 'North East',
    'newcastle upon tyne': 'North East',
    'sunderland': 'North East',
    'middlesbrough': 'North East',
    'gateshead': 'North East',
    'durham': 'North East',
    'darlington': 'North East',
    'hartlepool': 'North East',
    'stockton': 'North East',
    'stockton-on-tees': 'North East',
    'south shields': 'North East',
    'north shields': 'North East',
    'tynemouth': 'North East',
    'washington': 'North East',
    
    # Yorkshire and The Humber
    'leeds': 'Yorkshire and The Humber',
    'sheffield': 'Yorkshire and The Humber',
    'bradford': 'Yorkshire and The Humber',
    'hull': 'Yorkshire and The Humber',
    'kingston upon hull': 'Yorkshire and The Humber',
    'york': 'Yorkshire and The Humber',
    'huddersfield': 'Yorkshire and The Humber',
    'doncaster': 'Yorkshire and The Humber',
    'wakefield': 'Yorkshire and The Humber',
    'rotherham': 'Yorkshire and The Humber',
    'barnsley': 'Yorkshire and The Humber',
    'halifax': 'Yorkshire and The Humber',
    'harrogate': 'Yorkshire and The Humber',
    'scarborough': 'Yorkshire and The Humber',
    'grimsby': 'Yorkshire and The Humber',
    'scunthorpe': 'Yorkshire and The Humber',
    
    # Wales
    'cardiff': 'Wales',
    'swansea': 'Wales',
    'newport': 'Wales',
    'wrexham': 'Wales',
    'barry': 'Wales',
    'merthyr tydfil': 'Wales',
    'neath': 'Wales',
    'port talbot': 'Wales',
    'cwmbran': 'Wales',
    'bridgend': 'Wales',
    'llanelli': 'Wales',
    'caerphilly': 'Wales',
    'pontypridd': 'Wales',
    'rhondda': 'Wales',
    'aberystwyth': 'Wales',
    'bangor': 'Wales',
    
    # Scotland
    'edinburgh': 'Scotland',
    'glasgow': 'Scotland',
    'aberdeen': 'Scotland',
    'dundee': 'Scotland',
    'inverness': 'Scotland',
    'stirling': 'Scotland',
    'perth': 'Scotland',
    'paisley': 'Scotland',
    'east kilbride': 'Scotland',
    'livingston': 'Scotland',
    'hamilton': 'Scotland',
    'kirkcaldy': 'Scotland',
    'dunfermline': 'Scotland',
    'ayr': 'Scotland',
    'kilmarnock': 'Scotland',
    'greenock': 'Scotland',
    'motherwell': 'Scotland',
    'falkirk': 'Scotland',
    
    # Northern Ireland
    'belfast': 'Northern Ireland',
    'derry': 'Northern Ireland',
    'londonderry': 'Northern Ireland',
    'lisburn': 'Northern Ireland',
    'newry': 'Northern Ireland',
    'armagh': 'Northern Ireland',
    'bangor': 'Northern Ireland',
    'craigavon': 'Northern Ireland',
    'ballymena': 'Northern Ireland',
    'newtownabbey': 'Northern Ireland',
    'carrickfergus': 'Northern Ireland',
    'coleraine': 'Northern Ireland',
}


def extract_city_from_location(location: str) -> str:
    """
    Extract city name from location string.
    
    This function cleans location strings and extracts the city name,
    removing common suffixes and taking the first part before commas.
    
    Args:
        location: Location string from job posting (e.g., "London", "Manchester, UK")
    
    Returns:
        Extracted city name or "Unknown" if not found
        
    Examples:
        >>> extract_city_from_location("London")
        'London'
        >>> extract_city_from_location("Manchester, UK")
        'Manchester'
        >>> extract_city_from_location("Unknown City")
        'Unknown City'
    """
    if not location or pd.isna(location) or location.strip() == '':
        return 'Unknown'
    
    # Clean the location string
    location_clean = location.strip().lower()
    
    # Remove common suffixes like ", UK", ", England", etc.
    location_clean = location_clean.replace(', uk', '').replace(', england', '')
    location_clean = location_clean.replace(', scotland', '').replace(', wales', '')
    location_clean = location_clean.replace(', northern ireland', '')
    location_clean = location_clean.strip()
    
    # Extract city name (take first part before comma if present)
    city_parts = location_clean.split(',')
    city_name = city_parts[0].strip()
    
    # Return the original case for city name (capitalize each word)
    city_display = city_name.title()
    
    return city_display


def clean_jobs(
    df: pd.DataFrame,
    min_salary: float = 10000.0,
    max_salary: float = 200000.0
) -> pd.DataFrame:
    """
    Clean and standardize job posting data.
    
    This function performs the following operations:
    1. Removes duplicate rows (ignoring the id column)
    2. Calculates salary_avg from salary_min and salary_max
    3. Removes salary outliers (< £10k or > £200k by default)
    4. Handles missing values in critical fields
    
    Args:
        df: DataFrame with raw job posting data
        min_salary: Minimum acceptable salary threshold (default: £10,000)
        max_salary: Maximum acceptable salary threshold (default: £200,000)
    
    Returns:
        Cleaned DataFrame with standardized salary fields and outliers removed
        
    Raises:
        ValueError: If required columns are missing from the DataFrame
    """
    logger.info(f"Starting data cleaning. Input rows: {len(df)}")
    
    # Validate required columns
    required_columns = ['title', 'salary_min', 'salary_max']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Create a copy to avoid modifying the original DataFrame
    df_clean = df.copy()
    
    # Remove duplicate rows while ignoring the id column
    initial_count = len(df_clean)
    
    # Get all columns except 'id' for duplicate detection
    columns_for_dedup = [col for col in df_clean.columns if col != 'id']
    
    if columns_for_dedup:
        # Keep first occurrence of duplicates based on all columns except id
        df_clean = df_clean.drop_duplicates(subset=columns_for_dedup, keep='first')
        duplicates_removed = initial_count - len(df_clean)
        
        if duplicates_removed > 0:
            logger.info(f"Removed {duplicates_removed} duplicate rows (ignoring id column)")
    
    # Reset index after removing duplicates
    df_clean = df_clean.reset_index(drop=True)
    
    # Convert salary columns to numeric, handling any non-numeric values
    df_clean['salary_min'] = pd.to_numeric(df_clean['salary_min'], errors='coerce')
    df_clean['salary_max'] = pd.to_numeric(df_clean['salary_max'], errors='coerce')
    
    # Calculate salary_avg as the mean of salary_min and salary_max
    # Only calculate when both values are present
    df_clean['salary_avg'] = np.where(
        df_clean['salary_min'].notna() & df_clean['salary_max'].notna(),
        (df_clean['salary_min'] + df_clean['salary_max']) / 2,
        np.nan
    )
    
    # If only one salary value is present, use it as the average
    df_clean['salary_avg'] = df_clean['salary_avg'].fillna(df_clean['salary_min'])
    df_clean['salary_avg'] = df_clean['salary_avg'].fillna(df_clean['salary_max'])
    
    logger.info(f"Calculated salary_avg for {df_clean['salary_avg'].notna().sum()} rows")
    
    # Remove salary outliers
    # Keep rows where salary_avg is within the acceptable range OR is missing
    initial_count = len(df_clean)
    
    df_clean = df_clean[
        (df_clean['salary_avg'].isna()) |
        ((df_clean['salary_avg'] >= min_salary) & (df_clean['salary_avg'] <= max_salary))
    ]
    
    outliers_removed = initial_count - len(df_clean)
    if outliers_removed > 0:
        logger.info(f"Removed {outliers_removed} salary outliers (< £{min_salary:,.0f} or > £{max_salary:,.0f})")
    
    # Handle missing values in critical fields
    # For title: remove rows with missing titles (critical field)
    missing_titles = df_clean['title'].isna().sum()
    if missing_titles > 0:
        df_clean = df_clean[df_clean['title'].notna()]
        logger.info(f"Removed {missing_titles} rows with missing job titles")
    
    # For company: fill missing values with 'Unknown'
    missing_companies = df_clean['company'].isna().sum()
    if missing_companies > 0:
        df_clean['company'] = df_clean['company'].fillna('Unknown')
        logger.info(f"Filled {missing_companies} missing company names with 'Unknown'")
    
    # For location: fill missing values with 'Unknown'
    missing_locations = df_clean['location'].isna().sum()
    if missing_locations > 0:
        df_clean['location'] = df_clean['location'].fillna('Unknown')
        logger.info(f"Filled {missing_locations} missing locations with 'Unknown'")
    
    # For contract_type: fill missing values with 'Not Specified'
    if 'contract_type' in df_clean.columns:
        missing_contract_types = df_clean['contract_type'].isna().sum()
        if missing_contract_types > 0:
            df_clean['contract_type'] = df_clean['contract_type'].fillna('Not Specified')
            logger.info(f"Filled {missing_contract_types} missing contract types with 'Not Specified'")
    
    # For description: fill missing values with empty string
    if 'description' in df_clean.columns:
        missing_descriptions = df_clean['description'].isna().sum()
        if missing_descriptions > 0:
            df_clean['description'] = df_clean['description'].fillna('')
            logger.info(f"Filled {missing_descriptions} missing descriptions with empty string")
    
    # For posting_date: convert to datetime and handle missing values
    if 'posting_date' in df_clean.columns:
        df_clean['posting_date'] = pd.to_datetime(df_clean['posting_date'], errors='coerce')
        missing_dates = df_clean['posting_date'].isna().sum()
        if missing_dates > 0:
            logger.warning(f"{missing_dates} rows have missing or invalid posting dates")
    
    # Reset index after filtering
    df_clean = df_clean.reset_index(drop=True)
    
    logger.info(f"Data cleaning complete. Output rows: {len(df_clean)}")
    logger.info(f"Rows with salary data: {df_clean['salary_avg'].notna().sum()}")
    
    return df_clean


def classify_role(title: str) -> str:
    """
    Classify job title into standardized role categories.
    
    This function uses keyword matching to classify job titles into one of five categories:
    - Data Analyst: Roles focused on data analysis, insights, and reporting
    - Business Analyst: Roles focused on business processes and requirements
    - BI Analyst: Roles focused on business intelligence and visualization
    - Data Engineer: Roles focused on data infrastructure and pipelines
    - Other Analyst: Any other analyst roles that don't fit the above categories
    
    The classification uses a priority-based approach:
    1. Check for Data Engineer keywords (highest priority for technical roles)
    2. Check for BI Analyst keywords (specific BI/visualization focus)
    3. Check for Business Analyst keywords (business process focus)
    4. Check for Data Analyst keywords (general data analysis)
    5. Default to Other Analyst for any remaining analyst roles
    
    Args:
        title: Job title string to classify
    
    Returns:
        Role category as a string (one of the five categories above)
        
    Examples:
        >>> classify_role("Senior Data Analyst")
        'Data Analyst'
        >>> classify_role("Business Intelligence Analyst")
        'BI Analyst'
        >>> classify_role("Data Engineer")
        'Data Engineer'
        >>> classify_role("Junior Business Analyst")
        'Business Analyst'
        >>> classify_role("Financial Analyst")
        'Other Analyst'
    """
    if not title or not isinstance(title, str):
        return 'Other Analyst'
    
    # Convert to lowercase for case-insensitive matching
    title_lower = title.lower()
    
    # Priority 1: Data Engineer (check first as it's most specific)
    # Keywords: engineer, engineering, ETL, pipeline, infrastructure
    engineer_keywords = [
        'data engineer',
        'etl developer',
        'big data engineer',
        'platform engineer',
        'data pipeline'
    ]
    
    for keyword in engineer_keywords:
        if keyword in title_lower:
            return 'Data Engineer'
    
    # Priority 2: BI Analyst (check before general business analyst)
    # Keywords: business intelligence, BI, power bi, tableau, visualization
    bi_keywords = [
        'business intelligence',
        'bi analyst',
        'bi developer',
        'bi specialist',
        'tableau',
        'power bi',
        'qlik',
        'dashboard',
        'reporting analyst',  # Specific reporting analyst
        'intelligence analyst',  # Intelligence analyst
        'etl',
        'data warehouse'
    ]
    
    for keyword in bi_keywords:
        if keyword in title_lower:
            return 'BI Analyst'
    
    # Priority 3: Business Analyst (but exclude BI analyst which was already checked)
    # Keywords: business analyst, BA, business systems, requirements
    ba_keywords = [
        'business analyst',
        'business analysis',
        'ba ',
        ' ba',
        'process analyst',
        'systems analyst',
        'functional analyst',
        'requirements analyst',
        'business systems'
    ]
    
    for keyword in ba_keywords:
        if keyword in title_lower:
            return 'Business Analyst'
    
    # Priority 4: Data Analyst (general data analysis roles)
    # Keywords: data analyst, analytics, insights, reporting
    data_keywords = [
        'data analyst',
        'data analysis',
        'analytics',
        'data science',
        'quantitative analyst',
        'research analyst',  # Research analyst
        'insight analyst',
        'data specialist',
        'data officer'
    ]
    
    for keyword in data_keywords:
        if keyword in title_lower:
            return 'Data Analyst'
    
    # Priority 5: Default to Other Analyst for any remaining analyst roles
    # This catches financial analysts, marketing analysts, etc.
    return 'Other Analyst'


# Skill dictionary with regex patterns for extraction
# Updated to match task 5.4 requirements with specified skills and categories
SKILL_DICTIONARY = {
    # Technical skills
    'SQL': {
        'patterns': [
            r'\bSQL\b',
            r'\bT-SQL\b',
            r'\bPL/SQL\b',
            r'\bMySQL\b',
            r'\bPostgreSQL\b',
            r'\bMS SQL\b',
            r'\bSQL Server\b'
        ],
        'category': 'technical'
    },
    'Python': {
        'patterns': [r'\bPython\b'],
        'category': 'technical'
    },
    'R': {
        'patterns': [r'\b R\b', r'\bR programming\b', r'\bR language\b'],
        'category': 'technical'
    },
    'Machine Learning': {
        'patterns': [
            r'\bMachine Learning\b',
            r'\bML\b',
            r'\bDeep Learning\b',
            r'\bAI\b',
            r'\bArtificial Intelligence\b'
        ],
        'category': 'technical'
    },
    'Statistics': {
        'patterns': [
            r'\bStatistics\b',
            r'\bStatistical\b',
            r'\bStatistical Analysis\b',
            r'\bStatistical Modeling\b'
        ],
        'category': 'technical'
    },
    
    # Tool skills
    'Excel': {
        'patterns': [
            r'\bExcel\b',
            r'\bMS Excel\b',
            r'\bMicrosoft Excel\b',
            r'\bSpreadsheet\b'
        ],
        'category': 'tool'
    },
    'Power BI': {
        'patterns': [
            r'\bPower BI\b',
            r'\bPowerBI\b',
            r'\bPower-BI\b'
        ],
        'category': 'tool'
    },
    'Tableau': {
        'patterns': [r'\bTableau\b'],
        'category': 'tool'
    },
    
    # Soft skills
    'Communication': {
        'patterns': [
            r'\bCommunication\b',
            r'\bCommunicate\b',
            r'\bCommunicating\b',
            r'\bVerbal Communication\b',
            r'\bWritten Communication\b'
        ],
        'category': 'soft'
    },
    'Stakeholder': {
        'patterns': [
            r'\bStakeholder\b',
            r'\bStakeholders\b',
            r'\bStakeholder Management\b',
            r'\bStakeholder Engagement\b'
        ],
        'category': 'soft'
    }
}


def extract_skills(
    description: str,
    skill_dict: Optional[Dict[str, Dict[str, Any]]] = None
) -> List[Tuple[str, str]]:
    """
    Extract skills from job description using regex patterns.
    
    This function searches for predefined skills in job descriptions using
    case-insensitive regex matching. Each skill can have multiple patterns
    to capture different variations (e.g., 'SQL', 'T-SQL', 'MySQL').
    
    Args:
        description: Job description text to extract skills from
        skill_dict: Optional custom skill dictionary (defaults to SKILL_DICTIONARY)
                   Format: {skill_name: {'patterns': [regex_patterns], 'category': category}}
    
    Returns:
        List of tuples (skill_name, skill_category) for all matched skills
        Example: [('SQL', 'technical'), ('Python', 'technical'), ('Communication', 'soft')]
    
    Example:
        >>> description = "Looking for a Data Analyst with SQL and Python experience"
        >>> extract_skills(description)
        [('SQL', 'technical'), ('Python', 'technical')]
    """
    if not description or not isinstance(description, str):
        return []
    
    # Use default skill dictionary if none provided
    if skill_dict is None:
        skill_dict = SKILL_DICTIONARY
    
    extracted_skills = []
    
    # Iterate through each skill in the dictionary
    for skill_name, skill_info in skill_dict.items():
        patterns = skill_info.get('patterns', [])
        category = skill_info.get('category', 'unknown')
        
        # Check if any pattern matches the description
        skill_found = False
        for pattern in patterns:
            # Use case-insensitive matching
            if re.search(pattern, description, re.IGNORECASE):
                skill_found = True
                break
        
        # Add skill if found (avoid duplicates)
        if skill_found:
            extracted_skills.append((skill_name, category))
    
    return extracted_skills


def extract_skills_from_dataframe(
    df: pd.DataFrame,
    description_column: str = 'description'
) -> pd.DataFrame:
    """
    Extract skills from all job descriptions in a DataFrame.
    
    This function applies skill extraction to each row in the DataFrame
    and returns a new DataFrame with one row per skill per job.
    
    Args:
        df: DataFrame with job postings (must have 'id' and description column)
        description_column: Name of the column containing job descriptions (default: 'description')
    
    Returns:
        DataFrame with columns: job_id, skill_name, skill_category
        Each row represents one skill extracted from one job posting
        
    Raises:
        ValueError: If required columns are missing from the DataFrame
    """
    # Validate required columns
    if 'id' not in df.columns:
        raise ValueError("DataFrame must have 'id' column")
    
    if description_column not in df.columns:
        raise ValueError(f"DataFrame must have '{description_column}' column")
    
    logger.info(f"Extracting skills from {len(df)} job descriptions")
    
    # Extract skills for each job
    skills_data = []
    
    for idx, row in df.iterrows():
        job_id = row['id']
        description = row[description_column]
        
        # Extract skills from description
        skills = extract_skills(description)
        
        # Add each skill as a separate row
        for skill_name, skill_category in skills:
            skills_data.append({
                'job_id': job_id,
                'skill_name': skill_name,
                'skill_category': skill_category
            })
    
    # Create DataFrame from extracted skills with proper columns even if empty
    skills_df = pd.DataFrame(skills_data, columns=['job_id', 'skill_name', 'skill_category'])
    
    logger.info(f"Extracted {len(skills_df)} total skill mentions from {len(df)} jobs")
    
    if len(skills_df) > 0:
        unique_jobs_with_skills = skills_df['job_id'].nunique()
        logger.info(f"{unique_jobs_with_skills} jobs have at least one skill extracted")
        
        # Log skill statistics
        skill_counts = skills_df['skill_name'].value_counts()
        logger.info(f"Top 10 skills found: {skill_counts.head(10).to_dict()}")
        
        category_counts = skills_df['skill_category'].value_counts()
        logger.info(f"Skills by category: {category_counts.to_dict()}")
    
    return skills_df


def insert_skills_to_database(
    skills_df: pd.DataFrame,
    db_manager
) -> int:
    """
    Insert extracted skills into the skills_extracted database table.
    
    Args:
        skills_df: DataFrame with columns: job_id, skill_name, skill_category
        db_manager: DatabaseManager instance for database operations
    
    Returns:
        Number of skills inserted into the database
        
    Raises:
        ValueError: If skills_df is empty or missing required columns
    """
    if skills_df.empty:
        logger.warning("No skills to insert - DataFrame is empty")
        return 0
    
    # Validate required columns
    required_columns = ['job_id', 'skill_name', 'skill_category']
    missing_columns = [col for col in required_columns if col not in skills_df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    logger.info(f"Inserting {len(skills_df)} skills into database")
    
    # Convert DataFrame to list of dictionaries for bulk insert
    records = skills_df.to_dict('records')
    
    # Use bulk insert from database manager
    inserted_count = db_manager.bulk_insert(
        table_name='skills_extracted',
        records=records,
        batch_size=1000
    )
    
    logger.info(f"Successfully inserted {inserted_count} skills into skills_extracted table")
    
    return inserted_count


# Import region mapping functionality
try:
    from .region_mapping import standardize_region
except ImportError:
    # Fallback for direct execution
    import sys
    import os
    sys.path.insert(0, os.path.dirname(__file__))
    from region_mapping import standardize_region


def standardize_regions(df: pd.DataFrame, location_column: str = 'location') -> pd.DataFrame:
    """
    Standardize location data to ONS NUTS1 regions.
    
    This function applies region standardization to a DataFrame, mapping UK cities
    to their corresponding ONS NUTS1 regions. It also extracts clean city names
    and adds both region and city columns to the DataFrame.
    
    Args:
        df: DataFrame with location data
        location_column: Name of the column containing location data (default: 'location')
    
    Returns:
        DataFrame with new 'region' and 'city' columns containing standardized regions and cities
        
    Examples:
        >>> df = pd.DataFrame({'location': ['London', 'Manchester, UK', 'Unknown']})
        >>> result = standardize_regions(df)
        >>> result[['location', 'region', 'city']]
        location        region      city
        London          London      London
        Manchester, UK  North West  Manchester
        Unknown         Other       Unknown
    """
    logger.info(f"Standardizing regions from {location_column} column")
    
    if location_column not in df.columns:
        raise ValueError(f"Column '{location_column}' not found in DataFrame")
    
    df_result = df.copy()
    
    # Apply region standardization using the region_mapping module
    df_result['region'] = df_result[location_column].apply(standardize_region)
    
    # Extract clean city names
    df_result['city'] = df_result[location_column].apply(extract_city_from_location)
    
    # Log region distribution
    region_counts = df_result['region'].value_counts()
    logger.info(f"Region distribution: {region_counts.to_dict()}")
    
    # Log city distribution (top 10)
    city_counts = df_result['city'].value_counts()
    logger.info(f"Top 10 cities: {city_counts.head(10).to_dict()}")
    
    return df_result


# Role Classification Functions
ROLE_KEYWORDS = {
    'Data Engineer': [
        'data engineer', 'data engineering', 'etl developer',
        'data pipeline', 'big data', 'spark', 'hadoop',
        'kafka', 'airflow', 'data platform', 'data infrastructure'
    ],
    'BI Analyst': [
        'bi analyst', 'business intelligence', 'bi developer',
        'bi specialist', 'tableau', 'power bi', 'qlik',
        'dashboard', 'reporting', 'etl', 'data warehouse'
    ],
    'Business Analyst': [
        'business analyst', 'business analysis', 'ba ', ' ba',
        'process analyst', 'systems analyst', 'functional analyst',
        'requirements analyst', 'business systems'
    ],
    'Data Analyst': [
        'data analyst', 'data analysis', 'analytics', 'data science',
        'quantitative analyst', 'research analyst', 'insight analyst',
        'reporting analyst', 'data specialist', 'data officer'
    ]
}


def classify_role_legacy(job_title: str) -> str:
    """
    Legacy classify role function - kept for compatibility.
    Use the main classify_role function instead.
    """
    if not job_title or not isinstance(job_title, str):
        return 'Other Analyst'
    
    title_lower = job_title.lower().strip()
    
    # Check each role category
    for role, keywords in ROLE_KEYWORDS.items():
        for keyword in keywords:
            if keyword in title_lower:
                return role
    
    # Check if it's some other type of analyst
    if 'analyst' in title_lower or 'analysis' in title_lower:
        return 'Other Analyst'
    
    return 'Other Analyst'


def classify_roles(df: pd.DataFrame, title_column: str = 'title') -> pd.DataFrame:
    """
    Classify job roles for all jobs in DataFrame.
    
    Args:
        df: DataFrame with job titles
        title_column: Name of the column containing job titles
    
    Returns:
        DataFrame with new 'role_category' column
    """
    logger.info(f"Classifying roles from {title_column} column")
    
    df_result = df.copy()
    
    # Apply role classification
    df_result['role_category'] = df_result[title_column].apply(classify_role)
    
    # Log role distribution
    role_counts = df_result['role_category'].value_counts()
    logger.info(f"Role distribution: {role_counts.to_dict()}")
    
    return df_result


# Enhanced clean_jobs function
def clean_jobs_enhanced(
    df: pd.DataFrame,
    min_salary: float = 10000.0,
    max_salary: float = 200000.0,
    include_regions: bool = True,
    include_roles: bool = True
) -> pd.DataFrame:
    """
    Enhanced version of clean_jobs with region and role processing.
    
    Args:
        df: DataFrame with raw job posting data
        min_salary: Minimum acceptable salary threshold
        max_salary: Maximum acceptable salary threshold
        include_regions: Whether to standardize regions
        include_roles: Whether to classify roles
    
    Returns:
        Cleaned DataFrame with regions and roles processed
    """
    # First apply basic cleaning
    df_clean = clean_jobs(df, min_salary, max_salary)
    
    # Add region standardization
    if include_regions and 'location' in df_clean.columns:
        df_clean = standardize_regions(df_clean)
    
    # Add role classification
    if include_roles and 'title' in df_clean.columns:
        df_clean = classify_roles(df_clean)
    
    return df_clean


def process_job_skills(
    df: pd.DataFrame,
    db_manager,
    description_column: str = 'description'
) -> Tuple[pd.DataFrame, int]:
    """
    Complete skill extraction and database insertion workflow.
    
    This function combines skill extraction from job descriptions and
    database insertion into a single workflow function.
    
    Args:
        df: DataFrame with job postings (must have 'id' and description columns)
        db_manager: DatabaseManager instance for database operations
        description_column: Name of the column containing job descriptions
    
    Returns:
        Tuple of (skills_df, inserted_count) where:
        - skills_df: DataFrame with extracted skills
        - inserted_count: Number of skills inserted into database
    
    Example:
        >>> skills_df, count = process_job_skills(jobs_df, db_manager)
        >>> print(f"Extracted and inserted {count} skills")
    """
    logger.info("Starting complete skill extraction and database insertion workflow")
    
    # Extract skills from job descriptions
    skills_df = extract_skills_from_dataframe(df, description_column)
    
    # Insert skills into database
    inserted_count = insert_skills_to_database(skills_df, db_manager)
    
    logger.info(f"Skill processing complete: {len(skills_df)} skills extracted, {inserted_count} inserted")
    
    return skills_df, inserted_count