"""
Data Export Module for UK Job Market Intelligence Platform

This module exports cleaned and analyzed data to CSV files for Power BI visualization.
Exports include:
- job_postings_clean.csv: All cleaned job data
- skills_demand.csv: Ranked skill statistics
- regional_summary.csv: Regional opportunity scores
- weekly_trends.csv: Time series data

Author: UK Job Market Intelligence Platform
Date: 2026-04-22
"""

import pandas as pd
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import os
import sys

# Add database directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'database'))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def export_job_postings_clean(db_manager, output_dir: str = "data/cleaned") -> str:
    """
    Export cleaned job postings data to CSV for Power BI.
    
    This function exports all cleaned job data including:
    - Job details (title, company, location)
    - Salary information
    - Role classification
    - Region standardization
    - Posting dates
    
    Args:
        db_manager: DatabaseManager instance
        output_dir: Directory to save output files
        
    Returns:
        Path to the exported CSV file
    """
    logger.info("Exporting cleaned job postings data")
    
    try:
        # Query all cleaned job data
        query = """
        SELECT 
            id,
            source,
            title,
            company,
            region,
            city,
            salary_min,
            salary_max,
            salary_avg,
            contract_type,
            posting_date,
            collected_date,
            role_category,
            description
        FROM job_postings
        WHERE salary_avg IS NOT NULL
        ORDER BY posting_date DESC, id
        """
        
        with db_manager.get_connection() as conn:
            jobs_df = pd.read_sql(query, conn)
        
        if jobs_df.empty:
            logger.warning("No job postings found to export")
            return None
        
        logger.info(f"Loaded {len(jobs_df)} job postings for export")
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Export to CSV
        output_path = os.path.join(output_dir, 'job_postings_clean.csv')
        jobs_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        logger.info(f"Exported job postings to {output_path}")
        logger.info(f"  - Total jobs: {len(jobs_df):,}")
        logger.info(f"  - Date range: {jobs_df['posting_date'].min()} to {jobs_df['posting_date'].max()}")
        logger.info(f"  - Regions: {jobs_df['region'].nunique()}")
        logger.info(f"  - Role categories: {jobs_df['role_category'].nunique()}")
        
        return output_path
        
    except Exception as e:
        logger.error(f"Failed to export job postings: {e}")
        raise


def export_skills_demand(db_manager, output_dir: str = "data/cleaned") -> str:
    """
    Export skill demand statistics to CSV for Power BI.
    
    This function exports ranked skill statistics including:
    - Skill name and category
    - Job count (number of postings requiring the skill)
    - Demand percentage
    - Role category breakdown
    
    Args:
        db_manager: DatabaseManager instance
        output_dir: Directory to save output files
        
    Returns:
        Path to the exported CSV file
    """
    logger.info("Exporting skill demand statistics")
    
    try:
        # Query skill demand data
        query = """
        SELECT 
            se.skill_name,
            se.skill_category,
            COUNT(DISTINCT se.job_id) as job_count,
            COUNT(DISTINCT jp.role_category) as role_categories,
            AVG(jp.salary_avg) as avg_salary_for_skill
        FROM skills_extracted se
        JOIN job_postings jp ON se.job_id = jp.id
        WHERE jp.salary_avg IS NOT NULL
        GROUP BY se.skill_name, se.skill_category
        ORDER BY job_count DESC
        """
        
        with db_manager.get_connection() as conn:
            skills_df = pd.read_sql(query, conn)
        
        if skills_df.empty:
            logger.warning("No skill data found to export")
            return None
        
        # Get total job count for percentage calculation
        total_jobs_query = "SELECT COUNT(*) as total FROM job_postings WHERE salary_avg IS NOT NULL"
        with db_manager.get_connection() as conn:
            total_jobs = pd.read_sql(total_jobs_query, conn)['total'].iloc[0]
        
        # Calculate demand percentage
        skills_df['demand_percentage'] = (skills_df['job_count'] / total_jobs) * 100
        skills_df['demand_percentage'] = skills_df['demand_percentage'].round(2)
        
        # Round salary
        skills_df['avg_salary_for_skill'] = skills_df['avg_salary_for_skill'].round(2)
        
        logger.info(f"Loaded {len(skills_df)} unique skills for export")
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Export to CSV
        output_path = os.path.join(output_dir, 'skills_demand.csv')
        skills_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        logger.info(f"Exported skill demand to {output_path}")
        logger.info(f"  - Total skills: {len(skills_df):,}")
        logger.info(f"  - Top skill: {skills_df.iloc[0]['skill_name']} ({skills_df.iloc[0]['demand_percentage']:.1f}%)")
        logger.info(f"  - Skill categories: {skills_df['skill_category'].nunique()}")
        
        return output_path
        
    except Exception as e:
        logger.error(f"Failed to export skill demand: {e}")
        raise


def export_regional_summary(db_manager, output_dir: str = "data/cleaned") -> str:
    """
    Export regional opportunity scores to CSV for Power BI.
    
    This function exports regional analysis including:
    - Region name
    - Vacancy volume
    - Average salary
    - Year-over-year growth
    - Opportunity score
    
    Args:
        db_manager: DatabaseManager instance
        output_dir: Directory to save output files
        
    Returns:
        Path to the exported CSV file
    """
    logger.info("Exporting regional summary data")
    
    try:
        # Query regional data
        query = """
        SELECT 
            region,
            COUNT(*) as vacancy_volume,
            AVG(salary_avg) as avg_salary,
            MIN(posting_date) as earliest_posting,
            MAX(posting_date) as latest_posting,
            COUNT(DISTINCT role_category) as role_categories,
            COUNT(DISTINCT company) as unique_companies
        FROM job_postings
        WHERE region IS NOT NULL
        AND salary_avg IS NOT NULL
        GROUP BY region
        ORDER BY vacancy_volume DESC
        """
        
        with db_manager.get_connection() as conn:
            regional_df = pd.read_sql(query, conn)
        
        if regional_df.empty:
            logger.warning("No regional data found to export")
            return None
        
        logger.info(f"Loaded data for {len(regional_df)} regions")
        
        # Round salary values
        regional_df['avg_salary'] = regional_df['avg_salary'].round(2)
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Export to CSV
        output_path = os.path.join(output_dir, 'regional_summary.csv')
        regional_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        logger.info(f"Exported regional summary to {output_path}")
        logger.info(f"  - Total regions: {len(regional_df):,}")
        logger.info(f"  - Top region: {regional_df.iloc[0]['region']} ({regional_df.iloc[0]['vacancy_volume']} jobs)")
        logger.info(f"  - Avg salary range: £{regional_df['avg_salary'].min():,.0f} - £{regional_df['avg_salary'].max():,.0f}")
        
        return output_path
        
    except Exception as e:
        logger.error(f"Failed to export regional summary: {e}")
        raise


def export_weekly_trends(db_manager, output_dir: str = "data/cleaned") -> str:
    """
    Export time series data to CSV for Power BI.
    
    This function exports weekly job posting trends including:
    - Week start date
    - Job count
    - Average salary
    - 4-week rolling average
    - Week-over-week growth
    
    Args:
        db_manager: DatabaseManager instance
        output_dir: Directory to save output files
        
    Returns:
        Path to the exported CSV file
    """
    logger.info("Exporting weekly trends data")
    
    try:
        # Query job postings with dates
        query = """
        SELECT 
            posting_date,
            role_category,
            salary_avg,
            region
        FROM job_postings
        WHERE posting_date IS NOT NULL
        AND salary_avg IS NOT NULL
        ORDER BY posting_date
        """
        
        with db_manager.get_connection() as conn:
            jobs_df = pd.read_sql(query, conn)
        
        if jobs_df.empty:
            logger.warning("No time series data found to export")
            return None
        
        # Convert posting_date to datetime
        jobs_df['posting_date'] = pd.to_datetime(jobs_df['posting_date'], errors='coerce')
        jobs_df = jobs_df.dropna(subset=['posting_date'])
        
        if jobs_df.empty:
            logger.warning("No valid posting dates found")
            return None
        
        # Group by week
        jobs_df['week_start'] = jobs_df['posting_date'].dt.to_period('W').dt.start_time
        
        # Weekly aggregation
        weekly_df = jobs_df.groupby('week_start').agg({
            'posting_date': 'count',
            'salary_avg': 'mean'
        }).reset_index()
        weekly_df.columns = ['week_start', 'job_count', 'avg_salary']
        weekly_df = weekly_df.sort_values('week_start')
        
        # Calculate 4-week rolling average
        weekly_df['rolling_avg_4week'] = weekly_df['job_count'].rolling(
            window=4, min_periods=1
        ).mean().round(2)
        
        # Calculate week-over-week growth
        weekly_df['week_over_week_growth'] = (weekly_df['job_count'].pct_change() * 100).round(2)
        
        # Round salary
        weekly_df['avg_salary'] = weekly_df['avg_salary'].round(2)
        
        logger.info(f"Loaded {len(weekly_df)} weeks of data")
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Export to CSV
        output_path = os.path.join(output_dir, 'weekly_trends.csv')
        weekly_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        
        logger.info(f"Exported weekly trends to {output_path}")
        logger.info(f"  - Total weeks: {len(weekly_df):,}")
        logger.info(f"  - Date range: {weekly_df['week_start'].min()} to {weekly_df['week_start'].max()}")
        logger.info(f"  - Avg weekly jobs: {weekly_df['job_count'].mean():.1f}")
        logger.info(f"  - Peak week: {weekly_df['job_count'].max()} jobs")
        
        return output_path
        
    except Exception as e:
        logger.error(f"Failed to export weekly trends: {e}")
        raise


def export_all_for_powerbi(db_manager, output_dir: str = "data/cleaned") -> Dict[str, str]:
    """
    Export all data files for Power BI dashboard.
    
    This is the main export function that creates all required CSV files:
    1. job_postings_clean.csv - All cleaned job data
    2. skills_demand.csv - Ranked skill statistics
    3. regional_summary.csv - Regional opportunity scores
    4. weekly_trends.csv - Time series data
    
    Args:
        db_manager: DatabaseManager instance
        output_dir: Directory to save output files
        
    Returns:
        Dictionary with paths to all exported files
    """
    logger.info("=" * 60)
    logger.info("EXPORTING ALL DATA FOR POWER BI")
    logger.info("=" * 60)
    
    exported_files = {}
    
    try:
        # 1. Export job postings
        logger.info("\n1. Exporting job postings...")
        job_postings_path = export_job_postings_clean(db_manager, output_dir)
        if job_postings_path:
            exported_files['job_postings'] = job_postings_path
        
        # 2. Export skill demand
        logger.info("\n2. Exporting skill demand...")
        skills_path = export_skills_demand(db_manager, output_dir)
        if skills_path:
            exported_files['skills_demand'] = skills_path
        
        # 3. Export regional summary
        logger.info("\n3. Exporting regional summary...")
        regional_path = export_regional_summary(db_manager, output_dir)
        if regional_path:
            exported_files['regional_summary'] = regional_path
        
        # 4. Export weekly trends
        logger.info("\n4. Exporting weekly trends...")
        weekly_path = export_weekly_trends(db_manager, output_dir)
        if weekly_path:
            exported_files['weekly_trends'] = weekly_path
        
        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("EXPORT COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Exported {len(exported_files)} files to {output_dir}")
        for file_type, file_path in exported_files.items():
            logger.info(f"  - {file_type}: {os.path.basename(file_path)}")
        
        return exported_files
        
    except Exception as e:
        logger.error(f"Export process failed: {e}")
        raise


# Example usage
if __name__ == '__main__':
    from db_manager import create_db_manager
    
    # Create database connection
    db_manager = create_db_manager()
    
    # Export all data for Power BI
    exported_files = export_all_for_powerbi(db_manager)
    
    print("\nExported files:")
    for file_type, file_path in exported_files.items():
        print(f"  {file_type}: {file_path}")
