"""
Analysis and Modeling Module for UK Job Market Intelligence Platform

This module performs statistical analysis and machine learning on cleaned job data:
- Salary prediction modeling
- Skill demand ranking analysis
- Regional opportunity scoring
- Time series analysis
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import mean_absolute_error, r2_score
import warnings
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SalaryPredictor:
    """
    Machine learning model for salary prediction based on job features.
    """
    
    def __init__(self):
        self.model = LinearRegression()
        self.encoders = {}
        self.feature_importance = {}
        self.is_trained = False
        
    def prepare_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare features for salary prediction model.
        
        Args:
            df: DataFrame with job data
            
        Returns:
            DataFrame with encoded features
        """
        logger.info("Preparing features for salary prediction")
        
        # Select relevant columns
        feature_columns = ['region', 'role_category', 'contract_type']
        df_features = df[feature_columns + ['salary_avg']].copy()
        
        # Remove rows with missing salary data
        df_features = df_features.dropna(subset=['salary_avg'])
        
        # Encode categorical variables
        for column in feature_columns:
            if column not in self.encoders:
                self.encoders[column] = LabelEncoder()
                df_features[f'{column}_encoded'] = self.encoders[column].fit_transform(
                    df_features[column].fillna('Unknown')
                )
            else:
                df_features[f'{column}_encoded'] = self.encoders[column].transform(
                    df_features[column].fillna('Unknown')
                )
        
        # Add number of skills feature (if skills data available)
        if 'skills_count' in df.columns:
            df_features['skills_count'] = df['skills_count'].fillna(0)
        else:
            df_features['skills_count'] = 0
        
        return df_features
    
    def train(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Train salary prediction model.
        
        Args:
            df: DataFrame with job data
            
        Returns:
            Dictionary with training metrics
        """
        logger.info("Training salary prediction model")
        
        # Prepare features
        df_features = self.prepare_features(df)
        
        if len(df_features) < 10:
            logger.warning("Insufficient data for training (< 10 samples)")
            return {'error': 'Insufficient training data'}
        
        # Define feature columns
        X_columns = ['region_encoded', 'role_category_encoded', 'contract_type_encoded', 'skills_count']
        X = df_features[X_columns]
        y = df_features['salary_avg']
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        # Train model
        self.model.fit(X_train, y_train)
        self.is_trained = True
        
        # Calculate metrics
        y_pred_train = self.model.predict(X_train)
        y_pred_test = self.model.predict(X_test)
        
        train_mae = mean_absolute_error(y_train, y_pred_train)
        test_mae = mean_absolute_error(y_test, y_pred_test)
        train_r2 = r2_score(y_train, y_pred_train)
        test_r2 = r2_score(y_test, y_pred_test)
        
        # Calculate feature importance (coefficients)
        feature_names = ['Region', 'Role Category', 'Contract Type', 'Skills Count']
        self.feature_importance = dict(zip(feature_names, self.model.coef_))
        
        metrics = {
            'training_samples': len(X_train),
            'test_samples': len(X_test),
            'train_mae': train_mae,
            'test_mae': test_mae,
            'train_r2': train_r2,
            'test_r2': test_r2,
            'feature_importance': self.feature_importance
        }
        
        logger.info(f"Model trained successfully. Test R²: {test_r2:.3f}, Test MAE: £{test_mae:,.0f}")
        
        return metrics
    
    def predict(self, df: pd.DataFrame) -> np.ndarray:
        """
        Predict salaries for new data.
        
        Args:
            df: DataFrame with job features
            
        Returns:
            Array of predicted salaries
        """
        if not self.is_trained:
            raise ValueError("Model must be trained before making predictions")
        
        df_features = self.prepare_features(df)
        X_columns = ['region_encoded', 'role_category_encoded', 'contract_type_encoded', 'skills_count']
        X = df_features[X_columns]
        
        return self.model.predict(X)


def analyze_skill_demand(skills_df: pd.DataFrame, jobs_df: pd.DataFrame) -> pd.DataFrame:
    """
    Analyze skill demand across job postings.
    
    Args:
        skills_df: DataFrame with extracted skills
        jobs_df: DataFrame with job postings
        
    Returns:
        DataFrame with skill demand analysis
    """
    logger.info("Analyzing skill demand")
    
    if skills_df.empty:
        logger.warning("No skills data available for analysis")
        return pd.DataFrame()
    
    total_jobs = len(jobs_df)
    
    # Count skill occurrences
    skill_counts = skills_df['skill_name'].value_counts().reset_index()
    skill_counts.columns = ['skill_name', 'job_count']
    
    # Calculate demand percentage
    skill_counts['demand_percentage'] = (skill_counts['job_count'] / total_jobs) * 100
    
    # Add skill categories
    skill_categories = skills_df[['skill_name', 'skill_category']].drop_duplicates()
    skill_demand = skill_counts.merge(skill_categories, on='skill_name', how='left')
    
    # Segment by role category if available
    if 'role_category' in jobs_df.columns:
        role_skill_analysis = []
        
        for role in jobs_df['role_category'].unique():
            role_jobs = jobs_df[jobs_df['role_category'] == role]
            role_job_ids = role_jobs.index.tolist()
            
            role_skills = skills_df[skills_df['job_id'].isin(role_job_ids)]
            
            if not role_skills.empty:
                role_skill_counts = role_skills['skill_name'].value_counts().reset_index()
                role_skill_counts.columns = ['skill_name', 'job_count']
                role_skill_counts['demand_percentage'] = (
                    role_skill_counts['job_count'] / len(role_jobs)
                ) * 100
                role_skill_counts['role_category'] = role
                
                role_skill_analysis.append(role_skill_counts)
        
        if role_skill_analysis:
            role_skills_df = pd.concat(role_skill_analysis, ignore_index=True)
            skill_demand = skill_demand.merge(
                role_skills_df.groupby('skill_name')['role_category'].apply(list).reset_index(),
                on='skill_name', how='left'
            )
    
    # Sort by demand
    skill_demand = skill_demand.sort_values('demand_percentage', ascending=False)
    
    logger.info(f"Analyzed {len(skill_demand)} unique skills")
    logger.info(f"Top 5 skills: {skill_demand.head()['skill_name'].tolist()}")
    
    return skill_demand


def implement_skill_demand_ranking_analysis(db_manager) -> Dict[str, Any]:
    """
    Implement skill demand ranking analysis as specified in task 7.2.
    
    This function:
    1. Queries skills_extracted table to count skill occurrences
    2. Calculates skill_demand_% = (count / total_postings) * 100
    3. Segments skill demand by role_category
    4. Generates ranked skill lists per role type
    5. Saves results to analysis output
    
    Args:
        db_manager: DatabaseManager instance
        
    Returns:
        Dictionary with skill demand analysis results
    """
    logger.info("Implementing skill demand ranking analysis (Task 7.2)")
    
    try:
        # Query skills_extracted table with job posting data
        skills_query = """
        SELECT 
            se.skill_name,
            se.skill_category,
            se.job_id,
            jp.role_category
        FROM skills_extracted se
        JOIN job_postings jp ON se.job_id = jp.id
        WHERE jp.role_category IS NOT NULL
        """
        
        with db_manager.get_connection() as conn:
            skills_df = pd.read_sql(skills_query, conn)
        
        if skills_df.empty:
            logger.warning("No skills data found in database")
            return {'error': 'No skills data available'}
        
        # Get total job postings count
        total_jobs_query = "SELECT COUNT(*) as total_jobs FROM job_postings"
        with db_manager.get_connection() as conn:
            total_jobs_result = pd.read_sql(total_jobs_query, conn)
            total_jobs = total_jobs_result['total_jobs'].iloc[0]
        
        logger.info(f"Loaded {len(skills_df)} skill records from {total_jobs} total job postings")
        
        # 1. Count skill occurrences across all jobs
        overall_skill_counts = skills_df['skill_name'].value_counts().reset_index()
        overall_skill_counts.columns = ['skill_name', 'job_count']
        
        # 2. Calculate skill_demand_% = (count / total_postings) * 100
        overall_skill_counts['skill_demand_percent'] = (
            overall_skill_counts['job_count'] / total_jobs
        ) * 100
        
        # Add skill categories
        skill_categories = skills_df[['skill_name', 'skill_category']].drop_duplicates()
        overall_skill_demand = overall_skill_counts.merge(
            skill_categories, on='skill_name', how='left'
        )
        
        # Sort by demand percentage
        overall_skill_demand = overall_skill_demand.sort_values(
            'skill_demand_percent', ascending=False
        )
        
        # 3. Segment skill demand by role_category
        role_skill_rankings = {}
        role_categories = skills_df['role_category'].unique()
        
        for role in role_categories:
            if pd.isna(role):
                continue
                
            # Filter skills for this role category
            role_skills = skills_df[skills_df['role_category'] == role]
            
            # Count jobs for this role category
            role_jobs_query = f"""
            SELECT COUNT(*) as role_jobs 
            FROM job_postings 
            WHERE role_category = '{role}'
            """
            with db_manager.get_connection() as conn:
                role_jobs_result = pd.read_sql(role_jobs_query, conn)
                role_jobs_count = role_jobs_result['role_jobs'].iloc[0]
            
            if role_jobs_count == 0:
                continue
            
            # Count skill occurrences for this role
            role_skill_counts = role_skills['skill_name'].value_counts().reset_index()
            role_skill_counts.columns = ['skill_name', 'job_count']
            
            # Calculate demand percentage for this role
            role_skill_counts['skill_demand_percent'] = (
                role_skill_counts['job_count'] / role_jobs_count
            ) * 100
            
            # Add skill categories
            role_skill_demand = role_skill_counts.merge(
                skill_categories, on='skill_name', how='left'
            )
            
            # Sort by demand percentage
            role_skill_demand = role_skill_demand.sort_values(
                'skill_demand_percent', ascending=False
            )
            
            role_skill_rankings[role] = {
                'total_jobs': role_jobs_count,
                'unique_skills': len(role_skill_demand),
                'skill_rankings': role_skill_demand
            }
            
            logger.info(f"Analyzed {len(role_skill_demand)} skills for {role} "
                       f"({role_jobs_count} jobs)")
        
        # 4. Generate summary statistics
        summary_stats = {
            'total_job_postings': total_jobs,
            'total_unique_skills': len(overall_skill_demand),
            'total_skill_extractions': len(skills_df),
            'role_categories_analyzed': len(role_skill_rankings),
            'top_10_skills_overall': overall_skill_demand.head(10)[
                ['skill_name', 'skill_demand_percent', 'skill_category']
            ].to_dict('records')
        }
        
        # Generate top skills by category
        skill_category_rankings = {}
        for category in skills_df['skill_category'].unique():
            if pd.isna(category):
                continue
                
            category_skills = overall_skill_demand[
                overall_skill_demand['skill_category'] == category
            ].head(10)
            
            skill_category_rankings[category] = category_skills[
                ['skill_name', 'skill_demand_percent']
            ].to_dict('records')
        
        # 5. Prepare results for output
        results = {
            'timestamp': datetime.now().isoformat(),
            'task': '7.2 - Skill Demand Ranking Analysis',
            'summary_statistics': summary_stats,
            'overall_skill_rankings': overall_skill_demand,
            'role_based_skill_rankings': role_skill_rankings,
            'skill_category_rankings': skill_category_rankings
        }
        
        # Log key findings
        logger.info("=== SKILL DEMAND RANKING ANALYSIS RESULTS ===")
        logger.info(f"Total job postings analyzed: {total_jobs:,}")
        logger.info(f"Total unique skills found: {len(overall_skill_demand):,}")
        logger.info(f"Total skill extractions: {len(skills_df):,}")
        logger.info(f"Role categories analyzed: {len(role_skill_rankings)}")
        
        logger.info("\n=== TOP 10 SKILLS OVERALL ===")
        for i, skill in enumerate(overall_skill_demand.head(10).itertuples(), 1):
            logger.info(f"{i:2d}. {skill.skill_name:<20} "
                       f"{skill.skill_demand_percent:5.1f}% "
                       f"({skill.skill_category})")
        
        logger.info("\n=== TOP SKILLS BY ROLE CATEGORY ===")
        for role, data in role_skill_rankings.items():
            top_skills = data['skill_rankings'].head(5)
            logger.info(f"\n{role} ({data['total_jobs']} jobs):")
            for i, skill in enumerate(top_skills.itertuples(), 1):
                logger.info(f"  {i}. {skill.skill_name:<15} {skill.skill_demand_percent:5.1f}%")
        
        # Save results to analysis output files
        saved_files = save_skill_demand_analysis_results(results)
        results['saved_files'] = saved_files
        
        return results
        
    except Exception as e:
        logger.error(f"Skill demand ranking analysis failed: {e}")
        return {'error': str(e)}
        
def save_skill_demand_analysis_results(results: Dict[str, Any], output_dir: str = "data/cleaned") -> Dict[str, str]:
    """
    Save skill demand analysis results to CSV files for further analysis and visualization.
    
    Args:
        results: Results dictionary from implement_skill_demand_ranking_analysis
        output_dir: Directory to save output files
        
    Returns:
        Dictionary with paths to saved files
    """
    import os
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    saved_files = {}
    
    try:
        # 1. Save overall skill rankings
        if 'overall_skill_rankings' in results:
            overall_path = os.path.join(output_dir, 'skill_demand_overall.csv')
            results['overall_skill_rankings'].to_csv(overall_path, index=False)
            saved_files['overall_rankings'] = overall_path
            logger.info(f"Saved overall skill rankings to {overall_path}")
        
        # 2. Save role-based skill rankings
        if 'role_based_skill_rankings' in results:
            for role, data in results['role_based_skill_rankings'].items():
                # Clean role name for filename
                clean_role = role.replace(' ', '_').replace('/', '_').lower()
                role_path = os.path.join(output_dir, f'skill_demand_{clean_role}.csv')
                
                # Add role metadata to the dataframe
                role_df = data['skill_rankings'].copy()
                role_df['role_category'] = role
                role_df['total_jobs_in_role'] = data['total_jobs']
                
                role_df.to_csv(role_path, index=False)
                saved_files[f'role_{clean_role}'] = role_path
                logger.info(f"Saved {role} skill rankings to {role_path}")
        
        # 3. Save skill category rankings
        if 'skill_category_rankings' in results:
            category_data = []
            for category, skills in results['skill_category_rankings'].items():
                for skill in skills:
                    category_data.append({
                        'skill_category': category,
                        'skill_name': skill['skill_name'],
                        'skill_demand_percent': skill['skill_demand_percent']
                    })
            
            if category_data:
                category_df = pd.DataFrame(category_data)
                category_path = os.path.join(output_dir, 'skill_demand_by_category.csv')
                category_df.to_csv(category_path, index=False)
                saved_files['category_rankings'] = category_path
                logger.info(f"Saved skill category rankings to {category_path}")
        
        # 4. Save summary statistics
        if 'summary_statistics' in results:
            summary_path = os.path.join(output_dir, 'skill_demand_summary.csv')
            
            # Convert summary to DataFrame format
            summary_data = []
            stats = results['summary_statistics']
            
            # Add basic statistics
            summary_data.extend([
                {'metric': 'total_job_postings', 'value': stats['total_job_postings']},
                {'metric': 'total_unique_skills', 'value': stats['total_unique_skills']},
                {'metric': 'total_skill_extractions', 'value': stats['total_skill_extractions']},
                {'metric': 'role_categories_analyzed', 'value': stats['role_categories_analyzed']}
            ])
            
            # Add top skills
            for i, skill in enumerate(stats['top_10_skills_overall'], 1):
                summary_data.append({
                    'metric': f'top_skill_{i:02d}',
                    'value': f"{skill['skill_name']} ({skill['skill_demand_percent']:.1f}%)"
                })
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_csv(summary_path, index=False)
            saved_files['summary'] = summary_path
            logger.info(f"Saved summary statistics to {summary_path}")
        
        logger.info(f"Successfully saved {len(saved_files)} analysis output files")
        return saved_files
        
    except Exception as e:
        logger.error(f"Failed to save skill demand analysis results: {e}")
        return {'error': str(e)}


def implement_regional_opportunity_scoring(db_manager) -> Dict[str, Any]:
    """
    Implement regional opportunity scoring as specified in task 7.3.
    
    This function:
    1. Calculates composite opportunity_score per region
    2. Uses formula: (vacancy_volume * 0.4) + (salary_avg * 0.4) + (yoy_growth * 0.2)
    3. Joins job_postings with ons_vacancies data
    4. Calculates year-over-year growth rates
    5. Saves regional scores to analysis output
    
    Args:
        db_manager: DatabaseManager instance
        
    Returns:
        Dictionary with regional opportunity scoring results
    """
    logger.info("Implementing regional opportunity scoring (Task 7.3)")
    
    try:
        # Load job postings data by region
        jobs_query = """
        SELECT 
            region,
            COUNT(*) as vacancy_volume,
            AVG(salary_avg) as avg_salary,
            MIN(posting_date) as earliest_posting,
            MAX(posting_date) as latest_posting,
            COUNT(CASE WHEN posting_date >= date('now', '-30 days') THEN 1 END) as recent_jobs,
            COUNT(CASE WHEN posting_date < date('now', '-30 days') THEN 1 END) as older_jobs
        FROM job_postings 
        WHERE region IS NOT NULL 
        AND salary_avg IS NOT NULL 
        AND salary_avg > 0
        GROUP BY region
        HAVING COUNT(*) >= 5  -- Only include regions with at least 5 job postings
        ORDER BY region
        """
        
        with db_manager.get_connection() as conn:
            jobs_regional_df = pd.read_sql(jobs_query, conn)
        
        if jobs_regional_df.empty:
            logger.warning("No regional job data found")
            return {'error': 'No regional job data available'}
        
        logger.info(f"Loaded job data for {len(jobs_regional_df)} regions")
        
        # Load ONS vacancy data if available
        ons_vacancies_query = """
        SELECT 
            region,
            period,
            SUM(vacancy_count) as total_vacancies,
            AVG(yoy_change_percent) as avg_yoy_change
        FROM ons_vacancies 
        WHERE region IS NOT NULL
        GROUP BY region, period
        ORDER BY region, period DESC
        """
        
        try:
            with db_manager.get_connection() as conn:
                ons_vacancies_df = pd.read_sql(ons_vacancies_query, conn)
            logger.info(f"Loaded ONS vacancy data for {len(ons_vacancies_df)} region-period combinations")
        except Exception as e:
            logger.warning(f"Could not load ONS vacancy data: {e}")
            ons_vacancies_df = pd.DataFrame()
        
        # Calculate regional opportunity scores
        regional_scores = []
        
        for _, region_data in jobs_regional_df.iterrows():
            region = region_data['region']
            vacancy_volume = region_data['vacancy_volume']
            avg_salary = region_data['avg_salary']
            recent_jobs = region_data['recent_jobs']
            older_jobs = region_data['older_jobs']
            
            # Calculate year-over-year growth from job postings data
            if older_jobs > 0:
                # Simple growth calculation: (recent - older) / older * 100
                yoy_growth_jobs = ((recent_jobs - older_jobs) / older_jobs) * 100
            else:
                yoy_growth_jobs = 0 if recent_jobs == 0 else 100  # 100% if only recent jobs
            
            # Try to get ONS growth data for this region
            ons_growth = 0
            if not ons_vacancies_df.empty:
                region_ons = ons_vacancies_df[ons_vacancies_df['region'] == region]
                if not region_ons.empty:
                    # Use the most recent ONS growth data
                    latest_ons = region_ons.iloc[0]
                    ons_growth = latest_ons['avg_yoy_change'] if pd.notna(latest_ons['avg_yoy_change']) else 0
            
            # Use ONS growth if available, otherwise use job postings growth
            yoy_growth = ons_growth if ons_growth != 0 else yoy_growth_jobs
            
            # Normalize metrics to 0-100 scale for scoring
            
            # 1. Vacancy Volume Score (0-100)
            # Use log transformation to handle wide range of values
            volume_score = min(100, max(0, np.log1p(vacancy_volume) * 15))
            
            # 2. Salary Score (0-100)
            # Normalize based on typical analyst salary range (£20k-£80k)
            salary_score = min(100, max(0, (avg_salary - 20000) / 600)) if avg_salary > 0 else 0
            
            # 3. Growth Score (0-100, with negative growth getting lower scores)
            # Map -50% to +50% growth to 0-100 scale
            growth_score = min(100, max(0, (yoy_growth + 50) * 2))
            
            # Calculate composite opportunity score using specified formula
            # Formula: (vacancy_volume * 0.4) + (salary_avg * 0.4) + (yoy_growth * 0.2)
            opportunity_score = (volume_score * 0.4) + (salary_score * 0.4) + (growth_score * 0.2)
            
            regional_scores.append({
                'region': region,
                'vacancy_volume': int(vacancy_volume),
                'avg_salary': round(avg_salary, 2),
                'yoy_growth_percent': round(yoy_growth, 2),
                'volume_score': round(volume_score, 1),
                'salary_score': round(salary_score, 1),
                'growth_score': round(growth_score, 1),
                'opportunity_score': round(opportunity_score, 1),
                'recent_jobs': int(recent_jobs),
                'older_jobs': int(older_jobs),
                'ons_data_available': ons_growth != 0
            })
        
        # Convert to DataFrame and sort by opportunity score
        regional_scores_df = pd.DataFrame(regional_scores)
        regional_scores_df = regional_scores_df.sort_values('opportunity_score', ascending=False)
        
        # Calculate summary statistics
        summary_stats = {
            'total_regions_analyzed': len(regional_scores_df),
            'total_job_postings': int(jobs_regional_df['vacancy_volume'].sum()),
            'avg_opportunity_score': round(regional_scores_df['opportunity_score'].mean(), 1),
            'top_region': {
                'name': regional_scores_df.iloc[0]['region'],
                'score': regional_scores_df.iloc[0]['opportunity_score'],
                'vacancy_volume': regional_scores_df.iloc[0]['vacancy_volume'],
                'avg_salary': regional_scores_df.iloc[0]['avg_salary']
            },
            'regions_with_ons_data': int(regional_scores_df['ons_data_available'].sum()),
            'scoring_formula': '(vacancy_volume * 0.4) + (salary_avg * 0.4) + (yoy_growth * 0.2)'
        }
        
        # Prepare results
        results = {
            'timestamp': datetime.now().isoformat(),
            'task': '7.3 - Regional Opportunity Scoring',
            'summary_statistics': summary_stats,
            'regional_scores': regional_scores_df,
            'methodology': {
                'vacancy_volume_scoring': 'Log-transformed, scaled 0-100',
                'salary_scoring': 'Linear scale from £20k-£80k mapped to 0-100',
                'growth_scoring': '-50% to +50% growth mapped to 0-100',
                'composite_formula': '(volume_score * 0.4) + (salary_score * 0.4) + (growth_score * 0.2)',
                'data_sources': ['job_postings', 'ons_vacancies (if available)']
            }
        }
        
        # Log key findings
        logger.info("=== REGIONAL OPPORTUNITY SCORING RESULTS ===")
        logger.info(f"Total regions analyzed: {summary_stats['total_regions_analyzed']}")
        logger.info(f"Total job postings: {summary_stats['total_job_postings']:,}")
        logger.info(f"Average opportunity score: {summary_stats['avg_opportunity_score']}")
        logger.info(f"Regions with ONS data: {summary_stats['regions_with_ons_data']}")
        
        logger.info("\n=== TOP 10 REGIONS BY OPPORTUNITY SCORE ===")
        for i, region in enumerate(regional_scores_df.head(10).itertuples(), 1):
            logger.info(f"{i:2d}. {region.region:<20} "
                       f"Score: {region.opportunity_score:5.1f} "
                       f"(Vol: {region.vacancy_volume:3d}, "
                       f"Salary: £{region.avg_salary:,.0f}, "
                       f"Growth: {region.yoy_growth_percent:+5.1f}%)")
        
        # Save results to analysis output
        saved_files = save_regional_opportunity_results(results)
        results['saved_files'] = saved_files
        
        return results
        
    except Exception as e:
        logger.error(f"Regional opportunity scoring failed: {e}")
        return {'error': str(e)}


def save_regional_opportunity_results(results: Dict[str, Any], output_dir: str = "data/cleaned") -> Dict[str, str]:
    """
    Save regional opportunity scoring results to CSV files.
    
    Args:
        results: Results dictionary from implement_regional_opportunity_scoring
        output_dir: Directory to save output files
        
    Returns:
        Dictionary with paths to saved files
    """
    import os
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    saved_files = {}
    
    try:
        # Save regional scores
        if 'regional_scores' in results:
            scores_path = os.path.join(output_dir, 'regional_opportunity_scores.csv')
            results['regional_scores'].to_csv(scores_path, index=False)
            saved_files['regional_scores'] = scores_path
            logger.info(f"Saved regional opportunity scores to {scores_path}")
        
        # Save summary statistics
        if 'summary_statistics' in results:
            summary_path = os.path.join(output_dir, 'regional_opportunity_summary.csv')
            
            # Convert summary to DataFrame format
            summary_data = []
            stats = results['summary_statistics']
            
            # Add basic statistics
            summary_data.extend([
                {'metric': 'total_regions_analyzed', 'value': stats['total_regions_analyzed']},
                {'metric': 'total_job_postings', 'value': stats['total_job_postings']},
                {'metric': 'avg_opportunity_score', 'value': stats['avg_opportunity_score']},
                {'metric': 'regions_with_ons_data', 'value': stats['regions_with_ons_data']},
                {'metric': 'scoring_formula', 'value': stats['scoring_formula']}
            ])
            
            # Add top region details
            top_region = stats['top_region']
            summary_data.extend([
                {'metric': 'top_region_name', 'value': top_region['name']},
                {'metric': 'top_region_score', 'value': top_region['score']},
                {'metric': 'top_region_vacancy_volume', 'value': top_region['vacancy_volume']},
                {'metric': 'top_region_avg_salary', 'value': top_region['avg_salary']}
            ])
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_csv(summary_path, index=False)
            saved_files['summary'] = summary_path
            logger.info(f"Saved regional opportunity summary to {summary_path}")
        
        logger.info(f"Successfully saved {len(saved_files)} regional opportunity analysis files")
        return saved_files
        
    except Exception as e:
        logger.error(f"Failed to save regional opportunity results: {e}")
        return {'error': str(e)}


def calculate_regional_opportunity_score(
    jobs_df: pd.DataFrame,
    ons_vacancies_df: Optional[pd.DataFrame] = None,
    ons_salaries_df: Optional[pd.DataFrame] = None
) -> pd.DataFrame:
    """
    Calculate regional opportunity scores based on multiple factors.
    
    DEPRECATED: Use implement_regional_opportunity_scoring() for Task 7.3 implementation.
    This function is kept for backward compatibility.
    
    Args:
        jobs_df: DataFrame with job postings
        ons_vacancies_df: Optional ONS vacancy data
        ons_salaries_df: Optional ONS salary data
        
    Returns:
        DataFrame with regional opportunity scores
    """
    logger.warning("Using deprecated calculate_regional_opportunity_score(). "
                  "Use implement_regional_opportunity_scoring() for Task 7.3.")
    
    if 'region' not in jobs_df.columns:
        logger.error("Region column not found in jobs data")
        return pd.DataFrame()
    
    regional_stats = []
    
    for region in jobs_df['region'].unique():
        region_jobs = jobs_df[jobs_df['region'] == region]
        
        # Basic job market metrics
        vacancy_volume = len(region_jobs)
        avg_salary = region_jobs['salary_avg'].mean() if 'salary_avg' in region_jobs.columns else 0
        
        # Calculate year-over-year growth (simplified - using recent vs older postings)
        if 'posting_date' in region_jobs.columns:
            region_jobs['posting_date'] = pd.to_datetime(region_jobs['posting_date'], errors='coerce')
            recent_cutoff = datetime.now() - timedelta(days=30)
            
            recent_jobs = region_jobs[region_jobs['posting_date'] >= recent_cutoff]
            older_jobs = region_jobs[region_jobs['posting_date'] < recent_cutoff]
            
            if len(older_jobs) > 0:
                yoy_growth = ((len(recent_jobs) - len(older_jobs)) / len(older_jobs)) * 100
            else:
                yoy_growth = 0
        else:
            yoy_growth = 0
        
        # Normalize salary (0-100 scale)
        salary_score = min(100, max(0, (avg_salary - 20000) / 1000)) if avg_salary > 0 else 0
        
        # Normalize volume (0-100 scale, log transform for better distribution)
        volume_score = min(100, np.log1p(vacancy_volume) * 10)
        
        # Normalize growth (-100 to +100 scale)
        growth_score = max(-100, min(100, yoy_growth))
        
        # Calculate composite opportunity score
        # Formula: (vacancy_volume * 0.4) + (salary_avg * 0.4) + (yoy_growth * 0.2)
        opportunity_score = (volume_score * 0.4) + (salary_score * 0.4) + (growth_score * 0.2)
        
        regional_stats.append({
            'region': region,
            'vacancy_volume': vacancy_volume,
            'avg_salary': avg_salary,
            'yoy_growth_percent': yoy_growth,
            'volume_score': volume_score,
            'salary_score': salary_score,
            'growth_score': growth_score,
            'opportunity_score': opportunity_score
        })
    
    regional_df = pd.DataFrame(regional_stats)
    regional_df = regional_df.sort_values('opportunity_score', ascending=False)
    
    logger.info(f"Calculated opportunity scores for {len(regional_df)} regions")
    if not regional_df.empty:
        logger.info(f"Top region: {regional_df.iloc[0]['region']} (score: {regional_df.iloc[0]['opportunity_score']:.1f})")
    
    return regional_df


def implement_time_series_analysis(db_manager) -> Dict[str, Any]:
    """
    Implement time series analysis as specified in task 7.4.
    
    This function:
    1. Groups job postings by week/month
    2. Calculates 4-week rolling average of posting volume
    3. Identifies trending sectors (growing vs declining)
    4. Generates time series data for visualization
    5. Saves time series results to analysis output
    
    Args:
        db_manager: DatabaseManager instance
        
    Returns:
        Dictionary with time series analysis results
    """
    logger.info("Implementing time series analysis (Task 7.4)")
    
    try:
        # Load job postings data with posting dates
        jobs_query = """
        SELECT 
            id,
            posting_date,
            role_category,
            region,
            salary_avg,
            contract_type
        FROM job_postings 
        WHERE posting_date IS NOT NULL
        ORDER BY posting_date
        """
        
        with db_manager.get_connection() as conn:
            jobs_df = pd.read_sql(jobs_query, conn)
        
        if jobs_df.empty:
            logger.warning("No job postings with dates found")
            return {'error': 'No job postings with dates available'}
        
        logger.info(f"Loaded {len(jobs_df)} job postings for time series analysis")
        
        # Convert posting_date to datetime
        jobs_df['posting_date'] = pd.to_datetime(jobs_df['posting_date'], errors='coerce')
        jobs_df = jobs_df.dropna(subset=['posting_date'])
        
        if jobs_df.empty:
            logger.warning("No valid posting dates found after conversion")
            return {'error': 'No valid posting dates available'}
        
        # 1. Group job postings by week and month
        jobs_df['week'] = jobs_df['posting_date'].dt.to_period('W')
        jobs_df['month'] = jobs_df['posting_date'].dt.to_period('M')
        
        # Weekly aggregation
        weekly_counts = jobs_df.groupby('week').agg({
            'id': 'count',
            'salary_avg': 'mean'
        }).reset_index()
        weekly_counts.columns = ['week', 'job_count', 'avg_salary']
        weekly_counts['week'] = weekly_counts['week'].dt.start_time
        weekly_counts = weekly_counts.sort_values('week')
        
        # Monthly aggregation
        monthly_counts = jobs_df.groupby('month').agg({
            'id': 'count',
            'salary_avg': 'mean'
        }).reset_index()
        monthly_counts.columns = ['month', 'job_count', 'avg_salary']
        monthly_counts['month'] = monthly_counts['month'].dt.start_time
        monthly_counts = monthly_counts.sort_values('month')
        
        # 2. Calculate 4-week rolling average of posting volume
        weekly_counts['rolling_avg_4week'] = weekly_counts['job_count'].rolling(
            window=4, min_periods=1
        ).mean()
        
        # Calculate week-over-week growth
        weekly_counts['week_over_week_growth'] = weekly_counts['job_count'].pct_change() * 100
        
        # Calculate month-over-month growth
        monthly_counts['month_over_month_growth'] = monthly_counts['job_count'].pct_change() * 100
        
        # 3. Identify trending sectors (growing vs declining)
        sector_trends = []
        
        if 'role_category' in jobs_df.columns:
            for role in jobs_df['role_category'].unique():
                if pd.isna(role):
                    continue
                    
                role_jobs = jobs_df[jobs_df['role_category'] == role]
                
                # Weekly trends by role
                role_weekly = role_jobs.groupby('week').size().reset_index(name='job_count')
                role_weekly['week'] = role_weekly['week'].dt.start_time
                role_weekly = role_weekly.sort_values('week')
                
                if len(role_weekly) >= 2:  # Need at least 2 weeks for meaningful trend
                    # Calculate trend using linear regression
                    x = np.arange(len(role_weekly))
                    y = role_weekly['job_count'].values
                    
                    if len(x) > 1 and np.std(x) > 0 and np.std(y) > 0:
                        # Calculate correlation coefficient and slope
                        correlation = np.corrcoef(x, y)[0, 1]
                        slope = correlation * (np.std(y) / np.std(x))
                        
                        # Calculate percentage change from first to last period
                        if role_weekly.iloc[0]['job_count'] > 0:
                            total_change = (
                                (role_weekly.iloc[-1]['job_count'] - role_weekly.iloc[0]['job_count']) /
                                role_weekly.iloc[0]['job_count']
                            ) * 100
                        else:
                            total_change = 0
                        
                        # Determine trend direction
                        if slope > 0.1:
                            trend_direction = 'Growing'
                        elif slope < -0.1:
                            trend_direction = 'Declining'
                        else:
                            trend_direction = 'Stable'
                        
                        sector_trends.append({
                            'role_category': role,
                            'trend_slope': round(slope, 3),
                            'correlation': round(correlation, 3),
                            'total_change_percent': round(total_change, 1),
                            'avg_weekly_jobs': round(y.mean(), 1),
                            'total_jobs': int(y.sum()),
                            'weeks_analyzed': len(role_weekly),
                            'trend_direction': trend_direction,
                            'trend_strength': 'Strong' if abs(correlation) > 0.7 else 'Moderate' if abs(correlation) > 0.4 else 'Weak'
                        })
        
        # Sort sector trends by trend strength and slope
        sector_trends_df = pd.DataFrame(sector_trends)
        if not sector_trends_df.empty:
            sector_trends_df = sector_trends_df.sort_values(
                ['trend_direction', 'trend_slope'], 
                ascending=[True, False]
            )
        
        # 4. Generate regional time series data
        regional_trends = []
        
        if 'region' in jobs_df.columns:
            for region in jobs_df['region'].unique():
                if pd.isna(region):
                    continue
                    
                region_jobs = jobs_df[jobs_df['region'] == region]
                region_weekly = region_jobs.groupby('week').size().reset_index(name='job_count')
                region_weekly['week'] = region_weekly['week'].dt.start_time
                region_weekly = region_weekly.sort_values('week')
                
                if len(region_weekly) >= 4:
                    # Calculate 4-week rolling average for region
                    region_weekly['rolling_avg_4week'] = region_weekly['job_count'].rolling(
                        window=4, min_periods=1
                    ).mean()
                    
                    regional_trends.append({
                        'region': region,
                        'total_jobs': int(region_weekly['job_count'].sum()),
                        'avg_weekly_jobs': round(region_weekly['job_count'].mean(), 1),
                        'peak_week_jobs': int(region_weekly['job_count'].max()),
                        'weeks_analyzed': len(region_weekly)
                    })
        
        regional_trends_df = pd.DataFrame(regional_trends)
        if not regional_trends_df.empty:
            regional_trends_df = regional_trends_df.sort_values('total_jobs', ascending=False)
        
        # 5. Calculate summary statistics
        summary_stats = {
            'analysis_period': {
                'start_date': jobs_df['posting_date'].min().isoformat(),
                'end_date': jobs_df['posting_date'].max().isoformat(),
                'total_weeks': len(weekly_counts),
                'total_months': len(monthly_counts)
            },
            'posting_volume': {
                'total_jobs': len(jobs_df),
                'avg_weekly_jobs': round(weekly_counts['job_count'].mean(), 1),
                'peak_weekly_jobs': int(weekly_counts['job_count'].max()),
                'min_weekly_jobs': int(weekly_counts['job_count'].min()),
                'avg_monthly_jobs': round(monthly_counts['job_count'].mean(), 1)
            },
            'trends': {
                'sectors_analyzed': len(sector_trends_df),
                'growing_sectors': len(sector_trends_df[sector_trends_df['trend_direction'] == 'Growing']),
                'declining_sectors': len(sector_trends_df[sector_trends_df['trend_direction'] == 'Declining']),
                'stable_sectors': len(sector_trends_df[sector_trends_df['trend_direction'] == 'Stable']),
                'regions_analyzed': len(regional_trends_df)
            }
        }
        
        # Prepare results
        results = {
            'timestamp': datetime.now().isoformat(),
            'task': '7.4 - Time Series Analysis',
            'summary_statistics': summary_stats,
            'weekly_trends': weekly_counts,
            'monthly_trends': monthly_counts,
            'sector_trends': sector_trends_df,
            'regional_trends': regional_trends_df,
            'methodology': {
                'time_grouping': 'Weekly and monthly aggregation',
                'rolling_average': '4-week rolling average',
                'trend_calculation': 'Linear regression slope and correlation',
                'trend_classification': 'Growing (slope > 0.1), Declining (slope < -0.1), Stable (otherwise)',
                'trend_strength': 'Strong (|correlation| > 0.7), Moderate (> 0.4), Weak (≤ 0.4)'
            }
        }
        
        # Log key findings
        logger.info("=== TIME SERIES ANALYSIS RESULTS ===")
        logger.info(f"Analysis period: {summary_stats['analysis_period']['start_date']} to {summary_stats['analysis_period']['end_date']}")
        logger.info(f"Total jobs analyzed: {summary_stats['posting_volume']['total_jobs']:,}")
        logger.info(f"Total weeks: {summary_stats['analysis_period']['total_weeks']}")
        logger.info(f"Average weekly jobs: {summary_stats['posting_volume']['avg_weekly_jobs']}")
        logger.info(f"Peak weekly jobs: {summary_stats['posting_volume']['peak_weekly_jobs']}")
        
        logger.info("\n=== SECTOR TRENDS ===")
        logger.info(f"Growing sectors: {summary_stats['trends']['growing_sectors']}")
        logger.info(f"Declining sectors: {summary_stats['trends']['declining_sectors']}")
        logger.info(f"Stable sectors: {summary_stats['trends']['stable_sectors']}")
        
        if not sector_trends_df.empty:
            logger.info("\n=== TOP GROWING SECTORS ===")
            growing_sectors = sector_trends_df[sector_trends_df['trend_direction'] == 'Growing'].head(5)
            for i, sector in enumerate(growing_sectors.itertuples(), 1):
                logger.info(f"{i}. {sector.role_category:<20} "
                           f"Slope: {sector.trend_slope:+6.3f} "
                           f"Change: {sector.total_change_percent:+5.1f}% "
                           f"({sector.trend_strength})")
            
            logger.info("\n=== TOP DECLINING SECTORS ===")
            declining_sectors = sector_trends_df[sector_trends_df['trend_direction'] == 'Declining'].head(5)
            for i, sector in enumerate(declining_sectors.itertuples(), 1):
                logger.info(f"{i}. {sector.role_category:<20} "
                           f"Slope: {sector.trend_slope:+6.3f} "
                           f"Change: {sector.total_change_percent:+5.1f}% "
                           f"({sector.trend_strength})")
        
        if not regional_trends_df.empty:
            logger.info("\n=== TOP REGIONS BY VOLUME ===")
            for i, region in enumerate(regional_trends_df.head(5).itertuples(), 1):
                logger.info(f"{i}. {region.region:<20} "
                           f"Total: {region.total_jobs:4d} jobs "
                           f"Avg/week: {region.avg_weekly_jobs:5.1f}")
        
        # Save results to analysis output
        saved_files = save_time_series_analysis_results(results)
        results['saved_files'] = saved_files
        
        return results
        
    except Exception as e:
        logger.error(f"Time series analysis failed: {e}")
        return {'error': str(e)}


def save_time_series_analysis_results(results: Dict[str, Any], output_dir: str = "data/cleaned") -> Dict[str, str]:
    """
    Save time series analysis results to CSV files for visualization.
    
    Args:
        results: Results dictionary from implement_time_series_analysis
        output_dir: Directory to save output files
        
    Returns:
        Dictionary with paths to saved files
    """
    import os
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    saved_files = {}
    
    try:
        # 1. Save weekly trends
        if 'weekly_trends' in results and not results['weekly_trends'].empty:
            weekly_path = os.path.join(output_dir, 'time_series_weekly_trends.csv')
            results['weekly_trends'].to_csv(weekly_path, index=False)
            saved_files['weekly_trends'] = weekly_path
            logger.info(f"Saved weekly trends to {weekly_path}")
        
        # 2. Save monthly trends
        if 'monthly_trends' in results and not results['monthly_trends'].empty:
            monthly_path = os.path.join(output_dir, 'time_series_monthly_trends.csv')
            results['monthly_trends'].to_csv(monthly_path, index=False)
            saved_files['monthly_trends'] = monthly_path
            logger.info(f"Saved monthly trends to {monthly_path}")
        
        # 3. Save sector trends
        if 'sector_trends' in results and not results['sector_trends'].empty:
            sector_path = os.path.join(output_dir, 'time_series_sector_trends.csv')
            results['sector_trends'].to_csv(sector_path, index=False)
            saved_files['sector_trends'] = sector_path
            logger.info(f"Saved sector trends to {sector_path}")
        
        # 4. Save regional trends
        if 'regional_trends' in results and not results['regional_trends'].empty:
            regional_path = os.path.join(output_dir, 'time_series_regional_trends.csv')
            results['regional_trends'].to_csv(regional_path, index=False)
            saved_files['regional_trends'] = regional_path
            logger.info(f"Saved regional trends to {regional_path}")
        
        # 5. Save summary statistics
        if 'summary_statistics' in results:
            summary_path = os.path.join(output_dir, 'time_series_summary.csv')
            
            # Convert nested summary to flat DataFrame
            summary_data = []
            stats = results['summary_statistics']
            
            # Analysis period
            for key, value in stats['analysis_period'].items():
                summary_data.append({'category': 'analysis_period', 'metric': key, 'value': value})
            
            # Posting volume
            for key, value in stats['posting_volume'].items():
                summary_data.append({'category': 'posting_volume', 'metric': key, 'value': value})
            
            # Trends
            for key, value in stats['trends'].items():
                summary_data.append({'category': 'trends', 'metric': key, 'value': value})
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_csv(summary_path, index=False)
            saved_files['summary'] = summary_path
            logger.info(f"Saved time series summary to {summary_path}")
        
        logger.info(f"Successfully saved {len(saved_files)} time series analysis files")
        return saved_files
        
    except Exception as e:
        logger.error(f"Failed to save time series analysis results: {e}")
        return {'error': str(e)}


def analyze_time_series(jobs_df: pd.DataFrame) -> pd.DataFrame:
    """
    Perform time series analysis on job posting trends.
    
    DEPRECATED: Use implement_time_series_analysis() for Task 7.4 implementation.
    This function is kept for backward compatibility.
    
    Args:
        jobs_df: DataFrame with job postings
        
    Returns:
        DataFrame with time series analysis
    """
    logger.warning("Using deprecated analyze_time_series(). "
                  "Use implement_time_series_analysis() for Task 7.4.")
    
    if 'posting_date' not in jobs_df.columns:
        logger.warning("No posting_date column found for time series analysis")
        return pd.DataFrame()
    
    # Convert posting_date to datetime
    jobs_df = jobs_df.copy()
    jobs_df['posting_date'] = pd.to_datetime(jobs_df['posting_date'], errors='coerce')
    
    # Remove rows with invalid dates
    jobs_df = jobs_df.dropna(subset=['posting_date'])
    
    if jobs_df.empty:
        logger.warning("No valid posting dates found")
        return pd.DataFrame()
    
    # Group by week
    jobs_df['week'] = jobs_df['posting_date'].dt.to_period('W')
    weekly_counts = jobs_df.groupby('week').size().reset_index(name='job_count')
    weekly_counts['week'] = weekly_counts['week'].dt.start_time
    
    # Calculate 4-week rolling average
    weekly_counts = weekly_counts.sort_values('week')
    weekly_counts['rolling_avg_4week'] = weekly_counts['job_count'].rolling(window=4, min_periods=1).mean()
    
    # Calculate growth rates
    weekly_counts['week_over_week_growth'] = weekly_counts['job_count'].pct_change() * 100
    
    # Identify trending sectors (if role_category available)
    sector_trends = []
    if 'role_category' in jobs_df.columns:
        for role in jobs_df['role_category'].unique():
            role_jobs = jobs_df[jobs_df['role_category'] == role]
            role_weekly = role_jobs.groupby('week').size().reset_index(name='job_count')
            
            if len(role_weekly) >= 2:
                # Calculate trend (simple linear regression slope)
                x = np.arange(len(role_weekly))
                y = role_weekly['job_count'].values
                
                if len(x) > 1 and np.std(x) > 0:
                    slope = np.corrcoef(x, y)[0, 1] * (np.std(y) / np.std(x))
                    
                    sector_trends.append({
                        'role_category': role,
                        'trend_slope': slope,
                        'avg_weekly_jobs': y.mean(),
                        'trend_direction': 'Growing' if slope > 0 else 'Declining'
                    })
    
    # Combine results
    time_series_results = {
        'weekly_trends': weekly_counts,
        'sector_trends': pd.DataFrame(sector_trends) if sector_trends else pd.DataFrame()
    }
    
    logger.info(f"Analyzed {len(weekly_counts)} weeks of data")
    if sector_trends:
        logger.info(f"Analyzed trends for {len(sector_trends)} role categories")
    
    return time_series_results


def run_complete_analysis(
    jobs_df: pd.DataFrame,
    skills_df: Optional[pd.DataFrame] = None,
    ons_vacancies_df: Optional[pd.DataFrame] = None,
    ons_salaries_df: Optional[pd.DataFrame] = None
) -> Dict[str, Any]:
    """
    Run complete analysis pipeline.
    
    Args:
        jobs_df: DataFrame with job postings
        skills_df: Optional DataFrame with extracted skills
        ons_vacancies_df: Optional ONS vacancy data
        ons_salaries_df: Optional ONS salary data
        
    Returns:
        Dictionary with all analysis results
    """
    logger.info("Running complete analysis pipeline")
    
    results = {}
    
    # 1. Salary Prediction Model
    try:
        salary_predictor = SalaryPredictor()
        salary_metrics = salary_predictor.train(jobs_df)
        results['salary_model'] = {
            'predictor': salary_predictor,
            'metrics': salary_metrics
        }
    except Exception as e:
        logger.error(f"Salary prediction failed: {e}")
        results['salary_model'] = {'error': str(e)}
    
    # 2. Skill Demand Analysis
    if skills_df is not None and not skills_df.empty:
        try:
            skill_demand = analyze_skill_demand(skills_df, jobs_df)
            results['skill_demand'] = skill_demand
        except Exception as e:
            logger.error(f"Skill demand analysis failed: {e}")
            results['skill_demand'] = pd.DataFrame()
    else:
        logger.warning("No skills data provided for demand analysis")
        results['skill_demand'] = pd.DataFrame()
    
    # 3. Regional Opportunity Scoring
    try:
        regional_scores = calculate_regional_opportunity_score(
            jobs_df, ons_vacancies_df, ons_salaries_df
        )
        results['regional_opportunities'] = regional_scores
    except Exception as e:
        logger.error(f"Regional analysis failed: {e}")
        results['regional_opportunities'] = pd.DataFrame()
    
    # 4. Time Series Analysis
    try:
        time_series = analyze_time_series(jobs_df)
        results['time_series'] = time_series
    except Exception as e:
        logger.error(f"Time series analysis failed: {e}")
        results['time_series'] = {}
    
    logger.info("Complete analysis pipeline finished")
    
    return results


def load_cleaned_job_data(db_manager) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load cleaned job data and skills data from database.
    
    Args:
        db_manager: DatabaseManager instance
        
    Returns:
        Tuple of (jobs_df, skills_df)
    """
    logger.info("Loading cleaned job data from database")
    
    # Load job postings with skill counts
    jobs_query = """
    SELECT 
        jp.*,
        COALESCE(skill_counts.skills_count, 0) as skills_count
    FROM job_postings jp
    LEFT JOIN (
        SELECT 
            job_id, 
            COUNT(*) as skills_count
        FROM skills_extracted 
        GROUP BY job_id
    ) skill_counts ON jp.id = skill_counts.job_id
    WHERE jp.salary_avg IS NOT NULL
    AND jp.salary_avg > 0
    """
    
    with db_manager.get_connection() as conn:
        jobs_df = pd.read_sql(jobs_query, conn)
    logger.info(f"Loaded {len(jobs_df)} job postings with salary data")
    
    # Load skills data
    skills_query = """
    SELECT se.*, jp.role_category
    FROM skills_extracted se
    JOIN job_postings jp ON se.job_id = jp.id
    """
    
    with db_manager.get_connection() as conn:
        skills_df = pd.read_sql(skills_query, conn)
    logger.info(f"Loaded {len(skills_df)} skill records")
    
    return jobs_df, skills_df


def implement_salary_prediction_model(db_manager) -> Dict[str, Any]:
    """
    Implement salary prediction model as specified in task 7.1.
    
    This function:
    1. Loads cleaned job data from database
    2. Prepares features: region, role_category, contract_type, num_skills_required
    3. Encodes categorical variables using LabelEncoder
    4. Trains LinearRegression model with salary_avg as target
    5. Calculates feature importance and coefficients
    6. Saves model metrics and insights
    
    Args:
        db_manager: DatabaseManager instance
        
    Returns:
        Dictionary with model results and insights
    """
    logger.info("Implementing salary prediction model (Task 7.1)")
    
    try:
        # Load data
        jobs_df, skills_df = load_cleaned_job_data(db_manager)
        
        if jobs_df.empty:
            logger.error("No job data available for salary prediction")
            return {'error': 'No job data available'}
        
        # Initialize and train salary predictor
        salary_predictor = SalaryPredictor()
        
        # Train the model
        training_metrics = salary_predictor.train(jobs_df)
        
        if 'error' in training_metrics:
            logger.error(f"Training failed: {training_metrics['error']}")
            return training_metrics
        
        # Generate insights
        insights = {
            'model_performance': {
                'r2_score': training_metrics['test_r2'],
                'mean_absolute_error': training_metrics['test_mae'],
                'training_samples': training_metrics['training_samples'],
                'test_samples': training_metrics['test_samples']
            },
            'feature_importance': training_metrics['feature_importance'],
            'data_summary': {
                'total_jobs_analyzed': len(jobs_df),
                'avg_salary': jobs_df['salary_avg'].mean(),
                'salary_std': jobs_df['salary_avg'].std(),
                'unique_regions': jobs_df['region'].nunique(),
                'unique_roles': jobs_df['role_category'].nunique(),
                'unique_contract_types': jobs_df['contract_type'].nunique()
            }
        }
        
        # Log key findings
        logger.info("=== SALARY PREDICTION MODEL RESULTS ===")
        logger.info(f"Model R² Score: {insights['model_performance']['r2_score']:.3f}")
        logger.info(f"Mean Absolute Error: £{insights['model_performance']['mean_absolute_error']:,.0f}")
        logger.info(f"Training Samples: {insights['model_performance']['training_samples']}")
        
        logger.info("\n=== FEATURE IMPORTANCE (Coefficients) ===")
        for feature, importance in insights['feature_importance'].items():
            logger.info(f"{feature}: £{importance:,.0f}")
        
        logger.info("\n=== DATA SUMMARY ===")
        logger.info(f"Total jobs analyzed: {insights['data_summary']['total_jobs_analyzed']:,}")
        logger.info(f"Average salary: £{insights['data_summary']['avg_salary']:,.0f}")
        logger.info(f"Salary standard deviation: £{insights['data_summary']['salary_std']:,.0f}")
        logger.info(f"Unique regions: {insights['data_summary']['unique_regions']}")
        logger.info(f"Unique role categories: {insights['data_summary']['unique_roles']}")
        
        # Save results to a summary file
        results_summary = {
            'timestamp': datetime.now().isoformat(),
            'task': '7.1 - Salary Prediction Model',
            'model_type': 'LinearRegression',
            'features_used': ['region', 'role_category', 'contract_type', 'skills_count'],
            'encoding_method': 'LabelEncoder',
            'target_variable': 'salary_avg',
            'results': insights
        }
        
        return results_summary
        
    except Exception as e:
        logger.error(f"Salary prediction model implementation failed: {e}")
        return {'error': str(e)}


# Example usage
if __name__ == "__main__":
    # Import database manager
    import sys
    import os
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'database'))
    
    from db_manager import create_db_manager
    
    logger.info("Analysis module loaded successfully")
    
    # Example of running salary prediction model
    try:
        db_manager = create_db_manager()
        results = implement_salary_prediction_model(db_manager)
        
        if 'error' not in results:
            print("\n=== SALARY PREDICTION MODEL COMPLETED ===")
            print(f"R² Score: {results['results']['model_performance']['r2_score']:.3f}")
            print(f"MAE: £{results['results']['model_performance']['mean_absolute_error']:,.0f}")
            print("\nFeature Importance:")
            for feature, importance in results['results']['feature_importance'].items():
                print(f"  {feature}: £{importance:,.0f}")
        else:
            print(f"Error: {results['error']}")
            
    except Exception as e:
        print(f"Failed to run salary prediction: {e}")
    
    # Example of running skill demand ranking analysis
    try:
        db_manager = create_db_manager()
        skill_results = implement_skill_demand_ranking_analysis(db_manager)
        
        if 'error' not in skill_results:
            print("\n=== SKILL DEMAND RANKING ANALYSIS COMPLETED ===")
            stats = skill_results['summary_statistics']
            print(f"Total job postings: {stats['total_job_postings']:,}")
            print(f"Unique skills found: {stats['total_unique_skills']:,}")
            print(f"Role categories: {stats['role_categories_analyzed']}")
            print("\nTop 5 Skills Overall:")
            for i, skill in enumerate(stats['top_10_skills_overall'][:5], 1):
                print(f"  {i}. {skill['skill_name']}: {skill['skill_demand_percent']:.1f}%")
        else:
            print(f"Skill analysis error: {skill_results['error']}")
            
    except Exception as e:
        print(f"Failed to run skill demand analysis: {e}")
    
    # Example of running regional opportunity scoring
    try:
        db_manager = create_db_manager()
        regional_results = implement_regional_opportunity_scoring(db_manager)
        
        if 'error' not in regional_results:
            print("\n=== REGIONAL OPPORTUNITY SCORING COMPLETED ===")
            stats = regional_results['summary_statistics']
            print(f"Total regions analyzed: {stats['total_regions_analyzed']}")
            print(f"Total job postings: {stats['total_job_postings']:,}")
            print(f"Average opportunity score: {stats['avg_opportunity_score']}")
            print(f"Top region: {stats['top_region']['name']} (score: {stats['top_region']['score']})")
            print("\nTop 5 Regions:")
            for i, region in enumerate(regional_results['regional_scores'].head().itertuples(), 1):
                print(f"  {i}. {region.region}: {region.opportunity_score:.1f} "
                      f"(Vol: {region.vacancy_volume}, Salary: £{region.avg_salary:,.0f})")
        else:
            print(f"Regional scoring error: {regional_results['error']}")
            
    except Exception as e:
        print(f"Failed to run regional opportunity scoring: {e}")
    
    # Example of running time series analysis
    try:
        db_manager = create_db_manager()
        time_series_results = implement_time_series_analysis(db_manager)
        
        if 'error' not in time_series_results:
            print("\n=== TIME SERIES ANALYSIS COMPLETED ===")
            stats = time_series_results['summary_statistics']
            print(f"Analysis period: {stats['analysis_period']['start_date']} to {stats['analysis_period']['end_date']}")
            print(f"Total jobs analyzed: {stats['posting_volume']['total_jobs']:,}")
            print(f"Total weeks: {stats['analysis_period']['total_weeks']}")
            print(f"Average weekly jobs: {stats['posting_volume']['avg_weekly_jobs']}")
            print(f"Growing sectors: {stats['trends']['growing_sectors']}")
            print(f"Declining sectors: {stats['trends']['declining_sectors']}")
            
            if not time_series_results['sector_trends'].empty:
                print("\nTop Growing Sectors:")
                growing = time_series_results['sector_trends'][
                    time_series_results['sector_trends']['trend_direction'] == 'Growing'
                ].head(3)
                for i, sector in enumerate(growing.itertuples(), 1):
                    print(f"  {i}. {sector.role_category}: {sector.total_change_percent:+.1f}% change")
        else:
            print(f"Time series analysis error: {time_series_results['error']}")
            
    except Exception as e:
        print(f"Failed to run time series analysis: {e}")
    
    print("\nAvailable analysis functions:")
    print("- SalaryPredictor: Machine learning salary prediction")
    print("- analyze_skill_demand: Skill demand ranking")
    print("- calculate_regional_opportunity_score: Regional opportunity analysis (deprecated)")
    print("- analyze_time_series: Time series trend analysis (deprecated)")
    print("- run_complete_analysis: Complete analysis pipeline")
    print("- implement_salary_prediction_model: Task 7.1 implementation")
    print("- implement_skill_demand_ranking_analysis: Task 7.2 implementation")
    print("- implement_regional_opportunity_scoring: Task 7.3 implementation")
    print("- implement_time_series_analysis: Task 7.4 implementation")