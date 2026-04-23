"""
Complete Data Pipeline Runner
Processes raw CSV data through cleaning, analysis, and export stages
"""

import sys
import os
import pandas as pd
import glob
from datetime import datetime

# Add paths for imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'database'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'scripts'))

from db_manager import create_db_manager

# Import functions from scripts
import importlib.util

# Load 02_clean.py
spec_clean = importlib.util.spec_from_file_location("clean_module", "scripts/02_clean.py")
clean_module = importlib.util.module_from_spec(spec_clean)
spec_clean.loader.exec_module(clean_module)

# Load 03_analyse.py
spec_analyse = importlib.util.spec_from_file_location("analyse_module", "scripts/03_analyse.py")
analyse_module = importlib.util.module_from_spec(spec_analyse)
spec_analyse.loader.exec_module(analyse_module)

# Load 04_export.py
spec_export = importlib.util.spec_from_file_location("export_module", "scripts/04_export.py")
export_module = importlib.util.module_from_spec(spec_export)
spec_export.loader.exec_module(export_module)

print("="*80)
print("UK JOB MARKET INTELLIGENCE PLATFORM - DATA PIPELINE")
print("="*80)

# Step 0: Initialize database schema
print("\n[STEP 0] Initializing database schema...")
print("-"*80)

db_manager = create_db_manager()

# Check if tables exist, if not create them
if not db_manager.table_exists('job_postings'):
    print("  Creating database schema...")
    try:
        db_manager.execute_sql_file('database/schema_sqlite.sql')
        print("  [OK] Database schema created successfully")
    except Exception as e:
        print(f"  [ERROR] Error creating schema: {e}")
        sys.exit(1)
else:
    print("  [OK] Database schema already exists")
    # Clear existing data for fresh run
    print("  Clearing existing data...")
    try:
        with db_manager.get_connection() as conn:
            conn.execute("DELETE FROM skills_extracted")
            conn.execute("DELETE FROM job_postings")
            conn.commit()
        print("  [OK] Existing data cleared")
    except Exception as e:
        print(f"  [WARN] Warning: Could not clear data: {e}")

# Step 1: Load raw CSV data into database
print("\n[STEP 1] Loading raw CSV data...")
print("-"*80)

db_manager = create_db_manager()

# Find all raw CSV files
raw_files = glob.glob('data/raw/*.csv')
print(f"Found {len(raw_files)} raw data files")

all_jobs = []
for file_path in raw_files:
    print(f"  Loading: {os.path.basename(file_path)}")
    try:
        df = pd.read_csv(file_path)
        
        # Determine source from filename
        if 'adzuna' in file_path.lower():
            df['source'] = 'adzuna'
        elif 'reed' in file_path.lower():
            df['source'] = 'reed'
        else:
            df['source'] = 'unknown'
        
        all_jobs.append(df)
        print(f"    [OK] Loaded {len(df)} records")
    except Exception as e:
        print(f"    [ERROR] Error loading {file_path}: {e}")

if not all_jobs:
    print("\n[ERROR] No data loaded. Exiting.")
    sys.exit(1)

# Combine all data
combined_df = pd.concat(all_jobs, ignore_index=True)
print(f"\n[OK] Total records loaded: {len(combined_df)}")

# Step 2: Clean and enhance data
print("\n[STEP 2] Cleaning and enhancing data...")
print("-"*80)

try:
    cleaned_df = clean_module.clean_jobs_enhanced(
        combined_df,
        include_regions=True,
        include_roles=True
    )
    print(f"[OK] Cleaned {len(cleaned_df)} records")
    print(f"  - Regions standardized: {cleaned_df['region'].notna().sum()}")
    print(f"  - Roles classified: {cleaned_df['role_category'].notna().sum()}")
except Exception as e:
    print(f"[ERROR] Error cleaning data: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Step 3: Insert into database
print("\n[STEP 3] Inserting cleaned data into database...")
print("-"*80)

try:
    # Prepare data for database insertion
    # Replace NaN values with None for database compatibility
    cleaned_df_for_db = cleaned_df.replace({pd.NA: None, pd.NaT: None})
    cleaned_df_for_db = cleaned_df_for_db.where(pd.notnull(cleaned_df_for_db), None)
    
    job_records = cleaned_df_for_db.to_dict('records')
    
    # Insert job postings
    db_manager.bulk_insert('job_postings', job_records)
    print(f"[OK] Inserted {len(job_records)} job postings")
    
    # Extract and insert skills
    print("\n  Extracting skills from job descriptions...")
    
    # Fetch the inserted jobs with their IDs from database
    from sqlalchemy import text
    with db_manager.get_connection() as conn:
        result = conn.execute(text("SELECT * FROM job_postings"))
        jobs_with_ids = pd.DataFrame(result.fetchall(), columns=result.keys())
    
    skills_df = clean_module.extract_skills_from_dataframe(jobs_with_ids)
    
    if not skills_df.empty:
        clean_module.insert_skills_to_database(skills_df, db_manager)
        print(f"  [OK] Inserted {len(skills_df)} skill records")
    else:
        print("  [WARN] No skills extracted")
        
except Exception as e:
    print(f"[ERROR] Error inserting data: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Step 4: Run analysis
print("\n[STEP 4] Running analysis...")
print("-"*80)

# 4.1 Salary Prediction
print("\n  [4.1] Salary Prediction Model...")
try:
    salary_results = analyse_module.implement_salary_prediction_model(db_manager)
    if 'error' not in salary_results:
        metrics = salary_results['results']['model_performance']
        print(f"    [OK] R² Score: {metrics['r2_score']:.3f}")
        print(f"    [OK] MAE: £{metrics['mean_absolute_error']:,.0f}")
    else:
        print(f"    [ERROR] Error: {salary_results['error']}")
except Exception as e:
    print(f"    [ERROR] Failed: {e}")

# 4.2 Skill Demand Analysis
print("\n  [4.2] Skill Demand Ranking...")
try:
    skill_results = analyse_module.implement_skill_demand_ranking_analysis(db_manager)
    if 'error' not in skill_results:
        stats = skill_results['summary_statistics']
        print(f"    [OK] Total jobs: {stats['total_job_postings']:,}")
        print(f"    [OK] Unique skills: {stats['total_unique_skills']}")
        print(f"    [OK] Top skill: {stats['top_10_skills_overall'][0]['skill_name']} "
              f"({stats['top_10_skills_overall'][0]['skill_demand_percent']:.1f}%)")
    else:
        print(f"    [ERROR] Error: {skill_results['error']}")
except Exception as e:
    print(f"    [ERROR] Failed: {e}")

# 4.3 Regional Opportunity Scoring
print("\n  [4.3] Regional Opportunity Scoring...")
try:
    regional_results = analyse_module.implement_regional_opportunity_scoring(db_manager)
    if 'error' not in regional_results:
        stats = regional_results['summary_statistics']
        print(f"    [OK] Regions analyzed: {stats['total_regions_analyzed']}")
        print(f"    [OK] Top region: {stats['top_region']['name']} "
              f"(score: {stats['top_region']['score']})")
    else:
        print(f"    [ERROR] Error: {regional_results['error']}")
except Exception as e:
    print(f"    [ERROR] Failed: {e}")

# 4.4 Time Series Analysis
print("\n  [4.4] Time Series Analysis...")
try:
    time_results = analyse_module.implement_time_series_analysis(db_manager)
    if 'error' not in time_results:
        stats = time_results['summary_statistics']
        print(f"    [OK] Period: {stats['analysis_period']['start_date']} to "
              f"{stats['analysis_period']['end_date']}")
        print(f"    [OK] Total jobs: {stats['posting_volume']['total_jobs']:,}")
        print(f"    [OK] Growing sectors: {stats['trends']['growing_sectors']}")
    else:
        print(f"    [ERROR] Error: {time_results['error']}")
except Exception as e:
    print(f"    [ERROR] Failed: {e}")

# Step 5: Export data
print("\n[STEP 5] Exporting data...")
print("-"*80)

try:
    exported_files = export_module.export_all_for_powerbi(db_manager)
    print(f"[OK] Exported {len(exported_files)} files:")
    for file_type, file_path in exported_files.items():
        print(f"    - {file_type}: {file_path}")
except Exception as e:
    print(f"[ERROR] Error exporting data: {e}")
    import traceback
    traceback.print_exc()

# Summary
print("\n" + "="*80)
print("PIPELINE COMPLETED SUCCESSFULLY")
print("="*80)
print(f"\nData Summary:")
print(f"  - Raw records processed: {len(combined_df)}")
print(f"  - Cleaned records: {len(cleaned_df)}")
print(f"  - Data sources: {', '.join(combined_df['source'].unique())}")
print(f"\nNext Steps:")
print(f"  - Exported data available in data/cleaned/ directory")
print(f"  - Use the CSV files for further analysis or visualization")
print("\n" + "="*80)
