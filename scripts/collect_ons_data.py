"""
Collect ONS (Office for National Statistics) data.

This script downloads publicly available ONS datasets that don't require authentication:
- VACS01: Vacancies by Industry
- EARN01: Average Weekly Earnings
- Regional Labour Market data (NUTS1 regions)

These datasets provide valuable context for job market analysis.
"""

import sys
import os

# Add scripts directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import using importlib to handle numeric prefix
import importlib.util
module_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "01_collect.py")
spec = importlib.util.spec_from_file_location("collect", module_path)
collect = importlib.util.module_from_spec(spec)
spec.loader.exec_module(collect)

# Import ONS functions
fetch_ons_vacs01 = collect.fetch_ons_vacs01
fetch_ons_earn01 = collect.fetch_ons_earn01
fetch_ons_regional_labour_market = collect.fetch_ons_regional_labour_market


def main():
    """
    Provide instructions for downloading ONS datasets.
    
    Note: ONS has changed their data distribution system and direct
    download URLs are no longer available. This script provides
    instructions for manual download.
    """
    print("\n" + "="*60)
    print("ONS DATA COLLECTION - MANUAL DOWNLOAD REQUIRED")
    print("="*60)
    print("\nONS has changed their data distribution system.")
    print("Direct download URLs are no longer available.")
    print("\nPlease follow the instructions below to download manually:")
    print("="*60)
    
    output_dir = "data/raw"
    
    # VACS01 Instructions
    print("\n📊 1. VACS01 - Vacancies by Industry")
    print("-" * 60)
    print("Visit: https://www.ons.gov.uk/employmentandlabourmarket/")
    print("       peoplenotinwork/unemployment/datasets/")
    print("       vacanciesandunemploymentvacs01")
    print("\nSteps:")
    print("  1. Click the 'Download' button")
    print("  2. Save as: data/raw/ons_vacs01_[date].xlsx or .csv")
    print("  3. If Excel format, convert to CSV")
    
    # EARN01 Instructions
    print("\n💰 2. EARN01 - Average Weekly Earnings")
    print("-" * 60)
    print("Visit: https://www.ons.gov.uk/employmentandlabourmarket/")
    print("       peopleinwork/earningsandworkinghours/datasets/")
    print("       averageweeklyearningsearn01")
    print("\nSteps:")
    print("  1. Click the 'Download' button")
    print("  2. Save as: data/raw/ons_earn01_[date].xlsx or .csv")
    print("  3. If Excel format, convert to CSV")
    
    # Regional Labour Market Instructions
    print("\n🗺️  3. Regional Labour Market Data")
    print("-" * 60)
    print("Visit: https://www.ons.gov.uk/employmentandlabourmarket/")
    print("       peopleinwork/employmentandemployeetypes/bulletins/")
    print("       regionallabourmarket/latest")
    print("\nSteps:")
    print("  1. Scroll to 'Data downloads' section")
    print("  2. Download regional summary tables")
    print("  3. Save as: data/raw/ons_regional_[date].xlsx or .csv")
    print("  4. If Excel format, convert to CSV")
    
    # Alternative - Nomis Web
    print("\n" + "="*60)
    print("ALTERNATIVE: Use Nomis Web (Easier)")
    print("="*60)
    print("\nNomis provides easier access to ONS data:")
    print("\n1. Visit: https://www.nomisweb.co.uk/")
    print("2. Search for:")
    print("   - 'vacancies by industry' (vsic)")
    print("   - 'workforce jobs' (wfjsa)")
    print("   - 'employment by industry' (emp13)")
    print("3. Select your parameters (geography, time period)")
    print("4. Download as CSV")
    print("5. Save to data/raw/")
    
    # Summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print("\nONS data is optional for this project.")
    print("You already have 1,454 job postings from Adzuna and Reed.")
    print("\nONS data would add:")
    print("  ✓ Economic context (vacancy trends)")
    print("  ✓ Salary benchmarks")
    print("  ✓ Regional employment statistics")
    print("\nBut it's NOT required to complete the project.")
    print("\nRecommendation: Skip ONS for now and proceed with:")
    print("  1. Data cleaning (Task 5)")
    print("  2. Analysis (Task 7)")
    print("  3. Power BI dashboard (Task 10)")
    print("\n" + "="*60)
    
    return {
        'vacs01': False,
        'earn01': False,
        'regional': False,
        'manual_download_required': True
    }


if __name__ == "__main__":
    main()
