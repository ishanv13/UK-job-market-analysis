# UK Job Market Analysis

A data pipeline for collecting, processing, and analyzing UK job market data for graduate analyst roles.

## Features

- Collects job data from Adzuna and Reed APIs
- Stores data in PostgreSQL database
- Cleans and standardizes job postings
- Analyzes salary trends, skill demand, and regional opportunities
- Exports processed data for visualization

## Prerequisites

- Python 3.12+
- PostgreSQL 14+
- API keys for Adzuna and Reed

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd uk-job-market-analysis
```

2. Create and activate virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your API credentials and database connection string
```

5. Create database and schema:
```bash
createdb uk_jobs
psql -d uk_jobs -f database/schema.sql
```

## Usage

Run the complete pipeline:
```bash
python run_pipeline.py
```

Or run individual steps:
```bash
python scripts/01_collect.py  # Collect job data
python scripts/02_clean.py    # Clean and process data
python scripts/03_analyse.py  # Run analysis
python scripts/04_export.py   # Export results
```

## Project Structure

```
├── data/
│   ├── raw/              # Raw API data
│   └── cleaned/          # Processed data
├── database/
│   ├── schema.sql        # Database schema
│   └── db_manager.py     # Database utilities
├── scripts/
│   ├── 01_collect.py     # Data collection
│   ├── 02_clean.py       # Data cleaning
│   ├── 03_analyse.py     # Analysis
│   └── 04_export.py      # Export
├── .env.example          # Environment template
└── requirements.txt      # Dependencies
```

## Data Sources

- **Adzuna API**: Job postings and salary data
- **Reed API**: Job descriptions and details
- **ONS**: UK labour market statistics

## License

MIT License

## Acknowledgments

Job data provided by Adzuna and Reed. Labour market statistics from ONS (Open Government Licence v3.0).
