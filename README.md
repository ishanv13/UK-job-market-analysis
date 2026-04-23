# 🇬🇧 UK Graduate Job Market Intelligence Platform

> *An end-to-end data analytics project — from live API collection to interactive Power BI dashboard — answering the question: **where are the best opportunities for graduate analysts in the UK right now?***

---

## 📌 Project Overview

The UK graduate job market is competitive and opaque. Most job seekers apply blindly without understanding where demand is actually growing, which skills are truly in demand, or which regions offer the best salary-to-opportunity ratio.

This project builds a complete, data-driven answer to that question — collecting live job postings from the UK's largest job boards, storing them in a structured relational database, running statistical analysis to surface patterns, and presenting findings through an interactive Power BI dashboard.

This is not a static dataset project. The pipeline is designed to pull fresh data on a scheduled basis, meaning the insights stay current as the market evolves.

---

## 🎯 Business Questions Answered

| # | Question |
|---|---|
| 1 | Which analyst roles (Data, Business, BI, Operations) have the highest current demand in the UK? |
| 2 | What is the average salary for each analyst role, and how does it vary by region? |
| 3 | Which UK regions offer the best combination of vacancy volume and salary? |
| 4 | What technical and soft skills appear most frequently in analyst job postings? |
| 5 | How has demand for analyst roles trended over the past 12 months? |
| 6 | Which industries are hiring the most analysts right now? |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    LAYER 1 — DATA COLLECTION                │
│   Adzuna API  │  Reed API  │  ONS / Nomis CSVs  │  Kaggle   │
└───────────────────────────┬─────────────────────────────────┘
                            │ Python (requests, pandas)
┌───────────────────────────▼─────────────────────────────────┐
│                 LAYER 2 — STORAGE & PROCESSING              │
│        PostgreSQL Database  │  ETL Pipeline  │  Scheduler   │
└───────────────────────────┬─────────────────────────────────┘
                            │ pandas, scikit-learn, nltk
┌───────────────────────────▼─────────────────────────────────┐
│                    LAYER 3 — ANALYSIS                       │
│  Salary Regression │ Skill Extraction │ Regional Clustering │
│  Time Series Trends │ Opportunity Scoring │ A/B Comparison  │
└───────────────────────────┬─────────────────────────────────┘
                            │ Power BI Desktop
┌───────────────────────────▼─────────────────────────────────┐
│                LAYER 4 — VISUALISATION & OUTPUT             │
│     4-Page Power BI Dashboard  │  GitHub  │  LinkedIn       │
└─────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Layer | Tools Used |
|---|---|
| Data Collection | Python, Requests, Adzuna API, Reed API |
| Data Storage | PostgreSQL, SQLAlchemy, psycopg2 |
| Data Processing | Python, pandas, NumPy |
| Analysis & Modelling | scikit-learn, statsmodels, NLTK |
| Scheduling | schedule, python-dotenv |
| Visualisation | Power BI Desktop, DAX |
| Version Control | Git, GitHub |

---

## 📂 Repository Structure

```
uk-job-market/
│
├── data/
│   ├── raw/                    ← Raw API responses and ONS CSVs
│   └── cleaned/                ← Processed, analysis-ready files
│       ├── job_postings_clean.csv
│       ├── skills_demand.csv
│       ├── regional_summary.csv
│       └── weekly_trends.csv
│
├── scripts/
│   ├── 01_collect.py           ← API data collection pipeline
│   ├── 02_clean.py             ← Cleaning, normalisation, classification
│   ├── 03_analyse.py           ← Statistical analysis and modelling
│   └── 04_export.py            ← Export to Power BI-ready format
│
├── database/
│   └── schema.sql              ← Full PostgreSQL schema
│
├── dashboard/
│   └── uk_jobs.pbix            ← Power BI dashboard file
│
├── notebooks/
│   └── exploratory_analysis.ipynb  ← EDA and model development
│
├── screenshots/
│   ├── page1_overview.png
│   ├── page2_roles.png
│   ├── page3_regional.png
│   └── page4_insights.png
│
├── requirements.txt
└── README.md
```

---

## 🔌 Data Sources

| Source | What It Provides | Access |
|---|---|---|
| [Adzuna API](https://developer.adzuna.com/) | Live UK job postings — title, salary, location, description, company | Free, 250 req/day |
| [Reed API](https://www.reed.co.uk/developers/jobseeker) | Live UK postings with salary and contract type | Free, registration required |
| [ONS Labour Market Statistics](https://www.ons.gov.uk/employmentandlabourmarket) | Vacancy counts by industry, unemployment rates, regional breakdowns | Free, CSV download |
| [Nomis](https://www.nomisweb.co.uk/) | Granular regional and occupational labour market data | Free, no registration |
| [Kaggle UK Job Postings](https://www.kaggle.com/) | Historical postings dataset for skill frequency analysis | Free, account required |

---

## 🗄️ Database Schema

The PostgreSQL database is structured across four normalised tables:

```sql
job_postings        -- Core postings: title, company, region, salary, date
skills_extracted    -- Skills parsed from each job description
ons_vacancies       -- ONS vacancy counts by industry and period
ons_salaries        -- ONS salary benchmarks by occupation and region
```

**Relationship diagram:**

```
job_postings (id) ──< skills_extracted (posting_id)
ons_vacancies     (standalone — joined in analysis by region/period)
ons_salaries      (standalone — joined in analysis by occupation/region)
```

See [`database/schema.sql`](database/schema.sql) for the full schema with column definitions, constraints, and indexes.

---

## ⚙️ How to Run This Project

### 1. Clone the repository
```bash
git clone https://github.com/ishanv13/uk-job-market.git
cd uk-job-market
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Set up environment variables
Create a `.env` file in the root directory:
```
ADZUNA_APP_ID=your_app_id
ADZUNA_API_KEY=your_api_key
REED_API_KEY=your_reed_key
DB_HOST=localhost
DB_NAME=uk_jobs
DB_USER=your_username
DB_PASSWORD=your_password
```

### 4. Set up the PostgreSQL database
```bash
psql -U your_username -f database/schema.sql
```

### 5. Run the pipeline
```bash
# Step 1 — collect data from APIs
python scripts/01_collect.py

# Step 2 — clean and process
python scripts/02_clean.py

# Step 3 — run analysis and modelling
python scripts/03_analyse.py

# Step 4 — export to Power BI-ready CSVs
python scripts/04_export.py
```

### 6. Open the dashboard
Open `dashboard/uk_jobs.pbix` in Power BI Desktop. Refresh the data source to point to your local `data/cleaned/` folder.

---

## 📊 Dashboard Pages

### Page 1 — Executive Overview
A high-level snapshot of the entire UK analyst job market at a glance.

- **KPI cards** — Total postings collected, average analyst salary, top hiring region, fastest growing role
- **Line chart** — Weekly posting volume with 4-week rolling average overlay
- **Bar chart** — Vacancy distribution across role categories
- **Slicers** — Filter by date range, contract type, region

---

### Page 2 — Role & Skills Deep Dive
Drilling into what each analyst role looks like and what it demands.

- **Clustered bar chart** — Average salary by role category
- **Horizontal bar chart** — Top 15 most in-demand skills across all analyst postings
- **Stacked bar** — Skill demand split by role type (Data Analyst vs Business Analyst vs BI Analyst)
- **Table** — Top 20 hiring companies with posting count and average offered salary

---

### Page 3 — Regional Intelligence
Understanding where opportunity is geographically concentrated.

- **Filled UK map** — Regions coloured by composite Opportunity Score
- **Scatter plot** — Salary vs vacancy volume by region (best-value regions appear top-right)
- **Bar chart** — Top 10 cities by analyst posting volume
- **Bar chart** — Average salary by region, benchmarked against UK median

---

### Page 4 — Insights & Recommendations
Translating the data into clear, actionable conclusions.

- **Three key finding callouts** — Written as business insights, not data observations
- **Bar chart** — Entry-level vs experienced role split across sectors
- **Opportunity matrix** — Sectors ranked by salary and volume simultaneously
- **Recommendation panel** — Top 3 actions for a graduate analyst job seeker based on the data

---

## 📈 Key DAX Measures

```dax
-- Average salary across filtered context
Avg Salary = AVERAGE(job_postings[salary_avg])

-- Year-on-year posting volume change
YoY Change % =
DIVIDE(
    CALCULATE([Total Postings], YEAR('Date'[Date]) = 2025) -
    CALCULATE([Total Postings], YEAR('Date'[Date]) = 2024),
    CALCULATE([Total Postings], YEAR('Date'[Date]) = 2024)
) * 100

-- 4-week rolling average of posting volume
4-Week Rolling Avg =
AVERAGEX(
    DATESINPERIOD('Date'[Date], LASTDATE('Date'[Date]), -28, DAY),
    [Total Postings]
)

-- Composite regional opportunity score
Opportunity Score =
([Total Postings] * 0.4) +
(DIVIDE([Avg Salary], 1000) * 0.4) +
([YoY Change %] * 0.2)

-- Analyst role share of all UK postings
Analyst Role Share % =
DIVIDE([Analyst Vacancies], [Total Vacancies]) * 100
```

---

## 🔍 Analysis Methodology

### Salary Prediction Model
A linear regression model trained on role category, region, contract type, and number of skills required. Used to quantify how much each variable contributes to salary — for example, how much more London pays vs the North West for equivalent roles.

### Skill Extraction
A regex-based keyword matching approach applied to all job description text, categorising skills into three buckets: technical tools (SQL, Python, Power BI), analytical methods (regression, A/B testing, EDA), and soft skills (stakeholder management, communication). Frequency counts are normalised as a percentage of total postings.

### Regional Opportunity Scoring
A composite index built from three weighted components: vacancy volume (40%), average offered salary (40%), and year-on-year growth rate (20%). This produces a single comparable score per region, enabling straightforward ranking.

### Time Series Analysis
Weekly posting volumes aggregated and smoothed using a 4-week rolling average to reduce noise. Seasonal patterns and trend direction identified for each role category individually.

---

## 💡 Key Findings

> *These findings are based on data collected between [start date] and [end date]. Numbers will differ if you run the pipeline at a later date — that's the point of building a live system.*

1. **[Finding 1]** — e.g. SQL and Excel remain the most universally required skills, appearing in over 80% of all analyst postings regardless of role type
2. **[Finding 2]** — e.g. The North West and Midlands show the fastest year-on-year growth in analyst hiring, despite London still leading in absolute volume
3. **[Finding 3]** — e.g. Business Analyst roles offer a narrower salary range but significantly higher entry-level volume than Data Analyst roles

*Replace the above with your actual findings once the pipeline has run.*

---

## 🚀 Potential Extensions

- **Sentiment analysis** on job descriptions to classify role culture (collaborative vs independent, fast-paced vs structured)
- **Automated weekly email report** summarising the latest market shifts
- **Streamlit web app** to make the dashboard publicly accessible without Power BI Desktop
- **Company size enrichment** via Companies House API to understand whether SMEs or large enterprises drive more analyst hiring
- **Salary negotiation model** — predicting salary ceiling based on skills listed in a CV vs a target role

---

## 👤 About

Built by **Ishan Verma** — MSc Business Analytics, Warwick Business School.

Targeting data analyst, business analyst, and commercial analyst roles in the UK.

[![LinkedIn](https://img.shields.io/badge/LinkedIn-ishanv13-blue?style=flat&logo=linkedin)](https://linkedin.com/in/ishanv13)
[![GitHub](https://img.shields.io/badge/GitHub-ishanv13-black?style=flat&logo=github)](https://github.com/ishanv13)

---

## 📄 Licence

This project is open source under the [MIT Licence](LICENSE).

---

*If you found this project useful or have suggestions, feel free to open an issue or connect on LinkedIn.*
