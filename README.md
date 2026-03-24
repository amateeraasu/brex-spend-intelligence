# Brex Spend Intelligence Pipeline

A full-stack analytics project simulating a corporate card spend platform, built as a portfolio project targeting Brex Data Analyst II.

## Architecture
```
CSV (Faker) → Python → PostgreSQL/Supabase (raw) → dbt → mart.spend_mart → Looker Studio
                                    ↑
                              Airflow DAGs
                                    ↑
                            Claude API (ai_classify)
```

## Tech Stack
- Python + Faker — synthetic data generation (5,000 transactions)
- PostgreSQL / Supabase — cloud data warehouse
- dbt — staging and mart layer transformations
- Apache Airflow — pipeline orchestration
- Claude API (Anthropic) — AI-powered policy flag classification
- Looker Studio — BI dashboard (spend trends, category breakdown, policy flags, MoM growth)

## Setup

### 1. Install dependencies
```bash
pip install faker psycopg2-binary anthropic dbt-postgres
```

### 2. Generate and load data
```bash
python scripts/generate_data.py
python scripts/load_to_postgres.py
```

### 3. Run dbt models
```bash
cd dbt
dbt run --profiles-dir .
dbt test --profiles-dir .
```

### 4. Run full pipeline via Airflow
```bash
docker compose up -d
# UI at http://localhost:8080 (admin/admin)
```

### 5. Connect Looker Studio
PostgreSQL connector → Custom Query:
```sql
SELECT * FROM public_mart.spend_mart
```

## Dashboard Views
- Monthly spend trend by department (line chart)
- Spend by category breakdown (bar chart)
- Policy violation rate by department (table)
- MoM spend change % (scorecard)

- 
## 📊 Business Intelligence (Looker Studio)
[View Live Interactive Dashboard](https://lookerstudio.google.com/s/oV_FTOqhHSo)

![Dashboard Preview] <img width="1162" height="865" alt="Screenshot 2026-03-24 at 12 54 13" src="https://github.com/user-attachments/assets/b5bc6a9c-488a-4152-8d3b-3e695b443b61" />

### Key Insights Captured:
* **Anomaly Detection:** AI-powered classification of policy-breaking spend.
* **Departmental Efficiency:** MoM growth tracking to identify budget overruns.
