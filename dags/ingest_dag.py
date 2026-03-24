"""
ingest_dag.py
Airflow DAG: brex_spend_ingest
Schedule: daily at 06:00 UTC
Tasks (sequential):
  1. generate_data   → runs scripts/generate_data.py
  2. load_to_postgres → runs scripts/load_to_postgres.py
  3. ai_classify     → runs scripts/ai_classify.py
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def run_generate_data():
    import importlib.util

    spec = importlib.util.spec_from_file_location("generate_data", SCRIPTS_DIR / "generate_data.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.main()


def run_load_to_postgres():
    import importlib.util
    from pathlib import Path

    spec = importlib.util.spec_from_file_location(
        "load_to_postgres", SCRIPTS_DIR / "load_to_postgres.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    csv_path = SCRIPTS_DIR.parent / "data" / "transactions.csv"
    mod.load(csv_path)


def run_ai_classify():
    import importlib.util

    spec = importlib.util.spec_from_file_location("ai_classify", SCRIPTS_DIR / "ai_classify.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.main()


with DAG(
    dag_id="brex_spend_ingest",
    description="Generate → load → AI-classify Brex corporate card transactions",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * *",
    catchup=False,
    default_args=default_args,
    tags=["brex", "spend", "finance"],
) as dag:

    generate_data = PythonOperator(
        task_id="generate_data",
        python_callable=run_generate_data,
    )

    load_to_postgres = PythonOperator(
        task_id="load_to_postgres",
        python_callable=run_load_to_postgres,
    )

    ai_classify = PythonOperator(
        task_id="ai_classify",
        python_callable=run_ai_classify,
        env={
            **os.environ,
            "ANTHROPIC_API_KEY": os.getenv("ANTHROPIC_API_KEY", ""),
        },
    )

    generate_data >> load_to_postgres >> ai_classify
