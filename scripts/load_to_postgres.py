"""
load_to_postgres.py
Loads data/transactions.csv into the raw.transactions table in Postgres.
Idempotent: truncates the table before each load.
"""

import csv
import os
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "brex_spend"),
    "user": os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
}

DDL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.transactions (
    transaction_id     TEXT PRIMARY KEY,
    transaction_date   DATE NOT NULL,
    employee_id        TEXT,
    employee_name      TEXT,
    email              TEXT,
    department         TEXT,
    category           TEXT,
    merchant           TEXT,
    amount_usd         NUMERIC(12, 2),
    currency           TEXT,
    policy_limit_usd   NUMERIC(12, 2),
    policy_violation   BOOLEAN,
    notes              TEXT,
    ai_flagged         BOOLEAN DEFAULT FALSE,
    ai_flag_reason     TEXT,
    loaded_at          TIMESTAMPTZ DEFAULT NOW()
);
"""

INSERT_SQL = """
INSERT INTO raw.transactions (
    transaction_id, transaction_date, employee_id, employee_name,
    email, department, category, merchant, amount_usd, currency,
    policy_limit_usd, policy_violation, notes
) VALUES %s
ON CONFLICT (transaction_id) DO NOTHING;
"""


def load(csv_path: Path):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute(DDL)
    cur.execute("TRUNCATE raw.transactions;")

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        rows = [
            (
                r["transaction_id"],
                r["transaction_date"],
                r["employee_id"],
                r["employee_name"],
                r["email"],
                r["department"],
                r["category"],
                r["merchant"],
                float(r["amount_usd"]),
                r["currency"],
                float(r["policy_limit_usd"]),
                r["policy_violation"].lower() == "true",
                r["notes"],
            )
            for r in reader
        ]

    execute_values(cur, INSERT_SQL, rows)
    conn.commit()
    print(f"Loaded {len(rows)} rows into raw.transactions")
    cur.close()
    conn.close()


if __name__ == "__main__":
    csv_path = Path(__file__).parent.parent / "data" / "transactions.csv"
    load(csv_path)
