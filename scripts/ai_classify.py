"""
ai_classify.py
Uses the Anthropic Python SDK to batch-classify corporate card transactions
for policy violations. Processes 50 transactions at a time and writes
structured results (ai_flagged, ai_flag_reason) back to Postgres.
"""

import json
import os
import time
from typing import Any

import anthropic
import psycopg2
from psycopg2.extras import execute_values

BATCH_SIZE = 50
MODEL = "claude-opus-4-6"

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "brex_spend"),
    "user": os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
}

SYSTEM_PROMPT = """You are a corporate spend-compliance analyst.
Your job is to review corporate card transactions and flag any that violate
company policy or appear suspicious.

Policy rules:
1. Meals & Entertainment: max $150 per transaction. Flag anything above.
2. Travel & Lodging: must have a business purpose. Flag if notes are missing
   and amount > $500.
3. Advertising spend above $10,000 requires VP approval — flag for review.
4. Professional Services above $5,000 require a contract on file — flag for review.
5. Hardware purchases above $2,500 require IT approval — flag for review.
6. Office Supplies above $200 per transaction seem excessive — flag for review.
7. Software & SaaS above $1,000 should have a renewal justification — flag.
8. Any duplicate merchant + amount + date combination in the batch is suspicious.
9. Weekend transactions in non-travel categories are unusual — note but don't
   automatically flag.

Return ONLY a valid JSON array. Each element must have exactly these keys:
- "transaction_id": string
- "ai_flagged": boolean
- "ai_flag_reason": string (empty string "" if not flagged)
"""


def fetch_unclassified(conn, limit: int | None = None) -> list[dict]:
    cur = conn.cursor()
    sql = """
        SELECT transaction_id, transaction_date, employee_name, department,
               category, merchant, amount_usd, policy_limit_usd,
               policy_violation, notes
        FROM raw.transactions
        WHERE ai_flagged IS FALSE AND ai_flag_reason IS NULL
        ORDER BY transaction_date
    """
    if limit:
        sql += f" LIMIT {limit}"
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    cur.close()
    # Make dates JSON-serialisable
    for r in rows:
        if hasattr(r["transaction_date"], "isoformat"):
            r["transaction_date"] = r["transaction_date"].isoformat()
        r["amount_usd"] = float(r["amount_usd"])
        r["policy_limit_usd"] = float(r["policy_limit_usd"])
    return rows


def build_user_prompt(batch: list[dict]) -> str:
    return (
        "Classify the following transactions. "
        "Return a JSON array with one object per transaction.\n\n"
        + json.dumps(batch, indent=2)
    )


def classify_batch(client: anthropic.Anthropic, batch: list[dict]) -> list[dict[str, Any]]:
    """Send one batch to Claude and parse the response."""
    message = client.messages.create(
        model=MODEL,
        max_tokens=4096,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": build_user_prompt(batch)}],
    )

    raw = message.content[0].text.strip()

    # Strip markdown code fences if present
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]

    results: list[dict] = json.loads(raw)
    return results


def write_results(conn, results: list[dict[str, Any]]):
    cur = conn.cursor()
    execute_values(
        cur,
        """
        UPDATE raw.transactions AS t SET
            ai_flagged     = v.ai_flagged,
            ai_flag_reason = v.ai_flag_reason
        FROM (VALUES %s) AS v(transaction_id, ai_flagged, ai_flag_reason)
        WHERE t.transaction_id = v.transaction_id
        """,
        [(r["transaction_id"], r["ai_flagged"], r["ai_flag_reason"]) for r in results],
        template="(%s, %s::boolean, %s)",
    )
    conn.commit()
    cur.close()


def main():
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    conn = psycopg2.connect(**DB_CONFIG)

    transactions = fetch_unclassified(conn)
    total = len(transactions)
    print(f"Classifying {total} transactions in batches of {BATCH_SIZE}…")

    flagged = 0
    for start in range(0, total, BATCH_SIZE):
        batch = transactions[start : start + BATCH_SIZE]
        batch_num = start // BATCH_SIZE + 1
        print(f"  Batch {batch_num}: transactions {start + 1}–{start + len(batch)}")

        try:
            results = classify_batch(client, batch)
            write_results(conn, results)
            flagged += sum(1 for r in results if r.get("ai_flagged"))
        except (json.JSONDecodeError, KeyError) as e:
            print(f"    WARNING: parse error on batch {batch_num}: {e}. Skipping.")

        # Respect API rate limits
        if start + BATCH_SIZE < total:
            time.sleep(1)

    conn.close()
    print(f"\nDone. {flagged}/{total} transactions flagged by AI.")


if __name__ == "__main__":
    main()
