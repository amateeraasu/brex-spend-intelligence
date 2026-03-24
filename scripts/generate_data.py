"""
generate_data.py
Generates 5000 realistic corporate card transactions (Jan–Jun 2024)
using Faker and writes them to data/transactions.csv.
"""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

fake = Faker()
random.seed(42)

DEPARTMENTS = ["Engineering", "Marketing", "Sales", "Finance"]

CATEGORIES = [
    "Software & SaaS",
    "Travel & Lodging",
    "Meals & Entertainment",
    "Office Supplies",
    "Advertising",
    "Professional Services",
    "Hardware",
]

# Per-category spend ranges (min, max in USD)
SPEND_RANGES = {
    "Software & SaaS": (29, 2500),
    "Travel & Lodging": (150, 4000),
    "Meals & Entertainment": (12, 500),
    "Office Supplies": (5, 300),
    "Advertising": (500, 15000),
    "Professional Services": (250, 8000),
    "Hardware": (50, 3000),
}

# Department → category affinity weights
DEPT_CATEGORY_WEIGHTS = {
    "Engineering": [4, 1, 1, 1, 0, 1, 3],
    "Marketing": [2, 2, 2, 1, 5, 2, 1],
    "Sales": [1, 4, 4, 1, 2, 1, 0],
    "Finance": [2, 1, 1, 2, 0, 3, 1],
}

MERCHANTS = {
    "Software & SaaS": [
        "GitHub", "Datadog", "Snowflake", "Figma", "Notion",
        "Zoom", "Slack", "AWS Marketplace", "Heroku", "Jira Cloud",
    ],
    "Travel & Lodging": [
        "Delta Airlines", "United Airlines", "Marriott", "Hilton",
        "Airbnb", "Uber", "Lyft", "Enterprise Rent-A-Car", "AmericanAirlines",
    ],
    "Meals & Entertainment": [
        "DoorDash", "Seamless", "Grubhub", "OpenTable Restaurant",
        "Starbucks", "Chipotle", "Local Bistro", "Conference Catering Co.",
    ],
    "Office Supplies": [
        "Staples", "Office Depot", "Amazon Business", "Uline", "Grainger",
    ],
    "Advertising": [
        "Google Ads", "Meta Ads", "LinkedIn Ads", "Twitter/X Ads",
        "AdRoll", "The Trade Desk",
    ],
    "Professional Services": [
        "Deloitte Consulting", "Accenture", "McKinsey & Co",
        "Local Law Firm LLP", "Freelancer Invoice",
    ],
    "Hardware": [
        "Apple Business", "Dell Technologies", "B&H Photo", "CDW",
        "Best Buy Business",
    ],
}

# Policy limits (per transaction)
POLICY_LIMITS = {
    "Meals & Entertainment": 150,
    "Travel & Lodging": 3000,
    "Software & SaaS": 1000,
    "Advertising": 10000,
    "Professional Services": 5000,
    "Office Supplies": 200,
    "Hardware": 2500,
}

START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2024, 6, 30)


def random_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))


def generate_employee(dept: str) -> dict:
    first = fake.first_name()
    last = fake.last_name()
    return {
        "employee_id": f"EMP-{random.randint(1000, 9999)}",
        "employee_name": f"{first} {last}",
        "email": f"{first.lower()}.{last.lower()}@brex-demo.com",
        "department": dept,
    }


def generate_transactions(n: int = 5000) -> list[dict]:
    employees = {dept: [generate_employee(dept) for _ in range(10)] for dept in DEPARTMENTS}
    transactions = []

    for i in range(1, n + 1):
        dept = random.choice(DEPARTMENTS)
        emp = random.choice(employees[dept])
        category = random.choices(CATEGORIES, weights=DEPT_CATEGORY_WEIGHTS[dept])[0]
        lo, hi = SPEND_RANGES[category]
        amount = round(random.uniform(lo, hi), 2)
        merchant = random.choice(MERCHANTS[category])
        txn_date = random_date(START_DATE, END_DATE)
        policy_limit = POLICY_LIMITS[category]
        policy_violation = amount > policy_limit

        transactions.append(
            {
                "transaction_id": f"TXN-{i:05d}",
                "transaction_date": txn_date.strftime("%Y-%m-%d"),
                "employee_id": emp["employee_id"],
                "employee_name": emp["employee_name"],
                "email": emp["email"],
                "department": dept,
                "category": category,
                "merchant": merchant,
                "amount_usd": amount,
                "currency": "USD",
                "policy_limit_usd": policy_limit,
                "policy_violation": policy_violation,
                "notes": fake.sentence(nb_words=8) if random.random() < 0.3 else "",
            }
        )

    transactions.sort(key=lambda x: x["transaction_date"])
    return transactions


def main():
    out_path = Path(__file__).parent.parent / "data" / "transactions.csv"
    out_path.parent.mkdir(exist_ok=True)

    rows = generate_transactions(5000)
    fieldnames = list(rows[0].keys())

    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Generated {len(rows)} transactions → {out_path}")
    violations = sum(1 for r in rows if r["policy_violation"])
    print(f"Policy violations: {violations} ({violations/len(rows)*100:.1f}%)")


if __name__ == "__main__":
    main()
