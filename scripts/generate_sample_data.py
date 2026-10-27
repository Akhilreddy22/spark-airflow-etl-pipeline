#!/usr/bin/env python3
"""
Sample Data Generator for Customer 360 ETL Pipeline
Generates raw customer records, transactions, and clickstream events.
"""

import os
import json
import csv
import random
from datetime import datetime, timedelta

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")
os.makedirs(DATA_DIR, exist_ok=True)

CATEGORIES = ["Electronics", "Fashion", "Home & Garden", "Books", "Sports", "Beauty"]
DEVICE_TYPES = ["mobile_ios", "mobile_android", "desktop_mac", "desktop_windows", "tablet"]
EVENT_TYPES = ["page_view", "search", "add_to_cart", "remove_from_cart", "checkout_start", "purchase"]
COUNTRIES = ["US", "CA", "GB", "DE", "FR", "JP", "AU"]
TIERS = ["Bronze", "Silver", "Gold", "Platinum"]

def generate_customers(count=150):
    customers = []
    base_date = datetime(2025, 1, 1)
    for i in range(1, count + 1):
        cid = f"CUST_{i:04d}"
        created = base_date + timedelta(days=random.randint(0, 365))
        country = random.choice(COUNTRIES)
        tier = random.choices(TIERS, weights=[0.45, 0.30, 0.18, 0.07])[0]
        customers.append({
            "customer_id": cid,
            "first_name": f"User{i}",
            "last_name": f"LastName{i}",
            "email": f"user{i}@example.com" if random.random() > 0.03 else None, # Introduce occasional null for cleaning
            "country": country,
            "tier": tier,
            "created_at": created.strftime("%Y-%m-%d %H:%M:%S")
        })

    csv_path = os.path.join(DATA_DIR, "customers.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["customer_id", "first_name", "last_name", "email", "country", "tier", "created_at"])
        writer.writeheader()
        writer.writerows(customers)
    print(f"✓ Generated {len(customers)} customers -> {csv_path}")
    return [c["customer_id"] for c in customers]

def generate_clickstream(customer_ids, event_count=1200):
    events = []
    now = datetime(2026, 10, 27)
    for i in range(1, event_count + 1):
        cid = random.choice(customer_ids) if random.random() > 0.10 else None # Anonymous guest visits
        event_time = now - timedelta(hours=random.randint(0, 72), minutes=random.randint(0, 59))
        session_id = f"SESS_{random.randint(100, 999)}"
        event_type = random.choices(EVENT_TYPES, weights=[0.45, 0.25, 0.15, 0.05, 0.07, 0.03])[0]
        category = random.choice(CATEGORIES)
        dwell = random.randint(3, 420) if event_type != "purchase" else random.randint(30, 180)

        events.append({
            "event_id": f"EVT_{i:06d}",
            "session_id": session_id,
            "customer_id": cid,
            "event_type": event_type,
            "product_category": category,
            "product_id": f"PROD_{random.randint(1000, 9999)}",
            "device_type": random.choice(DEVICE_TYPES),
            "dwell_time_seconds": dwell,
            "event_timestamp": event_time.isoformat()
        })

    json_path = os.path.join(DATA_DIR, "clickstream.json")
    with open(json_path, "w", encoding="utf-8") as f:
        for ev in events:
            f.write(json.dumps(ev) + "\n")
    print(f"✓ Generated {len(events)} clickstream events -> {json_path}")

def generate_transactions(customer_ids, count=400):
    transactions = []
    base_date = datetime(2026, 10, 27)
    for i in range(1, count + 1):
        cid = random.choice(customer_ids)
        order_date = base_date - timedelta(days=random.randint(0, 90), hours=random.randint(0, 23))
        amount = round(random.uniform(15.0, 580.0), 2)
        payment_method = random.choice(["credit_card", "paypal", "apple_pay", "crypto"])
        status = random.choices(["COMPLETED", "REFUNDED", "FAILED"], weights=[0.92, 0.05, 0.03])[0]

        transactions.append({
            "transaction_id": f"TX_{i:05d}",
            "customer_id": cid,
            "amount": amount,
            "payment_method": payment_method,
            "status": status,
            "transaction_timestamp": order_date.strftime("%Y-%m-%d %H:%M:%S")
        })

    json_path = os.path.join(DATA_DIR, "transactions.json")
    with open(json_path, "w", encoding="utf-8") as f:
        for tx in transactions:
            f.write(json.dumps(tx) + "\n")
    print(f"✓ Generated {len(transactions)} transactions -> {json_path}")

if __name__ == "__main__":
    print("Generating raw dataset for PySpark & Airflow pipeline...")
    cids = generate_customers()
    generate_clickstream(cids)
    generate_transactions(cids)
    print("Done! Ready for ETL execution.")
