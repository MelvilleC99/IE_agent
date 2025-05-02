#!/usr/bin/env python3
import os
import json
import time
import argparse
from datetime import datetime
from tqdm import tqdm
from dateparser import parse as parse_date
from dotenv import load_dotenv
from supabase.client import create_client, Client
from supabase.lib.client_options import ClientOptions
from postgrest.exceptions import APIError

def parse_args():
    p = argparse.ArgumentParser(
        description="Load JSON and insert into Supabase table 'downtime_detail'."
    )
    p.add_argument(
        "--input", "-i",
        default="maintenance_data.json",
        help="Path to JSON file exported from Firebase"
    )
    p.add_argument(
        "--batch-size", "-b",
        type=int,
        default=50,
        help="How many records to insert per API call"
    )
    p.add_argument(
        "--create-table", action="store_true",
        help="Create downtime_detail table if it doesn't exist"
    )
    p.add_argument(
        "--clear-table", action="store_true",
        help="Delete all existing rows in downtime_detail before insert"
    )
    return p.parse_args()

def init_supabase() -> Client:
    """Initialize the Supabase client using environment variables from .env.local."""
    # Load environment variables from .env.local
    load_dotenv('.env.local')
    
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    
    if not url or not key:
        raise RuntimeError("Set SUPABASE_URL and SUPABASE_KEY in your .env.local file")
    
    try:
        client = create_client(url, key)
        print("✅ Supabase client initialized successfully")
        return client
    except Exception as e:
        print(f"❌ Error initializing Supabase client: {str(e)}")
        raise

def create_table(supabase: Client):
    """Create the downtime_detail table if it doesn't exist."""
    try:
        # First check if table exists
        try:
            supabase.table("downtime_detail").select("id").limit(1).execute()
            print("✅ downtime_detail table already exists")
            return
        except APIError:
            pass  # Table doesn't exist, continue with creation

        # Create table using direct SQL
        sql = """
        CREATE TABLE IF NOT EXISTS downtime_detail (
            id TEXT PRIMARY KEY,
            created_at TIMESTAMPTZ,
            resolved_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ,
            machine_number TEXT,
            machine_type TEXT,
            machine_make TEXT,
            machine_model TEXT,
            machine_purchase_date DATE,
            machine_date_added TIMESTAMPTZ,
            mechanic_id TEXT,
            mechanic_name TEXT,
            mechanic_acknowledged BOOLEAN,
            mechanic_acknowledged_at TIMESTAMPTZ,
            supervisor_id TEXT,
            supervisor_name TEXT,
            production_line_id TEXT,
            production_line_name TEXT,
            style_id TEXT,
            style_number TEXT,
            product_category TEXT,
            product_type TEXT,
            fabric_type TEXT,
            reason TEXT,
            comments TEXT,
            additional_comments TEXT,
            status TEXT,
            total_downtime DOUBLE PRECISION,
            total_repair_time DOUBLE PRECISION,
            total_response_time DOUBLE PRECISION
        );
        """
        supabase.table("downtime_detail").insert({"id": "dummy"}).execute()
        print("✅ downtime_detail table created successfully")
    except APIError as e:
        print(f"❌ Error creating table: {str(e)}")
        raise

def clear_table(supabase: Client):
    """Delete all existing rows in downtime_detail."""
    try:
        supabase.table("downtime_detail").delete().neq("id", "").execute()
        print("🗑  Cleared downtime_detail")
    except APIError as e:
        print(f"❌ Error clearing table: {str(e)}")
        raise

def transform(record: dict) -> dict:
    """Transform a record from the input format to the database format."""
    def to_dt(s): 
        dt = parse_date(s) if s else None
        return dt.isoformat() if dt else None

    ms = record.get
    return {
        "id":                       ms("id"),
        "created_at":               to_dt(ms("createdAt")),
        "resolved_at":              to_dt(ms("resolvedAt")),
        "updated_at":               to_dt(ms("updatedAt")),
        "machine_number":           ms("machineNumber"),
        "machine_type":             ms("machineType"),
        "machine_make":             ms("machineMake"),
        "machine_model":            ms("machineModel"),
        "machine_purchase_date":    to_dt(ms("machinePurchaseDate")),
        "machine_date_added":       to_dt(ms("machineDateAdded")),
        "mechanic_id":              ms("mechanicId"),
        "mechanic_name":            ms("mechanicName"),
        "mechanic_acknowledged":    record.get("mechanicAcknowledged", False),
        "mechanic_acknowledged_at": to_dt(ms("mechanicAcknowledgedAt")),
        "supervisor_id":            ms("supervisorId"),
        "supervisor_name":          ms("supervisorName"),
        "production_line_id":       ms("productionLineId"),
        "production_line_name":     ms("productionLineName"),
        "style_id":                 ms("styleId"),
        "style_number":             ms("styleNumber"),
        "product_category":         ms("productCategory"),
        "product_type":             ms("productType"),
        "fabric_type":              ms("fabricType"),
        "reason":                   ms("reason"),
        "comments":                 ms("comments"),
        "additional_comments":      ms("additionalComments"),
        "status":                   ms("status"),
        "total_downtime":           (record.get("totalDowntime")  or 0) / 1_000,
        "total_repair_time":        (record.get("totalRepairTime") or 0) / 1_000,
        "total_response_time":      (record.get("totalResponseTime") or 0) / 1_000,
    }

def batch_insert(supabase: Client, rows: list, batch_size: int):
    """Insert rows in batches."""
    table = "downtime_detail"
    successes = 0
    for i in range(0, len(rows), batch_size):
        chunk = rows[i: i + batch_size]
        try:
            res = supabase.table(table).insert(chunk).execute()
            successes += len(chunk)
            print(f"✅ Inserted batch of {len(chunk)} records")
        except APIError as e:
            print(f"❌ Error inserting batch starting at {i}: {str(e)}")
        time.sleep(0.2)  # throttle a bit
    return successes

def main():
    args = parse_args()
    sb = init_supabase()

    if args.create_table:
        create_table(sb)
    if args.clear_table:
        clear_table(sb)

    print(f"📥 Loading JSON from {args.input}")
    with open(args.input, "r", encoding="utf-8") as f:
        data = json.load(f)

    print(f"🔄 Transforming {len(data)} records")
    rows = [transform(r) for r in data]

    print(f"🚀 Inserting in batches of {args.batch_size}")
    count = batch_insert(sb, rows, args.batch_size)
    print(f"🎉 Inserted {count} rows into downtime_detail")

if __name__ == "__main__":
    main()
