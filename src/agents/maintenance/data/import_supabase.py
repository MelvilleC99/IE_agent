"""
Migrate Maintenance Data from JSON to Supabase

This script takes the exported JSON file from Firebase and migrates it to Supabase tables.
It creates the necessary tables if they don't exist and handles data transformation.

Usage:
1. Set up Supabase credentials in .env file or environment variables
2. Run the script with the path to the JSON file
   python migrate_to_supabase.py --input maintenance_data.json --create-tables

Required environment variables:
- SUPABASE_URL
- SUPABASE_KEY
"""

import os
import json
import argparse
import time
from datetime import datetime
from tqdm import tqdm
from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment variables
load_dotenv()

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Migrate maintenance data from JSON to Supabase.")
    parser.add_argument("--input", type=str, default="maintenance_data.json", help="Input JSON file path")
    parser.add_argument("--batch-size", type=int, default=10, help="Batch size for inserts")
    parser.add_argument("--create-tables", action="store_true", help="Create tables if they don't exist")
    parser.add_argument("--clear-tables", action="store_true", help="Clear existing data from tables")
    return parser.parse_args()

def initialize_supabase():
    """Initialize Supabase connection."""
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    
    if not supabase_url or not supabase_key:
        raise ValueError("Supabase URL and key must be set in .env file or environment variables")
    
    print(f"Connecting to Supabase at {supabase_url}")
    return create_client(supabase_url, supabase_key)

def create_supabase_tables(supabase: Client):
    """Create the necessary tables in Supabase if they don't exist."""
    print("Creating tables in Supabase...")
    
    try:
        # 1. Create employees table
        try:
            supabase.table("employees").select("count").limit(1).execute()
            print("Employees table already exists.")
        except Exception as e:
            print(f"Creating employees table... ({str(e)})")
            supabase.query("""
            CREATE TABLE IF NOT EXISTS employees (
                employee_number VARCHAR PRIMARY KEY,
                name VARCHAR,
                surname VARCHAR,
                role VARCHAR,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
            """).execute()
        
        # 2. Create production_lines table
        try:
            supabase.table("production_lines").select("count").limit(1).execute()
            print("Production lines table already exists.")
        except Exception as e:
            print(f"Creating production_lines table... ({str(e)})")
            supabase.query("""
            CREATE TABLE IF NOT EXISTS production_lines (
                id VARCHAR PRIMARY KEY,
                name VARCHAR,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
            """).execute()
        
        # 3. Create styles table
        try:
            supabase.table("styles").select("count").limit(1).execute()
            print("Styles table already exists.")
        except Exception as e:
            print(f"Creating styles table... ({str(e)})")
            supabase.query("""
            CREATE TABLE IF NOT EXISTS styles (
                id VARCHAR PRIMARY KEY,
                style_number VARCHAR,
                product_category VARCHAR,
                product_type VARCHAR,
                fabric_type VARCHAR,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
            """).execute()
        
        # 4. Create machines table
        try:
            supabase.table("machines").select("count").limit(1).execute()
            print("Machines table already exists.")
        except Exception as e:
            print(f"Creating machines table... ({str(e)})")
            supabase.query("""
            CREATE TABLE IF NOT EXISTS machines (
                machine_number VARCHAR PRIMARY KEY,
                machine_type VARCHAR,
                machine_make VARCHAR,
                machine_model VARCHAR,
                purchase_date VARCHAR,
                date_added TIMESTAMPTZ,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
            """).execute()
        
        # 5. Create machine_downtime table (renamed from maintenance_records)
        try:
            supabase.table("machine_downtime").select("count").limit(1).execute()
            print("Machine downtime table already exists.")
        except Exception as e:
            print(f"Creating machine_downtime table... ({str(e)})")
            supabase.query("""
            CREATE TABLE IF NOT EXISTS machine_downtime (
                id VARCHAR PRIMARY KEY,
                created_at TIMESTAMPTZ,
                resolved_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ,
                mechanic_id VARCHAR,
                mechanic_name VARCHAR,
                machine_number VARCHAR,
                machine_type VARCHAR,
                machine_make VARCHAR,
                machine_model VARCHAR,
                production_line_id VARCHAR,
                production_line_name VARCHAR,
                style_id VARCHAR,
                style_number VARCHAR,
                product_category VARCHAR,
                product_type VARCHAR,
                fabric_type VARCHAR,
                supervisor_id VARCHAR,
                supervisor_name VARCHAR,
                reason VARCHAR,
                comments TEXT,
                additional_comments TEXT,
                status VARCHAR,
                total_downtime FLOAT,
                total_repair_time FLOAT,
                total_response_time FLOAT,
                mechanic_acknowledged BOOLEAN,
                mechanic_acknowledged_at TIMESTAMPTZ
            );
            """).execute()
        
        print("Table creation complete.")
        
    except Exception as e:
        print(f"Error creating tables: {str(e)}")
        raise

def clear_supabase_tables(supabase: Client):
    """Clear all data from the tables."""
    print("Clearing existing data from tables...")
    
    try:
        # Clear in reverse order of dependencies
        print("Clearing machine_downtime table...")
        supabase.table("machine_downtime").delete().execute()
        
        print("Clearing machines table...")
        supabase.table("machines").delete().execute()
        
        print("Clearing styles table...")
        supabase.table("styles").delete().execute()
        
        print("Clearing production_lines table...")
        supabase.table("production_lines").delete().execute()
        
        print("Clearing employees table...")
        supabase.table("employees").delete().execute()
        
        print("Tables cleared.")
    
    except Exception as e:
        print(f"Error clearing tables: {str(e)}")
        raise

def safe_insert(supabase, table_name, records):
    """Safely insert records with error handling."""
    if not records:
        return 0
    
    try:
        response = supabase.table(table_name).upsert(records).execute()
        return len(records)
    except Exception as e:
        print(f"Error inserting batch to {table_name}: {str(e)}")
        
        # Try one by one
        successful = 0
        for record in records:
            try:
                supabase.table(table_name).upsert([record]).execute()
                successful += 1
            except Exception as e2:
                print(f"Error inserting single record to {table_name}: {str(e2)}")
        
        return successful

def migrate_data(supabase: Client, data, batch_size=10):
    """Migrate data from JSON to Supabase tables."""
    print("Starting data migration...")
    
    # Create sets to track unique entries
    unique_employees = {}  # employee_number -> {name, surname, role}
    unique_lines = {}      # id -> {name}
    unique_styles = {}     # id -> {style_number, product_category, product_type, fabric_type}
    unique_machines = {}   # machine_number -> {machine_type, machine_make, machine_model, purchase_date, date_added}
    
    # Extract unique entities from data
    print("Extracting unique entities...")
    for record in tqdm(data):
        # Extract employee data (mechanics and supervisors)
        if 'mechanicId' in record and record['mechanicId']:
            employee_id = record['mechanicId']
            if employee_id not in unique_employees:
                name_parts = record.get('mechanicName', '').split(' ', 1)
                name = name_parts[0] if name_parts else ''
                surname = name_parts[1] if len(name_parts) > 1 else ''
                unique_employees[employee_id] = {
                    'name': name,
                    'surname': surname,
                    'role': 'Mechanic'
                }
        
        if 'supervisorId' in record and record['supervisorId']:
            employee_id = record['supervisorId']
            if employee_id not in unique_employees:
                name_parts = record.get('supervisorName', '').split(' ', 1)
                name = name_parts[0] if name_parts else ''
                surname = name_parts[1] if len(name_parts) > 1 else ''
                unique_employees[employee_id] = {
                    'name': name,
                    'surname': surname,
                    'role': 'Supervisor'
                }
        
        # Extract production line data
        if 'productionLineId' in record and record['productionLineId']:
            line_id = record['productionLineId']
            if line_id not in unique_lines:
                unique_lines[line_id] = {
                    'name': record.get('productionLineName', 'Unknown')
                }
        
        # Extract style data
        if 'styleId' in record and record['styleId']:
            style_id = record['styleId']
            if style_id not in unique_styles:
                unique_styles[style_id] = {
                    'style_number': record.get('styleNumber', ''),
                    'product_category': record.get('productCategory', ''),
                    'product_type': record.get('productType', ''),
                    'fabric_type': record.get('fabricType', '')
                }
        
        # Extract machine data
        if 'machineNumber' in record and record['machineNumber']:
            machine_number = record['machineNumber']
            if machine_number not in unique_machines:
                unique_machines[machine_number] = {
                    'machine_type': record.get('machineType', ''),
                    'machine_make': record.get('machineMake', ''),
                    'machine_model': record.get('machineModel', ''),
                    'purchase_date': record.get('machinePurchaseDate', ''),
                    'date_added': record.get('machineDateAdded')
                }
    
    # Insert employees
    print(f"Inserting {len(unique_employees)} employees...")
    employee_batches = [list(unique_employees.items())[i:i+batch_size] for i in range(0, len(unique_employees), batch_size)]
    employee_count = 0
    
    for batch in tqdm(employee_batches):
        employee_records = []
        for employee_id, employee_data in batch:
            employee_records.append({
                'employee_number': employee_id,
                'name': employee_data['name'],
                'surname': employee_data['surname'],
                'role': employee_data['role']
            })
        
        employee_count += safe_insert(supabase, "employees", employee_records)
    
    print(f"Successfully inserted {employee_count} employees.")
    
    # Insert production lines
    print(f"Inserting {len(unique_lines)} production lines...")
    line_batches = [list(unique_lines.items())[i:i+batch_size] for i in range(0, len(unique_lines), batch_size)]
    line_count = 0
    
    for batch in tqdm(line_batches):
        line_records = []
        for line_id, line_data in batch:
            line_records.append({
                'id': line_id,
                'name': line_data['name']
            })
        
        line_count += safe_insert(supabase, "production_lines", line_records)
    
    print(f"Successfully inserted {line_count} production lines.")
    
    # Insert styles
    print(f"Inserting {len(unique_styles)} styles...")
    style_batches = [list(unique_styles.items())[i:i+batch_size] for i in range(0, len(unique_styles), batch_size)]
    style_count = 0
    
    for batch in tqdm(style_batches):
        style_records = []
        for style_id, style_data in batch:
            style_records.append({
                'id': style_id,
                'style_number': style_data['style_number'],
                'product_category': style_data['product_category'],
                'product_type': style_data['product_type'],
                'fabric_type': style_data['fabric_type']
            })
        
        style_count += safe_insert(supabase, "styles", style_records)
    
    print(f"Successfully inserted {style_count} styles.")
    
    # Insert machines
    print(f"Inserting {len(unique_machines)} machines...")
    machine_batches = [list(unique_machines.items())[i:i+batch_size] for i in range(0, len(unique_machines), batch_size)]
    machine_count = 0
    
    for batch in tqdm(machine_batches):
        machine_records = []
        for machine_number, machine_data in batch:
            machine_records.append({
                'machine_number': machine_number,
                'machine_type': machine_data['machine_type'],
                'machine_make': machine_data['machine_make'],
                'machine_model': machine_data['machine_model'],
                'purchase_date': machine_data['purchase_date'],
                'date_added': machine_data['date_added']
            })
        
        machine_count += safe_insert(supabase, "machines", machine_records)
    
    print(f"Successfully inserted {machine_count} machines.")
    
    # Insert machine downtime records
    print(f"Inserting {len(data)} machine downtime records...")
    maintenance_batches = [data[i:i+batch_size] for i in range(0, len(data), batch_size)]
    maintenance_count = 0
    
    for batch in tqdm(maintenance_batches):
        downtime_records = []
        for record in batch:
            try:
                # Convert time values from milliseconds to minutes (if they exist and are numbers)
                total_downtime = None
                if record.get('totalDowntime') is not None:
                    try:
                        total_downtime = float(record['totalDowntime']) / 60000  # Convert to minutes
                    except (ValueError, TypeError):
                        total_downtime = None
                
                total_repair_time = None
                if record.get('totalRepairTime') is not None:
                    try:
                        total_repair_time = float(record['totalRepairTime']) / 60000  # Convert to minutes
                    except (ValueError, TypeError):
                        total_repair_time = None
                
                total_response_time = None
                if record.get('totalResponseTime') is not None:
                    try:
                        total_response_time = float(record['totalResponseTime']) / 60000  # Convert to minutes
                    except (ValueError, TypeError):
                        total_response_time = None
                
                # Convert to proper format for Supabase
                downtime_record = {
                    'id': record.get('id'),
                    'created_at': record.get('createdAt'),
                    'resolved_at': record.get('resolvedAt'),
                    'updated_at': record.get('updatedAt'),
                    'mechanic_id': record.get('mechanicId'),
                    'mechanic_name': record.get('mechanicName'),
                    'machine_number': record.get('machineNumber'),
                    'machine_type': record.get('machineType'),
                    'machine_make': record.get('machineMake'),
                    'machine_model': record.get('machineModel'),
                    'production_line_id': record.get('productionLineId'),
                    'production_line_name': record.get('productionLineName'),
                    'style_id': record.get('styleId'),
                    'style_number': record.get('styleNumber'),
                    'product_category': record.get('productCategory'),
                    'product_type': record.get('productType'),
                    'fabric_type': record.get('fabricType'),
                    'supervisor_id': record.get('supervisorId'),
                    'supervisor_name': record.get('supervisorName'),
                    'reason': record.get('reason'),
                    'comments': record.get('comments'),
                    'additional_comments': record.get('additionalComments'),
                    'status': record.get('status'),
                    'total_downtime': total_downtime,
                    'total_repair_time': total_repair_time,
                    'total_response_time': total_response_time,
                    'mechanic_acknowledged': record.get('mechanicAcknowledged', False),
                    'mechanic_acknowledged_at': record.get('mechanicAcknowledgedAt')
                }
                downtime_records.append(downtime_record)
            except Exception as e:
                print(f"Error preparing record {record.get('id', 'unknown')}: {str(e)}")
        
        maintenance_count += safe_insert(supabase, "machine_downtime", downtime_records)
        
        # Small delay to avoid rate limits
        time.sleep(0.5)
    
    print(f"Successfully inserted {maintenance_count} out of {len(data)} machine downtime records.")
    print("Data migration complete.")

def main():
    """Main function to migrate data."""
    args = parse_arguments()
    
    try:
        # Load JSON data
        print(f"Loading data from {args.input}...")
        with open(args.input, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        print(f"Loaded {len(data)} records.")
        
        # Initialize Supabase client
        print("Connecting to Supabase...")
        supabase = initialize_supabase()
        
        # Create tables if requested
        if args.create_tables:
            create_supabase_tables(supabase)
        
        # Clear tables if requested
        if args.clear_tables:
            clear_supabase_tables(supabase)
        
        # Migrate data
        migrate_data(supabase, data, batch_size=args.batch_size)
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()