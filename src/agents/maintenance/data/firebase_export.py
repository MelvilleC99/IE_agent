"""
Firebase Data Export Script for Maintenance AI Agent

This script exports maintenance data from Firebase to a structured JSON file.
It pulls data from multiple collections and combines them into a comprehensive dataset.

Collections:
- machineDowntimes (main data)
- productionLines (line names)
- supportFunctions (mechanic and supervisor details)
- styles (product information)
- machines (machine details)

Usage:
1. Run the script:
   python firebase_export.py
"""

import os
import json
import argparse
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timedelta
from tqdm import tqdm  # For progress bar
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Export maintenance data from Firebase to JSON.")
    parser.add_argument("--start-date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--output", type=str, default="maintenance_data.json", help="Output file path")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of records (0 for no limit)")
    return parser.parse_args()

def initialize_firebase():
    """Initialize Firebase with service account JSON file."""
    try:
        # Use hard-coded path to Firebase credentials
        cred_path = "/Users/melville/Documents/Firebase/firebase.json"
        print(f"Using credentials from: {cred_path}")
        
        if not os.path.exists(cred_path):
            raise ValueError(f"Firebase credentials file not found at: {cred_path}")
            
        cred = credentials.Certificate(cred_path)
        
        # Initialize Firebase admin
        firebase_admin.initialize_app(cred)
        db = firestore.client()
        print("Firebase initialized successfully!")
        return db
    
    except Exception as e:
        print(f"Error initializing Firebase: {str(e)}")
        print("\nTroubleshooting steps:")
        print("1. Verify the file exists at: /Users/melville/Documents/Firebase/firebase.json")
        print("2. Check that the service account JSON file has proper permissions")
        print("3. Ensure the service account has access to the Firestore database")
        raise

def convert_timestamp(ts):
    """Convert Firestore timestamp to ISO format string."""
    if hasattr(ts, 'seconds'):  # Firestore Timestamp object
        return datetime.fromtimestamp(ts.seconds + ts.nanoseconds / 1e9).isoformat()
    elif isinstance(ts, dict) and '_seconds' in ts:  # Firestore timestamp dict
        return datetime.fromtimestamp(ts['_seconds'] + ts['_nanoseconds'] / 1e9).isoformat()
    elif ts is None:
        return None
    return str(ts)  # Fallback

def export_data(db, start_date=None, end_date=None, limit=0):
    """Export data from Firebase Firestore."""
    print("Starting data export process...")
    
    # Get collection references
    downtimes_ref = db.collection('machineDowntimes')
    production_lines_ref = db.collection('productionLines')
    support_functions_ref = db.collection('supportFunctions')
    styles_ref = db.collection('styles')
    machines_ref = db.collection('machines')  # Added machines collection
    
    # Fetch support data first
    print("Fetching reference data...")
    
    # Machines collection
    print("Fetching machines data...")
    machines_data = {}
    for doc in machines_ref.stream():
        machines_data[doc.id] = doc.to_dict()
    print(f"Fetched {len(machines_data)} machines from machines collection")
    
    # Create a lookup by machine number/asset number
    machine_by_number = {}
    for machine_id, machine_data in machines_data.items():
        if 'assetNumber' in machine_data:
            machine_by_number[machine_data['assetNumber']] = machine_data
    
    # Production lines
    print("Fetching production lines...")
    production_lines = {}
    for doc in production_lines_ref.stream():
        production_lines[doc.id] = doc.to_dict()
    print(f"Fetched {len(production_lines)} production lines")
    
    # Support functions (employees)
    print("Fetching employees...")
    employees = {}
    employee_by_number = {}
    for doc in support_functions_ref.stream():
        data = doc.to_dict()
        employees[doc.id] = data
        # Create lookup by employee number
        if 'employeeNumber' in data:
            employee_by_number[data['employeeNumber']] = data
    print(f"Fetched {len(employees)} employees")
    
    # Styles
    print("Fetching styles...")
    styles = {}
    for doc in styles_ref.stream():
        styles[doc.id] = doc.to_dict()
    print(f"Fetched {len(styles)} styles")
    
    # Query for machine downtimes
    query = downtimes_ref.where('status', '==', 'Closed')
    
    # Add date filters if provided
    if start_date:
        start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
        query = query.where('createdAt', '>=', start_datetime)
    
    if end_date:
        end_datetime = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)
        query = query.where('createdAt', '<', end_datetime)
    
    print("Fetching machine downtimes...")
    downtimes_docs = list(query.stream())
    print(f"Fetched {len(downtimes_docs)} machine downtimes")
    
    # Apply limit if set
    if limit > 0 and len(downtimes_docs) > limit:
        print(f"Limiting to {limit} records as requested")
        downtimes_docs = downtimes_docs[:limit]
    
    # Process records
    print("Processing records...")
    enriched_records = []
    
    for doc in tqdm(downtimes_docs):
        record_id = doc.id
        data = doc.to_dict()
        
        # Skip records that don't have required fields
        if 'createdAt' not in data or 'resolvedAt' not in data:
            print(f"Skipping record {record_id} - missing required fields")
            continue
        
        # Create base record
        record = {
            'id': record_id,
            'createdAt': convert_timestamp(data.get('createdAt')),
            'resolvedAt': convert_timestamp(data.get('resolvedAt')),
            'updatedAt': convert_timestamp(data.get('updatedAt')),
            'machineNumber': data.get('machineNumber'),
            'machineType': data.get('machineType', ''),
            'machineMake': data.get('machineMake', ''),
            'totalDowntime': data.get('totalDowntime'),
            'totalRepairTime': data.get('totalRepairTime'),
            'totalResponseTime': data.get('totalResponseTime'),
            'reason': data.get('reason'),
            'comments': data.get('comments', ''),
            'additionalComments': data.get('additionalComments', ''),
            'status': data.get('status')
        }
        
        # Add machine details from machines collection if available
        if 'machineNumber' in data and data['machineNumber'] in machine_by_number:
            machine_data = machine_by_number[data['machineNumber']]
            
            # Only fill in missing data from machine collection
            if not record['machineType'] and 'type' in machine_data:
                record['machineType'] = machine_data['type']
                
            if not record['machineMake'] and 'make' in machine_data:
                record['machineMake'] = machine_data['make']
                
            # Add additional machine information
            record['machineModel'] = machine_data.get('model', '')
            record['machinePurchaseDate'] = machine_data.get('purchaseDate', '')
            record['machineDateAdded'] = convert_timestamp(machine_data.get('dateAdded'))
        
        # Add mechanic info
        if 'mechanicId' in data:
            mechanic_id = data['mechanicId']
            record['mechanicId'] = mechanic_id
            
            # Get mechanic name
            if 'mechanicName' in data:
                record['mechanicName'] = data['mechanicName']
            elif mechanic_id in employee_by_number:
                mechanic = employee_by_number[mechanic_id]
                name = mechanic.get('name', '')
                surname = mechanic.get('surname', '')
                record['mechanicName'] = f"{name} {surname}".strip()
            
            # Mechanic acknowledgement
            record['mechanicAcknowledged'] = data.get('mechanicAcknowledged', False)
            record['mechanicAcknowledgedAt'] = convert_timestamp(data.get('mechanicAcknowledgedAt'))
        
        # Add supervisor info
        if 'supervisorId' in data:
            supervisor_id = data['supervisorId']
            record['supervisorId'] = supervisor_id
            
            if supervisor_id in employee_by_number:
                supervisor = employee_by_number[supervisor_id]
                name = supervisor.get('name', '')
                surname = supervisor.get('surname', '')
                record['supervisorName'] = f"{name} {surname}".strip()
        
        # Add production line info
        if 'productionLineId' in data:
            line_id = data['productionLineId']
            record['productionLineId'] = line_id
            
            if line_id in production_lines:
                record['productionLineName'] = production_lines[line_id].get('name', 'Unknown')
        
        # Add style info
        if 'styleId' in data:
            style_id = data['styleId']
            record['styleId'] = style_id
            
            if style_id in styles:
                style = styles[style_id]
                record['styleNumber'] = style.get('styleNumber', '')
                record['productCategory'] = style.get('productCategory', '')
                record['productType'] = style.get('productType', '')
                record['fabricType'] = style.get('fabricType', '')
        
        enriched_records.append(record)
    
    return enriched_records

def main():
    """Main function."""
    args = parse_arguments()
    
    try:
        # Initialize Firebase
        db = initialize_firebase()
        
        # Export data
        data = export_data(
            db, 
            start_date=args.start_date,
            end_date=args.end_date,
            limit=args.limit
        )
        
        # Save to file - use ensure_ascii=False to properly handle Unicode characters
        output_file = args.output
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"Successfully exported {len(data)} records to {output_file}")
        
        # Print summary
        if data:
            earliest_date = min((r['createdAt'] for r in data if 'createdAt' in r), default="N/A")
            latest_date = max((r['createdAt'] for r in data if 'createdAt' in r), default="N/A")
            unique_mechanics = len(set(r.get('mechanicId') for r in data if r.get('mechanicId')))
            unique_machines = len(set(r.get('machineNumber') for r in data if r.get('machineNumber')))
            
            print("\nData Summary:")
            print(f"- Date range: {earliest_date} to {latest_date}")
            print(f"- Unique machines: {unique_machines}")
            print(f"- Unique mechanics: {unique_mechanics}")
            
            # Calculate average response and repair times
            total_response_times = [r.get('totalResponseTime') for r in data if r.get('totalResponseTime')]
            total_repair_times = [r.get('totalRepairTime') for r in data if r.get('totalRepairTime')]
            
            if total_response_times:
                avg_response_time = sum(total_response_times) / len(total_response_times) / 60000  # Convert to minutes
                print(f"- Average response time: {avg_response_time:.2f} minutes")
            
            if total_repair_times:
                avg_repair_time = sum(total_repair_times) / len(total_repair_times) / 60000  # Convert to minutes
                print(f"- Average repair time: {avg_repair_time:.2f} minutes")
    
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()