import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import json
from datetime import datetime, timedelta
import logging
import os
from config.settings import FIREBASE_CONFIG

# Set up logging
logging.basicConfig(
    filename='debug.log',
    level=logging.INFO,
    format='%(asctime)s: %(message)s'
)

# Initialize Firebase
if not firebase_admin._apps:
    # Get the absolute path to the service account file
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_dir, '../../../../'))
    service_account_path = os.path.join(project_root, 'firebase-service-account.json')
    
    cred = credentials.Certificate(service_account_path)
    firebase_admin.initialize_app(cred)
db = firestore.client()

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime objects"""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

def fetch_maintenance_records(start_date, end_date, date_field="createdAt"):
    """Fetch basic maintenance records within a date range"""
    logging.info(f"Fetching maintenance records from: {start_date} to {end_date}")
    
    # Convert to timestamps for Firestore query
    ts_start = start_date
    ts_end = end_date
    
    # Query Firestore
    records = []
    records_ref = (db.collection("machineDowntimes")
                    .where(date_field, ">=", ts_start)
                    .where(date_field, "<", ts_end)
                    .order_by(date_field, "desc"))
    
    docs = records_ref.stream()
    
    for doc in docs:
        record = doc.to_dict()
        record['id'] = doc.id
        records.append(record)
    
    logging.info(f"Fetched {len(records)} maintenance records")
    return records

def fetch_style_data(style_id):
    """Fetch style data for a given style ID"""
    logging.info(f"Fetching style data for styleId: {style_id}")
    doc = db.collection("styles").document(style_id).get()
    
    if doc.exists:
        logging.info(f"Style data found for styleId: {style_id}")
        return doc.to_dict()
    else:
        logging.info(f"No style data found for styleId: {style_id}")
        return None

def fetch_supervisor_data(supervisor_id):
    """Fetch supervisor data for a given supervisor ID"""
    logging.info(f"Fetching supervisor data for supervisorId: {supervisor_id}")
    doc = db.collection("supportFunctions").document(supervisor_id).get()
    
    if doc.exists:
        data = doc.to_dict()
        data['name'] = f"{data.get('name', '')} {data.get('surname', '')}".strip()
        logging.info(f"Supervisor data found: {json.dumps(data)}")
        return data
    else:
        logging.info(f"No supervisor data found for supervisorId: {supervisor_id}")
        return None

def fetch_production_line_data(production_line_id):
    """Fetch production line data for a given production line ID"""
    logging.info(f"Fetching production line data for productionLineId: {production_line_id}")
    doc = db.collection("productionLines").document(production_line_id).get()
    
    if doc.exists:
        logging.info(f"Production line data found")
        return doc.to_dict()
    else:
        logging.info(f"No production line data found for productionLineId: {production_line_id}")
        return None

def fetch_machine_data(machine_number):
    """Fetch machine data for a given machine number"""
    logging.info("\n=== MACHINE DATA FETCH START ===")
    
    if not machine_number:
        logging.info("Machine number is empty or None")
        logging.info("=== MACHINE DATA FETCH END ===\n")
        return None
    
    trimmed = machine_number.strip()
    
    # Debug logging
    logging.info(f"Original machineNumber: \"{machine_number}\"")
    logging.info(f"Trimmed machineNumber: \"{trimmed}\"")
    
    # For debugging, get all machines first
    all_machines_ref = db.collection("machines")
    all_machines = all_machines_ref.stream()
    
    logging.info("\nAll machines in collection:")
    for doc in all_machines:
        data = doc.to_dict()
        asset_number = data.get('assetNumber', '')
        logging.info(f"Machine - assetNumber: \"{asset_number}\", type: {type(asset_number)}, length: {len(asset_number) if asset_number else 0}")
    
    # Perform the actual query
    logging.info(f"\nQuerying for machine with assetNumber: \"{trimmed}\"")
    machine_ref = db.collection("machines").where("assetNumber", "==", trimmed).limit(1)
    machines = list(machine_ref.stream())
    
    logging.info(f"Machine query snapshot size: {len(machines)}")
    
    if machines:
        doc = machines[0]
        machine = doc.to_dict()
        logging.info(f"Found machine document ID: {doc.id} with assetNumber: {machine.get('assetNumber')}")
        logging.info("=== MACHINE DATA FETCH END ===\n")
        return machine
    
    logging.info(f"No machine found for assetNumber: {trimmed}")
    logging.info("=== MACHINE DATA FETCH END ===\n")
    return None

def fetch_maintenance_data(start_date, end_date, date_field="createdAt"):
    """Fetch and enrich maintenance data within a date range"""
    try:
        logging.info("\n=== MAINTENANCE DATA FETCH START ===")
        
        # Fetch basic maintenance records
        maintenance_records = fetch_maintenance_records(start_date, end_date, date_field)
        logging.info("Enriching maintenance records with related data...")
        
        # Debug log
        logging.info("\nMaintenance Records Machine Numbers:")
        for record in maintenance_records:
            if 'machineNumber' in record:
                logging.info(f"ID: {record.get('id')}, machineNumber: \"{record.get('machineNumber')}\", type: {type(record.get('machineNumber'))}")
        
        enriched_records = []
        for record in maintenance_records:
            logging.info(f"\nEnriching record ID: {record.get('id')}")
            
            # Fetch related data
            style_data = fetch_style_data(record.get('styleId')) if record.get('styleId') else None
            supervisor_data = fetch_supervisor_data(record.get('supervisorId')) if record.get('supervisorId') else None
            production_line_data = fetch_production_line_data(record.get('productionLineId')) if record.get('productionLineId') else None
            machine_data = fetch_machine_data(record.get('machineNumber')) if record.get('machineNumber') else None
            
            # Convert totalDowntime from milliseconds to minutes if available
            downtime_min = record.get('totalDowntime') / 60000 if isinstance(record.get('totalDowntime'), (int, float)) else None
            
            # Create enriched record
            enriched_record = {
                **record,
                'downtime_min': downtime_min,
                'styleData': style_data,
                'supervisorData': supervisor_data,
                'productionLineData': production_line_data,
                'machineData': machine_data
            }
            
            enriched_records.append(enriched_record)
            logging.info(f"Record ID {record.get('id')} enrichment complete. Machine data found: {'Yes' if machine_data else 'No'}")
        
        logging.info(f"Enrichment complete. Total enriched records: {len(enriched_records)}")
        logging.info("=== MAINTENANCE DATA FETCH END ===\n")
        return enriched_records
    
    except Exception as e:
        logging.error(f"ERROR in fetch_maintenance_data: {str(e)}")
        raise Exception(f"Failed to fetch maintenance data: {str(e)}")

def save_to_json(data, filename):
    """Save data to a JSON file"""
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2, cls=DateTimeEncoder)
    logging.info(f"Data saved to {filename}")

if __name__ == "__main__":
    # Define date range for last 30 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    logging.info("\n=== TEST RUN START ===")
    logging.info(f"Date range: {start_date.isoformat()} to {end_date.isoformat()}")
    
    try:
        # Fetch data
        data = fetch_maintenance_data(start_date, end_date)
        logging.info(f"Fetched maintenance records: {len(data)}")
        
        # Log sample record
        if data:
            sample = data[0]
            sample_info = {
                'id': sample.get('id'),
                'downtime': sample.get('downtime_min'),
                'machineNumber': sample.get('machineNumber'),
                'purchaseDate': sample.get('machineData', {}).get('purchaseDate', 'Not Available'),
                'hasStyle': bool(sample.get('styleData')),
                'hasSupervisor': bool(sample.get('supervisorData')),
                'hasLine': bool(sample.get('productionLineData')),
                'hasMachine': bool(sample.get('machineData'))
            }
            logging.info(f"Sample record: {json.dumps(sample_info, indent=2)}")
        
        # Save data to JSON file
        save_to_json(data, 'maintenance_data.json')
        logging.info("=== TEST RUN END ===\n")
        
    except Exception as e:
        logging.error(f"TEST ERROR: {str(e)}")