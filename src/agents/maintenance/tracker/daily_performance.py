import sys
import os
from datetime import datetime, timedelta

# Add src to path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(current_dir, "../../../"))
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

# Import dependencies
from shared_services.db_client import get_connection
import firebase_admin
from firebase_admin import credentials, firestore

class DailyPerformanceMeasurement:
    """
    Daily Performance Measurement Script
    
    Processes tasks for daily measurement:
    1. Retrieves current performance data from Firebase
    2. Calculates metrics based on issue type
    3. Records measurements in the database
    """
    
    def __init__(self, firebase_credentials_path=None):
        """Initialize the daily performance measurement script"""
        # Set today's date
        self.today = datetime.now().date()
        print(f"DAILY: Running for {self.today}")
        
        # Set time window for daily measurement (last 24 hours)
        self.time_window_start = datetime.combine(self.today - timedelta(days=1), 
                                                 datetime.min.time()).timestamp() * 1000
        self.time_window_end = datetime.combine(self.today, 
                                               datetime.min.time()).timestamp() * 1000
        
        # Initialize database connections
        self.supabase = get_connection()
        print("DAILY: Connected to Supabase")
        
        # Initialize Firebase
        try:
            # If Firebase is already initialized, just get the client
            if firebase_admin._apps:
                self.db = firestore.client()
                print("DAILY: Using existing Firebase connection")
            else:
                # Initialize with credentials
                if firebase_credentials_path and os.path.exists(firebase_credentials_path):
                    cred = credentials.Certificate(firebase_credentials_path)
                    firebase_admin.initialize_app(cred)
                else:
                    firebase_admin.initialize_app()
                self.db = firestore.client()
                print("DAILY: Connected to Firebase")
        except Exception as e:
            print(f"DAILY: Firebase initialization error: {e}")
            self.db = None
    
    def get_baseline_measurement(self, task_id):
        """Get the baseline (first) measurement for a task"""
        result = self.supabase.table('measurements').select('*').eq('task_id', task_id) \
                             .order('measurement_date').limit(1).execute()
        
        if result.data:
            return result.data[0]
        return None
    
    def query_firebase_data(self, task):
        """Query Firebase for performance data related to the task"""
        if not self.db:
            print(f"DAILY: Firebase not connected for task ID {task['id']}")
            return []
        
        # Get task details
        entity_id = task['entity_id']
        issue_type = task['issue_type']
        
        # Query the machineDowntimes collection
        collection_ref = self.db.collection('machineDowntimes')
        
        # Build query for closed records in the time window
        query = collection_ref.where('status', '==', 'Closed') \
                             .where('updatedAt', '>=', self.time_window_start) \
                             .where('updatedAt', '<', self.time_window_end)
        
        # Execute query
        documents = query.get()
        
        # Filter results by mechanic ID or name
        results = []
        for doc in documents:
            data = doc.to_dict()
            if not isinstance(data, dict):
                continue
                
            # Check if this document is for the mechanic we're interested in
            if (data.get('mechanicId') == entity_id or 
                data.get('mechanicName') == entity_id):
                
                # For specific issues, also filter by machine type and reason
                if issue_type == 'repair_time' and 'machine_type' in task and 'reason' in task:
                    # Only include if machine type and reason match
                    if (data.get('machineType') == task.get('machine_type') and 
                        data.get('reason') == task.get('reason')):
                        results.append(data)
                # For general repair time by machine type
                elif issue_type == 'repair_time' and 'machine_type' in task:
                    # Only include if machine type matches
                    if data.get('machineType') == task.get('machine_type'):
                        results.append(data)
                # For general repair or response time
                else:
                    results.append(data)
        
        print(f"DAILY: Found {len(results)} relevant records for task ID {task['id']}")
        return results
    
    def calculate_metrics(self, firebase_data, issue_type):
        """Calculate performance metrics based on Firebase data"""
        if not firebase_data:
            return {
                'value': None,
                'count': 0,
                'min': None,
                'max': None
            }
        
        # Initialize values
        total = 0
        count = len(firebase_data)
        values = []
        
        # Process each document
        for doc in firebase_data:
            if issue_type == 'response_time':
                # Convert milliseconds to minutes
                if 'totalResponseTime' in doc:
                    value_ms = doc['totalResponseTime']
                    value_min = value_ms / 60000  # Convert ms to minutes
                    total += value_min
                    values.append(value_min)
            elif issue_type == 'repair_time':
                # Convert milliseconds to minutes
                if 'totalRepairTime' in doc:
                    value_ms = doc['totalRepairTime']
                    value_min = value_ms / 60000  # Convert ms to minutes
                    total += value_min
                    values.append(value_min)
        
        # Calculate average
        average = total / count if count > 0 else None
        
        # Calculate min and max
        min_value = min(values) if values else None
        max_value = max(values) if values else None
        
        return {
            'value': round(average, 2) if average is not None else None,
            'count': count,
            'min': round(min_value, 2) if min_value is not None else None,
            'max': round(max_value, 2) if max_value is not None else None
        }
    
    def record_measurement(self, task, metrics, baseline_value):
        """Record a new measurement in the database"""
        if metrics['value'] is None:
            print(f"DAILY: No valid metrics to record for task ID {task['id']}")
            return None
        
        # Calculate change percentage from baseline
        current_value = metrics['value']
        
        if baseline_value == 0:
            change_pct = 0  # Avoid division by zero
        else:
            change_pct = ((current_value - baseline_value) / baseline_value) * 100
        
        # Determine if there is improvement
        # For time metrics (response_time, repair_time), lower is better
        issue_type = task['issue_type']
        is_improved = change_pct < 0 if issue_type in ['response_time', 'repair_time'] else change_pct > 0
        
        # Create measurement notes with additional context
        notes = f"Daily measurement for {task['entity_id']}. "
        notes += f"Based on {metrics['count']} instances. "
        
        if metrics['min'] is not None and metrics['max'] is not None:
            notes += f"Range: {metrics['min']} to {metrics['max']} minutes."
        
        # Create the measurement record
        measurement_data = {
            'task_id': task['id'],
            'measurement_date': self.today.isoformat(),
            'value': current_value,
            'change_pct': round(change_pct, 2),
            'is_improved': is_improved,
            'notes': notes
        }
        
        # Insert the measurement
        measurement_result = self.supabase.table('measurements').insert(measurement_data).execute()
        
        if measurement_result.data:
            measurement_id = measurement_result.data[0]['id']
            print(f"DAILY: Created measurement ID {measurement_id} for task ID {task['id']}")
            
            # Check if we should send a notification based on this measurement
            # Big improvements or deteriorations warrant notifications
            if abs(change_pct) > 10:
                direction = "improvement" if is_improved else "deterioration"
                notification = f"ALERT: {task['entity_id']} shows significant {direction} of {abs(round(change_pct, 1))}% in {issue_type}"
                print(f"DAILY: Would send notification: {notification}")
            
            return measurement_result.data[0]
        else:
            print(f"DAILY: ERROR creating measurement for task ID {task['id']}")
            return None
    
    def process_task(self, task):
        """Process a single task for daily measurement"""
        task_id = task['id']
        print(f"DAILY: Processing task ID {task_id}: {task['title']}")
        
        # Get baseline measurement
        baseline = self.get_baseline_measurement(task_id)
        if not baseline:
            print(f"DAILY: ERROR - No baseline measurement found for task ID {task_id}")
            return None
        
        baseline_value = baseline['value']
        
        # Query Firebase for current performance data
        firebase_data = self.query_firebase_data(task)
        
        # If no data found, log but don't record a measurement
        if not firebase_data:
            print(f"DAILY: No performance data found for task ID {task_id}, skipping measurement")
            return {
                'task_id': task_id,
                'status': 'skipped',
                'reason': 'no_data'
            }
        
        # Calculate metrics based on the data
        metrics = self.calculate_metrics(firebase_data, task['issue_type'])
        
        # Record the measurement
        measurement = self.record_measurement(task, metrics, baseline_value)
        
        if measurement:
            return {
                'task_id': task_id,
                'status': 'measured',
                'measurement_id': measurement['id'],
                'value': metrics['value'],
                'count': metrics['count'],
                'change_pct': measurement['change_pct'],
                'is_improved': measurement['is_improved']
            }
        else:
            return {
                'task_id': task_id,
                'status': 'failed',
                'reason': 'recording_failed'
            }
    
    def process_tasks(self, tasks):
        """Process a list of tasks for daily measurement"""
        if not tasks:
            print("DAILY: No tasks to process")
            return []
        
        print(f"DAILY: Processing {len(tasks)} tasks for daily measurement")
        
        results = []
        for task in tasks:
            result = self.process_task(task)
            if result:
                results.append(result)
        
        # Print summary
        successful = len([r for r in results if r['status'] == 'measured'])
        skipped = len([r for r in results if r['status'] == 'skipped'])
        failed = len([r for r in results if r['status'] == 'failed'])
        
        print("\nDAILY: Daily Performance Measurement Summary:")
        print(f"- Successfully measured: {successful}")
        print(f"- Skipped (no data): {skipped}")
        print(f"- Failed: {failed}")
        
        return results


# For testing the script directly
if __name__ == '__main__':
    import json
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Process daily performance measurements')
    parser.add_argument('--task-file', help='Path to JSON file with tasks')
    parser.add_argument('--credentials', help='Path to Firebase credentials')
    args = parser.parse_args()
    
    # Initialize with Firebase credentials if provided
    firebase_creds = args.credentials
    
    # Create the processor
    processor = DailyPerformanceMeasurement(firebase_creds)
    
    # Process tasks from file if provided
    if args.task_file and os.path.exists(args.task_file):
        with open(args.task_file, 'r') as f:
            tasks = json.load(f)
        
        if tasks:
            processor.process_tasks(tasks)
        else:
            print("No tasks found in file")
    else:
        print("This script processes tasks passed to it from the task monitor.")
        print("Run with --task-file to specify a JSON file with tasks.")