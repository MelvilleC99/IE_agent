#!/usr/bin/env python3
import sys
import os
import json
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(current_dir, "../../../"))
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

# Load environment
load_dotenv(Path(__file__).resolve().parents[3] / ".env.local")

from shared_services.db_client import get_connection
import firebase_admin
from firebase_admin import credentials, firestore

class DailyPerformanceMeasurement:
    """
    Daily Performance Measurement Script
    Uses mechanic_id from tasks to filter Firestore records.
    """
    def __init__(self):
        self.today = datetime.now().date()
        print(f"DAILY: Running for {self.today}")
        # 24h window as datetime objects
        self.time_window_start = datetime.combine(self.today - timedelta(days=1), datetime.min.time())
        self.time_window_end   = datetime.combine(self.today,             datetime.min.time())

        # Supabase
        self.supabase = get_connection()
        print("DAILY: Connected to Supabase")

        # Firebase Admin SDK init
        try:
            if not firebase_admin._apps:
                cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
                if not cred_path or not os.path.exists(cred_path):
                    print(f"DAILY: ERROR - credentials not found at {cred_path}")
                    self.db = None
                    return
                cred = credentials.Certificate(cred_path)
                firebase_admin.initialize_app(cred)
            self.db = firestore.client()
            print("DAILY: Connected to Firebase Admin SDK")
        except Exception as e:
            print(f"DAILY: Firebase initialization error: {e}")
            self.db = None

    def get_baseline_measurement(self, task_id):
        res = (self.supabase.table('measurements')
                     .select('*')
                     .eq('task_id', task_id)
                     .order('measurement_date')
                     .limit(1)
                     .execute())
        return res.data[0] if res.data else None

    def query_firebase_data(self, task):
        """Query Firestore using the numeric mechanic_id"""
        if not self.db:
            print(f"DAILY: Firebase not connected for task ID {task.get('id')}")
            return []

        mech_id    = task.get('mechanic_id')
        issue_type = task.get('issue_type')
        coll = self.db.collection('machineDowntimes')
        query = (coll
                 .where('status', '==', 'Closed')
                 .where('updatedAt', '>=', self.time_window_start)
                 .where('updatedAt', '<',  self.time_window_end)
                 .where('mechanicId', '==', mech_id))
        docs = query.get()

        # Further filter by machine_type/reason if needed
        results = []
        for doc in docs:
            data = doc.to_dict() or {}
            if issue_type == 'repair_time' and task.get('machine_type'):
                if data.get('machineType') != task.get('machine_type'):
                    continue
                if task.get('reason') and data.get('reason') != task.get('reason'):
                    continue
            results.append(data)

        print(f"DAILY: Found {len(results)} records for mechanic_id={mech_id}")
        return results

    def calculate_metrics(self, firebase_data, issue_type):
        if not firebase_data:
            return {'value': None, 'count': 0, 'min': None, 'max': None}
        vals = []
        key = 'totalResponseTime' if issue_type == 'response_time' else 'totalRepairTime'
        for d in firebase_data:
            if key in d:
                vals.append(d[key] / 60000)
        if not vals:
            return {'value': None, 'count': 0, 'min': None, 'max': None}
        avg = sum(vals) / len(vals)
        return {'value': round(avg,2), 'count': len(vals), 'min': round(min(vals),2), 'max': round(max(vals),2)}

    def record_measurement(self, task, metrics, baseline):
        if metrics['value'] is None:
            print(f"DAILY: No valid metrics for task {task.get('id')}")
            return None
        curr = metrics['value']
        change = ((curr - baseline) / baseline * 100) if baseline else 0
        improved = change < 0 if task.get('issue_type') in ['response_time','repair_time'] else change > 0
        notes = f"Daily measurement for mechanic {task.get('mechanic_id')}. Based on {metrics['count']} instances."
        mdata = {'task_id': task['id'], 'measurement_date': self.today.isoformat(),
                 'value': curr, 'change_pct': round(change,2), 'is_improved': improved, 'notes': notes}
        res = self.supabase.table('measurements').insert(mdata).execute()
        if res.data:
            mid = res.data[0]['id']
            print(f"DAILY: Measurement ID {mid} created for task {task.get('id')}")
            return res.data[0]
        print(f"DAILY: ERROR creating measurement for task {task.get('id')}")
        return None

    def process_task(self, task):
        print(f"DAILY: Processing task {task.get('id')}: {task.get('title')}")
        base = self.get_baseline_measurement(task.get('id'))
        if not base:
            print("DAILY: No baseline, skipping")
            return None
        data = self.query_firebase_data(task)
        if not data:
            print("DAILY: No data for task, skipping")
            return {'task_id': task.get('id'), 'status':'skipped'}
        metrics = self.calculate_metrics(data, task.get('issue_type'))
        rec = self.record_measurement(task, metrics, base['value'])
        if rec:
            return {'task_id': task.get('id'), 'status':'measured', 'measurement_id':rec['id'],
                    'value':metrics['value'],'count':metrics['count'],'change_pct':rec['change_pct'],'is_improved':rec['is_improved']}
        return {'task_id': task.get('id'), 'status':'failed'}

    def process_tasks(self, tasks):
        print(f"DAILY: Processing {len(tasks)} tasks")
        results = [self.process_task(t) for t in tasks]
        print(f"DAILY: Completed with {sum(1 for r in results if r and r['status']=='measured')} measured")
        return results

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--task-file')
    args = parser.parse_args()
    proc = DailyPerformanceMeasurement()
    if args.task_file and os.path.exists(args.task_file):
        with open(args.task_file) as f:
            ts = json.load(f)
        proc.process_tasks(ts)
    else:
        print("Run with --task-file")
