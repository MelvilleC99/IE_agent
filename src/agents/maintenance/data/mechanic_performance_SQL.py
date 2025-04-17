#!/usr/bin/env python3
import os
import sys
import json
import ast
from datetime import date
from dotenv import load_dotenv
from supabase.client import create_client, Client

# -------------------------------------------------------------------
# 1) Bootstrap project paths and environment
# -------------------------------------------------------------------
# Current file: src/agents/maintenance/data/mechanic_performance_SQL.py
current_dir = os.path.dirname(os.path.abspath(__file__))
# src/ directory
src_dir = os.path.abspath(os.path.join(current_dir, '../../../'))
# project root (one level above src)
project_root = os.path.abspath(os.path.join(src_dir, '../'))

# Add src to Python path so we can import config
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

# Load .env.local from project root
env_path = os.path.join(project_root, '.env.local')
print(f"Loading .env.local from: {env_path}")
print(f"File exists: {os.path.exists(env_path)}")
load_dotenv(dotenv_path=env_path)

# -------------------------------------------------------------------
# 2) Initialize Supabase client
# -------------------------------------------------------------------
supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_KEY')
if not supabase_url or not supabase_key:
    raise RuntimeError("SUPABASE_URL and SUPABASE_KEY must be set in .env.local")

supabase: Client = create_client(supabase_url, supabase_key)
print("Supabase client initialized.")

# -------------------------------------------------------------------
# 3) Load JSON data
# -------------------------------------------------------------------
json_path = os.path.join(project_root, 'summary_clean.json')
print(f"Reading JSON from: {json_path}")
if not os.path.exists(json_path):
    raise FileNotFoundError(f"summary_clean.json not found at {json_path}")
with open(json_path, 'r') as f:
    data = json.load(f)

# -------------------------------------------------------------------
# 4) Prepare period window
# -------------------------------------------------------------------
today = date.today().isoformat()
period_start = today
period_end = today

# Helper to parse keys like "('Bartack',)" → "Bartack"
def parse_tuple_key(key_str: str) -> str:
    try:
        val = ast.literal_eval(key_str)
        if isinstance(val, tuple) and val:
            return val[0]
    except Exception:
        pass
    return key_str.strip("()'")

# -------------------------------------------------------------------
# 5) Build insertion payload
# -------------------------------------------------------------------
rows = []
for context, content in data.items():
    if context == 'overall':
        for m in content.get('mechanic_stats', []):
            rows.append({
                'context': context,
                'dimension_1': None,
                'dimension_2': None,
                'mechanic_name': m['mechanicName'],
                'avg_repair_time_min': m['avgRepairTime_min'],
                'avg_response_time_min': m['avgResponseTime_min'],
                'pct_worse_than_best': m['pct_worse_than_best'],
                'is_best': False,
                'period_start_date': period_start,
                'period_end_date': period_end
            })
        best = content.get('best', {})
        rows.append({
            'context': context,
            'dimension_1': None,
            'dimension_2': None,
            'mechanic_name': best.get('mechanicName'),
            'avg_repair_time_min': best.get('avgRepairTime_min'),
            'avg_response_time_min': best.get('avgResponseTime_min'),
            'pct_worse_than_best': None,
            'is_best': True,
            'period_start_date': period_start,
            'period_end_date': period_end
        })
    elif context == 'byMachineType':
        for key, grp in content.items():
            dim1 = parse_tuple_key(key)
            for m in grp.get('mechanic_stats', []):
                rows.append({
                    'context': context,
                    'dimension_1': dim1,
                    'dimension_2': None,
                    'mechanic_name': m['mechanicName'],
                    'avg_repair_time_min': m['avgRepairTime_min'],
                    'avg_response_time_min': m['avgResponseTime_min'],
                    'pct_worse_than_best': m['pct_worse_than_best'],
                    'is_best': False,
                    'period_start_date': period_start,
                    'period_end_date': period_end
                })
            best = grp.get('best', {})
            rows.append({
                'context': context,
                'dimension_1': dim1,
                'dimension_2': None,
                'mechanic_name': best.get('mechanicName'),
                'avg_repair_time_min': best.get('avgRepairTime_min'),
                'avg_response_time_min': best.get('avgResponseTime_min'),
                'pct_worse_than_best': None,
                'is_best': True,
                'period_start_date': period_start,
                'period_end_date': period_end
            })
    elif context == 'byFailureReason':
        for key, grp in content.items():
            dim1 = parse_tuple_key(key)
            for m in grp.get('mechanic_stats', []):
                rows.append({
                    'context': context,
                    'dimension_1': dim1,
                    'dimension_2': None,
                    'mechanic_name': m['mechanicName'],
                    'avg_repair_time_min': m['avgRepairTime_min'],
                    'avg_response_time_min': m['avgResponseTime_min'],
                    'pct_worse_than_best': m['pct_worse_than_best'],
                    'is_best': False,
                    'period_start_date': period_start,
                    'period_end_date': period_end
                })
            best = grp.get('best', {})
            rows.append({
                'context': context,
                'dimension_1': dim1,
                'dimension_2': None,
                'mechanic_name': best.get('mechanicName'),
                'avg_repair_time_min': best.get('avgRepairTime_min'),
                'avg_response_time_min': best.get('avgResponseTime_min'),
                'pct_worse_than_best': None,
                'is_best': True,
                'period_start_date': period_start,
                'period_end_date': period_end
            })
    elif context == 'byMachineAndReason':
        for machine, reasons in content.items():
            dim1 = parse_tuple_key(machine)
            for reason, grp in reasons.items():
                for m in grp.get('mechanic_stats', []):
                    rows.append({
                        'context': context,
                        'dimension_1': dim1,
                        'dimension_2': reason,
                        'mechanic_name': m['mechanicName'],
                        'avg_repair_time_min': m['avgRepairTime_min'],
                        'avg_response_time_min': m['avgResponseTime_min'],
                        'pct_worse_than_best': m['pct_worse_than_best'],
                        'is_best': False,
                        'period_start_date': period_start,
                        'period_end_date': period_end
                    })
                best = grp.get('best', {})
                rows.append({
                    'context': context,
                    'dimension_1': dim1,
                    'dimension_2': reason,
                    'mechanic_name': best.get('mechanicName'),
                    'avg_repair_time_min': best.get('avgRepairTime_min'),
                    'avg_response_time_min': best.get('avgResponseTime_min'),
                    'pct_worse_than_best': None,
                    'is_best': True,
                    'period_start_date': period_start,
                    'period_end_date': period_end
                })

# -------------------------------------------------------------------
# 6) Bulk insert into Supabase
# -------------------------------------------------------------------
print(f"Inserting {len(rows)} rows into 'mechanic_performance'...")
result = supabase.table('mechanic_performance').insert(rows).execute()
if hasattr(result, 'data') and result.data is not None:
    print(f"✅ Successfully inserted {len(result.data)} rows.")
else:
    print("❌ Insert may have failed. Server response:", result)
