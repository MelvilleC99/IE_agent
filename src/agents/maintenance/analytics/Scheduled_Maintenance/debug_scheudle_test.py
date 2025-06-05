#!/usr/bin/env python3
"""
Debug script to find exactly why the machine clustering is not identifying machines for maintenance.
This will test both the clustering analysis AND the interpreter step by step.
"""

import os
import sys

current_file = os.path.abspath(__file__)
project_root = os.path.abspath(os.path.join(current_file, "../../../../.."))
src_path = os.path.join(project_root, 'src')
if project_root not in sys.path:
    sys.path.insert(0, project_root)
if src_path not in sys.path:
    sys.path.insert(0, src_path)

print(f"[DEBUG] sys.path: {sys.path}")
print(f"[DEBUG] src_path: {src_path}")
print(f"[DEBUG] project_root: {project_root}")

# Load environment variables from .env.local
from dotenv import load_dotenv
env_path = os.path.join(project_root, '.env.local')
print(f"[DEBUG] Attempting to load env file: {env_path}")
print(f"[DEBUG] File exists: {os.path.exists(env_path)}")
if os.path.exists(env_path):
    with open(env_path, 'r') as f:
        for i, line in enumerate(f):
            if i >= 5:
                break
            if 'SUPABASE' in line:
                print(f"[DEBUG] .env.local line: {line.strip().split('=')[0]}=***REDACTED***")
            else:
                print(f"[DEBUG] .env.local line: {line.strip()}")
load_dotenv(dotenv_path=env_path, override=True)
print("[DEBUG] SUPABASE_URL:", os.getenv("SUPABASE_URL"))
print("[DEBUG] SUPABASE_KEY:", os.getenv("SUPABASE_KEY"))

import logging
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from collections import Counter

# Configure detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("cluster_debug")

def debug_machine_clustering():
    """Debug the machine clustering step by step."""
    
    print("=" * 80)
    print("MACHINE CLUSTERING ANALYSIS DEBUG")
    print("=" * 80)
    
    # Step 1: Get the same data the workflow is using
    print("\n1. Retrieving maintenance records...")
    
    try:
        from shared_services.supabase_client import get_shared_supabase_client
        
        db = get_shared_supabase_client()
        
        # Use the same date range as your log shows
        start_date = datetime(2024, 11, 1)
        end_date = datetime(2025, 5, 1, 23, 59, 59, 999999)
        
        print(f"   📅 Date range: {start_date} to {end_date}")
        
        # Try to replicate the exact query from your workflow
        filters = {
            'resolved_at.gte': start_date.isoformat(),
            'resolved_at.lte': end_date.isoformat()
        }
        
        records = db.query_table(
            table_name="downtime_detail",
            columns="*",
            filters=filters,
            limit=1000
        )
        
        print(f"   📊 Retrieved {len(records)} records")
        
        if not records:
            print("   ❌ No records found! This is the problem.")
            return
        
        # Analyze the data structure
        print(f"   📝 Sample record keys: {list(records[0].keys())}")
        
        # Check data quality
        machines = [str(r.get('machine_number')) for r in records if r.get('machine_number') is not None]
        if machines:
            print(f"   🏭 Unique machines in data: {len(set(machines))}")
            print(f"   🏭 Machine numbers: {sorted(set(machines))[:10]}...")
        else:
            print("   🏭 No valid machine numbers found.")
        
        # Check failure reasons
        reasons = [r.get('reason') for r in records if r.get('reason')]
        reason_counts = Counter(reasons)
        print(f"   🔧 Top failure reasons: {dict(list(reason_counts.most_common(5)))}")
        
        # Check date distribution
        dates = [str(r.get('resolved_at')) for r in records if r.get('resolved_at') is not None]
        if dates:
            print(f"   📅 Date range in data: {min(dates)} to {max(dates)}")
        else:
            print("   📅 No valid dates found in data.")
        
    except Exception as e:
        print(f"   ❌ Error retrieving records: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Step 2: Test the clustering analysis
    print(f"\n2. Testing MachineCluster.run_analysis...")
    
    try:
        # Import the clustering module
        from agents.maintenance.analytics.Scheduled_Maintenance.MachineCluster import run_analysis
        
        print("   🔄 Running clustering analysis...")
        analysis_results = run_analysis(records)
        
        print(f"   📊 Analysis results type: {type(analysis_results)}")
        
        if analysis_results is None:
            print("   ❌ Analysis returned None!")
            return
        
        if isinstance(analysis_results, dict):
            print(f"   📊 Analysis result keys: {list(analysis_results.keys())}")
            
            # Print details of each key
            for key, value in analysis_results.items():
                print(f"   📈 {key}:")
                print(f"      Type: {type(value)}")
                if isinstance(value, (list, tuple)):
                    print(f"      Length: {len(value)}")
                    if value and len(value) > 0:
                        print(f"      Sample: {value[0] if len(str(value[0])) < 100 else str(value[0])[:100] + '...'}")
                elif isinstance(value, dict):
                    print(f"      Keys: {list(value.keys())}")
                else:
                    print(f"      Value: {str(value)[:200]}")
        
        else:
            print(f"   📊 Analysis result: {str(analysis_results)[:200]}")
        
    except Exception as e:
        print(f"   ❌ Clustering analysis failed: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Step 3: Test the interpreter
    print(f"\n3. Testing machine_cluster_interpreter.interpret_results...")
    
    try:
        from agents.maintenance.analytics.Scheduled_Maintenance.machine_cluster_interpreter import interpret_results
        
        print("   🔄 Running result interpretation...")
        machines_to_service = interpret_results(analysis_results)
        
        print(f"   📊 Machines to service type: {type(machines_to_service)}")
        print(f"   📊 Number of machines to service: {len(machines_to_service) if machines_to_service else 0}")
        
        if machines_to_service:
            print("   ✅ Machines identified for service:")
            for i, machine in enumerate(machines_to_service[:5]):  # Show first 5
                print(f"     {i+1}. Machine: {machine}")
        else:
            print("   ❌ No machines identified for service!")
            print("   🔍 Let's debug the interpretation logic...")
            
            # Debug the interpretation step by step
            debug_interpreter_logic(analysis_results, records)
    
    except Exception as e:
        print(f"   ❌ Interpretation failed: {e}")
        import traceback
        traceback.print_exc()
        return

def debug_interpreter_logic(analysis_results, records):
    """Debug the interpreter logic step by step."""
    
    print("\n   🔍 DEBUGGING INTERPRETER LOGIC")
    print("   " + "-" * 40)
    
    # Check what the interpreter expects
    print("   📋 Analysis results structure:")
    if isinstance(analysis_results, dict):
        for key, value in analysis_results.items():
            print(f"      {key}: {type(value)}")
            
            # Look for clustering results
            if 'cluster' in key.lower() or 'label' in key.lower():
                if isinstance(value, (list, tuple)):
                    print(f"         Length: {len(value)}")
                    if value:
                        unique_values = list(set(value)) if isinstance(value, list) else [value]
                        print(f"         Unique values: {unique_values}")
    
    # Try to manually identify problematic machines
    print("\n   🔧 Manual machine analysis:")
    
    # Group by machine and count failures
    machine_failures = {}
    for record in records:
        machine_id = record.get('machine_number')
        if machine_id:
            if machine_id not in machine_failures:
                machine_failures[machine_id] = {
                    'count': 0,
                    'reasons': [],
                    'machine_type': record.get('machine_type'),
                    'total_downtime': 0
                }
            
            machine_failures[machine_id]['count'] += 1
            machine_failures[machine_id]['reasons'].append(record.get('reason'))
            
            # Add downtime if available
            downtime = record.get('total_downtime')
            if downtime:
                try:
                    machine_failures[machine_id]['total_downtime'] += float(downtime)
                except (ValueError, TypeError):
                    pass
    
    # Sort by failure count
    sorted_machines = sorted(machine_failures.items(), key=lambda x: x[1]['count'], reverse=True)
    
    print(f"   📊 Top 10 machines by failure count:")
    for machine_id, data in sorted_machines[:10]:
        print(f"      {machine_id}: {data['count']} failures, {data['machine_type']}, {data['total_downtime']:.1f} min downtime")
    
    # Check if any machines meet typical maintenance criteria
    high_failure_threshold = 3  # Machines with 3+ failures
    high_failure_machines = [m for m, d in machine_failures.items() if d['count'] >= high_failure_threshold]
    
    print(f"\n   🚨 Machines with {high_failure_threshold}+ failures: {len(high_failure_machines)}")
    for machine_id in high_failure_machines[:5]:
        data = machine_failures[machine_id]
        print(f"      {machine_id}: {data['count']} failures")
    
    # Check if the interpreter has the right thresholds
    print(f"\n   🎯 Possible issues with interpreter:")
    print(f"      - Threshold too high? (try lowering failure count threshold)")
    print(f"      - Missing clustering results? (check if clustering produces the expected format)")
    print(f"      - Wrong key names? (interpreter might be looking for different keys)")

def test_clustering_manually():
    """Test basic clustering manually to see what should happen."""
    
    print("\n4. Testing manual clustering approach...")
    
    try:
        from shared_services.supabase_client import get_shared_supabase_client
        
        db = get_shared_supabase_client()
        
        # Get recent data
        end_date = datetime.now()
        start_date = end_date - timedelta(days=60)  # Last 60 days
        
        filters = {
            'resolved_at.gte': start_date.isoformat(),
            'resolved_at.lte': end_date.isoformat()
        }
        
        records = db.query_table(
            table_name="downtime_detail",
            columns="*",
            filters=filters,
            limit=1000
        )
        
        print(f"   📊 Using {len(records)} records from last 60 days")
        
        # Simple manual clustering - group by machine and count issues
        machine_stats = {}
        
        for record in records:
            machine_id = record.get('machine_number')
            if not machine_id:
                continue
                
            if machine_id not in machine_stats:
                machine_stats[machine_id] = {
                    'failure_count': 0,
                    'total_downtime': 0,
                    'machine_type': record.get('machine_type', 'Unknown'),
                    'reasons': []
                }
            
            machine_stats[machine_id]['failure_count'] += 1
            machine_stats[machine_id]['reasons'].append(record.get('reason'))
            
            # Add downtime
            try:
                downtime = float(record.get('total_downtime', 0))
                machine_stats[machine_id]['total_downtime'] += downtime
            except (ValueError, TypeError):
                pass
        
        # Identify machines that should need maintenance
        # Criteria: 2+ failures OR high downtime
        machines_needing_maintenance = []
        
        for machine_id, stats in machine_stats.items():
            if stats['failure_count'] >= 2 or stats['total_downtime'] > 500:  # 500+ minutes downtime
                priority = 'high' if stats['failure_count'] >= 4 or stats['total_downtime'] > 1000 else 'medium'
                
                machines_needing_maintenance.append({
                    'machineNumber': machine_id,
                    'machine_type': stats['machine_type'],
                    'failure_count': stats['failure_count'],
                    'total_downtime': stats['total_downtime'],
                    'priority': priority,
                    'reasons': list(set(stats['reasons']))  # Unique reasons
                })
        
        print(f"   🔧 Machines that should need maintenance (manual criteria): {len(machines_needing_maintenance)}")
        
        if machines_needing_maintenance:
            print("   📋 Top candidates:")
            for machine in sorted(machines_needing_maintenance, key=lambda x: x['failure_count'], reverse=True)[:5]:
                print(f"      {machine['machineNumber']}: {machine['failure_count']} failures, {machine['total_downtime']:.1f} min, {machine['priority']} priority")
        
        return machines_needing_maintenance
        
    except Exception as e:
        print(f"   ❌ Manual clustering test failed: {e}")
        return []

if __name__ == "__main__":
    print("Starting comprehensive clustering debug...")
    debug_machine_clustering()
    
    print("\n" + "=" * 80)
    manual_results = test_clustering_manually()
    
    if manual_results:
        print(f"\n✅ CONCLUSION: There ARE machines that should need maintenance!")
        print(f"   The issue is likely in the MachineCluster.run_analysis() or interpret_results() functions.")
        print(f"   Expected {len(manual_results)} machines to be identified.")
    else:
        print(f"\n❌ CONCLUSION: Even manual analysis found no machines needing maintenance.")
        print(f"   This suggests either:")
        print(f"   1. The data really doesn't have problematic machines")
        print(f"   2. The criteria are too strict")
        print(f"   3. There's an issue with the data itself")