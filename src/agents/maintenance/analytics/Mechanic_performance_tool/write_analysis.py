#!/usr/bin/env python3
import sys
import os
import json
from datetime import datetime, timedelta, date
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

class AnalysisWriter:
    """
    Writes mechanic performance analysis results to the database for historical reference.
    """
    
    def __init__(self):
        """Initialize the analysis writer"""
        try:
            self.supabase = get_connection()
            print("WRITER: Connected to database successfully")
        except Exception as e:
            print(f"WRITER: Error connecting to database: {e}")
            self.supabase = None
            
        self.today = datetime.now().date()
        
    def write_analysis_results(self, analysis_results, period_start_date=None, period_end_date=None):
        """
        Write analysis results to the mechanic_performance table
        
        Args:
            analysis_results: Dictionary containing analysis results
            period_start_date: Start date for the analysis period (defaults to today)
            period_end_date: End date for the analysis period (defaults to today)
            
        Returns:
            list: Records that were successfully written to the database
        """
        if not self.supabase:
            print("WRITER: No database connection available")
            return []
        
        if not analysis_results:
            print("WRITER: No analysis results provided")
            return []
            
        # Set default dates if not provided
        if not period_start_date:
            period_start_date = self.today
        if not period_end_date:
            period_end_date = self.today
            
        # Convert dates to ISO format strings if they are date objects
        if isinstance(period_start_date, (datetime, date)):
            period_start_date = period_start_date.isoformat()
        if isinstance(period_end_date, (datetime, date)):
            period_end_date = period_end_date.isoformat()
            
        print(f"WRITER: Writing analysis results for period {period_start_date} to {period_end_date}")
        
        # Records to be inserted or upserted
        records = []
        
        try:
            # Process overall results
            if 'overall_response' in analysis_results and 'mechanic_stats' in analysis_results['overall_response']:
                overall_stats = analysis_results['overall_response']['mechanic_stats']
                
                # Determine the best mechanic
                best_mechanic = None
                best_repair_time = float('inf')
                
                for mechanic in overall_stats:
                    repair_time = mechanic.get('avgRepairTime_min', 0)
                    if repair_time < best_repair_time and repair_time > 0:
                        best_repair_time = repair_time
                        best_mechanic = mechanic.get('mechanicName')
                
                # Create records for each mechanic
                for mechanic in overall_stats:
                    mechanic_name = mechanic.get('mechanicName')
                    
                    # Calculate percentage worse than best
                    pct_worse = None
                    if best_repair_time and best_repair_time > 0:
                        repair_time = mechanic.get('avgRepairTime_min', 0)
                        if repair_time > 0:
                            pct_worse = ((repair_time - best_repair_time) / best_repair_time) * 100
                    
                    record = {
                        'context': 'overall',
                        'dimension_1': None,
                        'dimension_2': None,
                        'mechanic_name': mechanic_name,
                        'avg_repair_time_min': mechanic.get('avgRepairTime_min'),
                        'avg_response_time_min': mechanic.get('avgResponseTime_min'),
                        'pct_worse_than_best': pct_worse,
                        'is_best': mechanic_name == best_mechanic,
                        'period_start_date': period_start_date,
                        'period_end_date': period_end_date
                    }
                    records.append(record)
            
            # Process machine type dimension
            if 'machine_repair' in analysis_results:
                for machine_type, machine_data in analysis_results['machine_repair'].items():
                    if not machine_data or 'mechanic_stats' not in machine_data:
                        continue
                        
                    mechanic_stats = machine_data['mechanic_stats']
                    
                    # Determine best mechanic for this machine type
                    best_mechanic = None
                    best_repair_time = float('inf')
                    
                    for mechanic in mechanic_stats:
                        repair_time = mechanic.get('avgRepairTime_min', 0)
                        if repair_time < best_repair_time and repair_time > 0:
                            best_repair_time = repair_time
                            best_mechanic = mechanic.get('mechanicName')
                    
                    # Create records for each mechanic
                    for mechanic in mechanic_stats:
                        mechanic_name = mechanic.get('mechanicName')
                        
                        # Calculate percentage worse than best
                        pct_worse = None
                        if best_repair_time and best_repair_time > 0:
                            repair_time = mechanic.get('avgRepairTime_min', 0)
                            if repair_time > 0:
                                pct_worse = ((repair_time - best_repair_time) / best_repair_time) * 100
                        
                        record = {
                            'context': 'byMachineType',
                            'dimension_1': machine_type,
                            'dimension_2': None,
                            'mechanic_name': mechanic_name,
                            'avg_repair_time_min': mechanic.get('avgRepairTime_min'),
                            'avg_response_time_min': mechanic.get('avgResponseTime_min'),
                            'pct_worse_than_best': pct_worse,
                            'is_best': mechanic_name == best_mechanic,
                            'period_start_date': period_start_date,
                            'period_end_date': period_end_date
                        }
                        records.append(record)
            
            # Process failure reason dimension
            if 'byFailureReason' in analysis_results:
                for reason, reason_data in analysis_results['byFailureReason'].items():
                    if not reason_data or 'mechanic_stats' not in reason_data:
                        continue
                        
                    mechanic_stats = reason_data['mechanic_stats']
                    
                    # Determine best mechanic for this reason
                    best_mechanic = None
                    best_repair_time = float('inf')
                    
                    for mechanic in mechanic_stats:
                        repair_time = mechanic.get('avgRepairTime_min', 0)
                        if repair_time < best_repair_time and repair_time > 0:
                            best_repair_time = repair_time
                            best_mechanic = mechanic.get('mechanicName')
                    
                    # Create records for each mechanic
                    for mechanic in mechanic_stats:
                        mechanic_name = mechanic.get('mechanicName')
                        
                        # Calculate percentage worse than best
                        pct_worse = None
                        if best_repair_time and best_repair_time > 0:
                            repair_time = mechanic.get('avgRepairTime_min', 0)
                            if repair_time > 0:
                                pct_worse = ((repair_time - best_repair_time) / best_repair_time) * 100
                        
                        record = {
                            'context': 'byFailureReason',
                            'dimension_1': reason,
                            'dimension_2': None,
                            'mechanic_name': mechanic_name,
                            'avg_repair_time_min': mechanic.get('avgRepairTime_min'),
                            'avg_response_time_min': mechanic.get('avgResponseTime_min'),
                            'pct_worse_than_best': pct_worse,
                            'is_best': mechanic_name == best_mechanic,
                            'period_start_date': period_start_date,
                            'period_end_date': period_end_date
                        }
                        records.append(record)
            
            # Process machine and reason combination dimension
            if 'machine_reason_repair' in analysis_results:
                for combo_key, combo_data in analysis_results['machine_reason_repair'].items():
                    if not combo_data or 'mechanic_stats' not in combo_data:
                        continue
                        
                    machine_type = combo_data.get('machine_type', 'Unknown')
                    reason = combo_data.get('reason', 'Unknown')
                    mechanic_stats = combo_data['mechanic_stats']
                    
                    # Determine best mechanic for this combination
                    best_mechanic = None
                    best_repair_time = float('inf')
                    
                    for mechanic in mechanic_stats:
                        repair_time = mechanic.get('avgRepairTime_min', 0)
                        if repair_time < best_repair_time and repair_time > 0:
                            best_repair_time = repair_time
                            best_mechanic = mechanic.get('mechanicName')
                    
                    # Create records for each mechanic
                    for mechanic in mechanic_stats:
                        mechanic_name = mechanic.get('mechanicName')
                        
                        # Calculate percentage worse than best
                        pct_worse = None
                        if best_repair_time and best_repair_time > 0:
                            repair_time = mechanic.get('avgRepairTime_min', 0)
                            if repair_time > 0:
                                pct_worse = ((repair_time - best_repair_time) / best_repair_time) * 100
                        
                        record = {
                            'context': 'byMachineAndReason',
                            'dimension_1': machine_type,
                            'dimension_2': reason,
                            'mechanic_name': mechanic_name,
                            'avg_repair_time_min': mechanic.get('avgRepairTime_min'),
                            'avg_response_time_min': mechanic.get('avgResponseTime_min'),
                            'pct_worse_than_best': pct_worse,
                            'is_best': mechanic_name == best_mechanic,
                            'period_start_date': period_start_date,
                            'period_end_date': period_end_date
                        }
                        records.append(record)
            
            # Upsert all records into the database
            print(f"WRITER: Upserting {len(records)} records into mechanic_performance")
            
            successful_records = []
            for record in records:
                result = self.supabase.table('mechanic_performance')\
                    .upsert(
                        record,
                        on_conflict='context,dimension_1,dimension_2,mechanic_name,period_start_date,period_end_date'
                    )\
                    .execute()
                if result.data:
                    successful_records.append(result.data[0])
            
            print(f"WRITER: Successfully wrote {len(successful_records)} records to the database")
            return successful_records
            
        except Exception as e:
            print(f"WRITER: Error writing analysis results: {e}")
            return []

# Test function for direct execution
if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Write analysis results to the database')
    parser.add_argument('--file', help='Path to JSON file containing analysis results')
    args = parser.parse_args()
    
    if args.file and os.path.exists(args.file):
        # Load analysis results from file
        with open(args.file, 'r') as f:
            analysis_results = json.load(f)
        
        # Write results to database
        writer = AnalysisWriter()
        records = writer.write_analysis_results(analysis_results)
        
        print(f"Wrote {len(records)} records to the mechanic_performance table")
    else:
        print("Please provide a valid JSON file with analysis results using --file")
