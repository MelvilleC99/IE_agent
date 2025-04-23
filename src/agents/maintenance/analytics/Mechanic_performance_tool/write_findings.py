#!/usr/bin/env python3
import sys
import os
import json
from datetime import datetime
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

class FindingsWriter:
    """
    Handles saving of findings to the database.
    """
    
    def __init__(self):
        """Initialize the findings writer"""
        try:
            self.supabase = get_connection()
            print("FINDINGS_WRITER: Connected to database successfully")
        except Exception as e:
            print(f"FINDINGS_WRITER: Error connecting to database: {e}")
            self.supabase = None
    
    def save_findings(self, findings):
        """
        Save findings to the database
        
        Args:
            findings: List of finding dictionaries to save
            
        Returns:
            list: The findings that were successfully saved
        """
        if not self.supabase:
            print("FINDINGS_WRITER: No database connection available")
            return []
            
        if not findings:
            print("FINDINGS_WRITER: No findings to save")
            return []
            
        print(f"FINDINGS_WRITER: Saving {len(findings)} findings to database")
        
        # Check for existing findings to prevent duplicates
        existing_findings = {}
        try:
            findings_result = self.supabase.table('findings_log').select('finding_id, analysis_type, finding_details->mechanic_id, finding_details->metric').execute()
            for finding in findings_result.data:
                # Create a key to identify unique findings
                mechanic_id = finding.get('mechanic_id', '')
                metric = finding.get('metric', '')
                existing_key = f"{finding['analysis_type']}_{mechanic_id}_{metric}"
                existing_findings[existing_key] = finding['finding_id']
            
            print(f"FINDINGS_WRITER: Found {len(existing_findings)} existing findings")
        except Exception as e:
            print(f"FINDINGS_WRITER: Warning - could not check for existing findings: {e}")
        
        # Save each finding
        saved_findings = []
        for finding in findings:
            try:
                # Create a unique key for this finding
                mechanic_id = finding['finding_details'].get('mechanic_id', '')
                metric = finding['finding_details'].get('metric', '')
                finding_key = f"{finding['analysis_type']}_{mechanic_id}_{metric}"
                
                if finding_key in existing_findings:
                    # Update existing finding
                    finding_id = existing_findings[finding_key]
                    update_result = self.supabase.table('findings_log').update({
                        'finding_summary': finding['finding_summary'],
                        'finding_details': finding['finding_details'],
                        'updated_at': 'NOW()'
                    }).eq('finding_id', finding_id).execute()
                    
                    if update_result.data:
                        print(f"FINDINGS_WRITER: Updated finding ID {finding_id}")
                        finding['finding_id'] = finding_id
                        saved_findings.append(finding)
                else:
                    # Insert new finding
                    result = self.supabase.table('findings_log').insert({
                        'analysis_type': finding['analysis_type'],
                        'finding_summary': finding['finding_summary'],
                        'finding_details': finding['finding_details'],
                        'status': 'New'
                    }).execute()
                    
                    if result.data:
                        saved_id = result.data[0]['finding_id']
                        print(f"FINDINGS_WRITER: Saved new finding ID {saved_id}")
                        finding['finding_id'] = saved_id
                        saved_findings.append(finding)
            except Exception as e:
                print(f"FINDINGS_WRITER: Error saving finding: {e}")
        
        print(f"FINDINGS_WRITER: Successfully saved {len(saved_findings)} findings")
        return saved_findings

# Test function for direct execution
if __name__ == '__main__':
    import argparse
    from mechanic_repair_interpreter import interpret_analysis_results
    
    parser = argparse.ArgumentParser(description='Save findings to the database')
    parser.add_argument('--analysis-file', help='Path to JSON file containing analysis results')
    parser.add_argument('--findings-file', help='Path to JSON file containing findings')
    args = parser.parse_args()
    
    # Create findings writer
    writer = FindingsWriter()
    
    if args.findings_file and os.path.exists(args.findings_file):
        # Load findings directly from file
        with open(args.findings_file, 'r') as f:
            findings = json.load(f)
        
        # Save findings to database
        saved_findings = writer.save_findings(findings)
        print(f"Saved {len(saved_findings)} findings to database")
        
    elif args.analysis_file and os.path.exists(args.analysis_file):
        # Load analysis results from file
        with open(args.analysis_file, 'r') as f:
            analysis_results = json.load(f)
        
        # Interpret analysis results
        findings = interpret_analysis_results(analysis_results)
        
        # Save findings to database
        saved_findings = writer.save_findings(findings)
        print(f"Saved {len(saved_findings)} findings to database")
        
    else:
        print("Please provide either a findings file with --findings-file or an analysis results file with --analysis-file")