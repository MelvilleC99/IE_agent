#!/usr/bin/env python3
import sys
import os
import logging
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

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class FindingsWriter:
    """
    Handles saving of findings to the database.
    """
    
    def __init__(self):
        """Initialize the findings writer"""
        try:
            self.supabase = get_connection()
            logger.info("FINDINGS_WRITER: Connected to database successfully")
        except Exception as e:
            logger.error(f"FINDINGS_WRITER: Error connecting to database: {e}")
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
            logger.error("FINDINGS_WRITER: No database connection available")
            return []
            
        if not findings:
            logger.info("FINDINGS_WRITER: No findings to save")
            return []
            
        logger.info(f"FINDINGS_WRITER: Saving {len(findings)} findings to database")
        
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
            
            logger.info(f"FINDINGS_WRITER: Found {len(existing_findings)} existing findings")
        except Exception as e:
            logger.warning(f"FINDINGS_WRITER: Warning - could not check for existing findings: {e}")
        
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
                        logger.info(f"FINDINGS_WRITER: Updated finding ID {finding_id}")
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
                        logger.info(f"FINDINGS_WRITER: Saved new finding ID {saved_id}")
                        finding['finding_id'] = saved_id
                        saved_findings.append(finding)
            except Exception as e:
                logger.error(f"FINDINGS_WRITER: Error saving finding: {e}")
        
        logger.info(f"FINDINGS_WRITER: Successfully saved {len(saved_findings)} findings")
        return saved_findings

    def get_recent_findings(self, limit=100, status="New"):
        """
        Get recent findings from the database
        
        Args:
            limit: Maximum number of findings to retrieve
            status: Filter findings by status
            
        Returns:
            list: Recent findings
        """
        if not self.supabase:
            logger.error("FINDINGS_WRITER: No database connection available")
            return []
            
        try:
            findings_result = self.supabase.table('findings_log')\
                .select('*')\
                .eq('status', status)\
                .order('created_at', desc=True)\
                .limit(limit)\
                .execute()
                
            if findings_result.data:
                logger.info(f"FINDINGS_WRITER: Retrieved {len(findings_result.data)} {status} findings")
                return findings_result.data
            else:
                logger.info(f"FINDINGS_WRITER: No {status} findings found")
                return []
        except Exception as e:
            logger.error(f"FINDINGS_WRITER: Error retrieving findings: {e}")
            return []

# Test function for direct execution
if __name__ == '__main__':
    from agents.maintenance.analytics.Mechanic_performance_tool.mechanic_repair_analyzer import run_mechanic_analysis
    from agents.maintenance.analytics.Mechanic_performance_tool.mechanic_repair_interpreter import interpret_analysis_results
    
    # Create findings writer
    writer = FindingsWriter()
    
    # Run analysis
    analysis_results = run_mechanic_analysis()
    
    if analysis_results:
        # Interpret analysis results
        findings = interpret_analysis_results(analysis_results)
        
        # Save findings to database
        saved_findings = writer.save_findings(findings)
        logger.info(f"Saved {len(saved_findings)} findings to database")
    else:
        logger.error("No analysis results to interpret")