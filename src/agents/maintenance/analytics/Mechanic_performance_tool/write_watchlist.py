#!/usr/bin/env python3
import sys
import os
import logging
import re
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(current_dir, "../../../"))
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

# Load environment - fix the path
project_root = os.path.abspath(os.path.join(current_dir, "../../../../../"))
env_path = os.path.join(project_root, ".env.local")
load_dotenv(env_path)

from shared_services.supabase_client import get_shared_supabase_client

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class WatchlistWriter:
    """
    Handles creating watchlist items from findings and writing them to the database.
    """
    
    def __init__(self):
        """Initialize the watchlist writer"""
        try:
            self.supabase = get_shared_supabase_client()
            logger.info("WATCHLIST_WRITER: Connected to shared database connection successfully")
            self.today = datetime.now().date()
        except Exception as e:
            logger.error(f"WATCHLIST_WRITER: Error connecting to database: {e}")
            self.supabase = None
            self.today = datetime.now().date()
    
    def clean_text(self, text):
        """Clean and normalize text"""
        if not text:
            return ""
        cleaned = re.sub(r'\s+', ' ', text.strip())
        cleaned = re.sub(r'([.,!?;:])\s*', r'\1 ', cleaned)
        return cleaned.strip()
    
    def format_title(self, issue_type, display_name, mech_id, context_info=""):
        """Format the watchlist title with consistent structure"""
        formatted_issue = issue_type.replace('_', ' ').title()
        context = self.clean_text(context_info)
        if context:
            context = f" - {context}"
        emp_num = f" (#{mech_id})" if mech_id and mech_id != 'Unknown' else ''
        return f"{formatted_issue}: {display_name}{emp_num}{context}"
    
    def extract_mechanic_from_summary(self, summary):
        """
        Extract mechanic display name and numeric ID from summary:
        '...: Duncan J (#003) ...'
        Returns (name, id)
        """
        match = re.search(r":\s*(.*?)\s*\(#(\d+)\)", summary)
        if match:
            name = match.group(1).strip()
            mech_id = match.group(2)
            return name, mech_id
        return None, None
    
    def extract_machine_reason_from_summary(self, summary):
        """Extract machine type and reason from finding summary"""
        machine_match = re.search(r'on\s+([A-Za-z\s]+?)\s+(?:machines|with)', summary)
        reason_match = re.search(r"with\s+'([^']+)'", summary)
        machine = machine_match.group(1).strip() if machine_match else None
        reason = reason_match.group(1).strip() if reason_match else None
        return machine, reason
    
    def create_watchlist_item_from_finding(self, finding):
        """
        Create a watchlist item record from a finding
        
        Args:
            finding: Dictionary with finding information
            
        Returns:
            dict: The created watchlist item record or None if creation failed
        """
        if not self.supabase:
            logger.error("WATCHLIST_WRITER: No database connection")
            return None
            
        # Extract finding details
        summary = finding.get('finding_summary', '')
        details = finding.get('finding_details', {})
        mechanic_performance_id = finding.get('mechanic_performance_id')
        
        # Skip if no mechanic_performance_id (can't link to source data)
        if not mechanic_performance_id:
            logger.warning("WATCHLIST_WRITER: Skipping finding - no mechanic_performance_id provided")
            return None
        
        # Check if watchlist item already exists for this mechanic performance record
        existing_check = self.supabase.table('watch_list').select('id').eq('mechanic_performance_id', mechanic_performance_id).execute()
        if existing_check.data:
            logger.info(f"WATCHLIST_WRITER: Watchlist item already exists for mechanic_performance_id {mechanic_performance_id}")
            return None
        
        # Extract mechanic info from summary
        display_name, mech_id = self.extract_mechanic_from_summary(summary)
        display_name = display_name or details.get('mechanic_id') or 'Unknown'
        mech_id = mech_id or details.get('employee_number') or 'Unknown'
        
        # Determine issue type
        analysis_type = finding.get('analysis_type', '')
        if 'response_time' in analysis_type:
            issue_type = 'response_time'
        elif 'repair_time' in analysis_type or 'machine_repair' in analysis_type:
            issue_type = 'repair_time'
        else:
            issue_type = analysis_type.split('_')[-1] if '_' in analysis_type else 'other'
        
        # Determine monitoring schedule
        if issue_type == 'response_time':
            freq, end = 'daily', self.today + timedelta(days=14)
        elif issue_type == 'repair_time':
            freq, end = 'weekly', self.today + timedelta(days=28)
        else:
            freq, end = 'weekly', self.today + timedelta(days=21)
        
        # Extract machine type and reason
        machine_type = details.get('machine_type')
        reason = details.get('reason')
        
        if not machine_type or not reason:
            m, r = self.extract_machine_reason_from_summary(summary)
            machine_type = machine_type or m
            reason = reason or r
        
        # Format title and notes
        context = ''
        if machine_type:
            context += machine_type
        if reason:
            context += (' - ' if context else '') + reason
            
        title = self.format_title(issue_type, display_name, mech_id, context)
        notes = f"Auto-created from finding. Original issue: {self.clean_text(summary)}"
        
        # Create watchlist item record
        watchlist_data = {
            'mechanic_performance_id': mechanic_performance_id,
            'title': title,
            'issue_type': issue_type,
            'entity_type': 'mechanic',
            'mechanic_name': display_name,
            'mechanic_id': mech_id,
            'assigned_to': None,
            'status': 'open',
            'monitor_frequency': freq,
            'monitor_start_date': self.today.isoformat(),
            'monitor_end_date': end.isoformat(),
            'monitor_status': 'active',
            'notes': notes,
            'extension_count': 0
        }
        
        try:
            # Insert watchlist item
            watchlist_res = self.supabase.table('watch_list').insert(watchlist_data).execute()
            
            if not watchlist_res.data:
                logger.error(f"WATCHLIST_WRITER: Error creating watchlist item for mechanic_performance_id {mechanic_performance_id}")
                return None
                
            watchlist_id = watchlist_res.data[0]['id']
            logger.info(f"WATCHLIST_WRITER: Created watchlist item ID {watchlist_id} for mechanic_performance_id {mechanic_performance_id}")
            
            # Create baseline measurement
            try:
                value = float(details.get('value', 0))
            except (ValueError, TypeError):
                value = 0
                
            mdata = {
                'watchlist_id': watchlist_id,
                'measurement_date': self.today.isoformat(),
                'value': round(value, 2),
                'change_pct': 0,
                'is_improved': False,
                'notes': 'Baseline measurement from finding'
            }
            
            m_res = self.supabase.table('measurements').insert(mdata).execute()
            
            if m_res.data:
                logger.info(f"WATCHLIST_WRITER: Created baseline measurement for watchlist item {watchlist_id}")
            else:
                logger.warning(f"WATCHLIST_WRITER: Failed to create baseline measurement for watchlist item {watchlist_id}")
            
            # Return the created watchlist item with additional info
            created_item = watchlist_res.data[0]
            created_item.update({
                'machine_type': machine_type,
                'reason': reason,
                'baseline_value': round(value, 2)
            })
            
            return created_item
            
        except Exception as e:
            logger.error(f"WATCHLIST_WRITER: Error creating watchlist item: {e}")
            return None
    
    def create_watchlist_items_from_findings(self, findings=None):
        """
        Create watchlist items from findings - either passed directly or from database
        
        Args:
            findings: List of findings to process (optional - if None, reads from database)
            
        Returns:
            list: Created watchlist item records
        """
        if not self.supabase:
            logger.error("WATCHLIST_WRITER: No database connection")
            return []
            
        try:
            # Use provided findings or read from database (legacy support)
            if findings is not None:
                logger.info(f"WATCHLIST_WRITER: Processing {len(findings)} findings passed directly")
                logger.info(f"WATCHLIST_WRITER: Sample finding keys: {list(findings[0].keys()) if findings else 'No findings'}")
                findings_data = findings
            else:
                # Legacy mode - read from findings_log table
                findings_result = self.supabase.table('findings_log').select('*').eq('status', 'New').execute()
                if not findings_result.data:
                    logger.info("WATCHLIST_WRITER: No new findings to process")
                    return []
                findings_data = findings_result.data
                logger.info(f"WATCHLIST_WRITER: Found {len(findings_data)} new findings from database")
            
            # Create watchlist items
            created_items = []
            for i, finding in enumerate(findings_data):
                logger.info(f"WATCHLIST_WRITER: Processing finding {i+1}/{len(findings_data)}")
                logger.info(f"WATCHLIST_WRITER: Finding keys: {list(finding.keys())}")
                logger.info(f"WATCHLIST_WRITER: mechanic_performance_id = {finding.get('mechanic_performance_id')}")
                
                item = self.create_watchlist_item_from_finding(finding)
                if item:
                    created_items.append(item)
                else:
                    logger.warning(f"WATCHLIST_WRITER: Failed to create watchlist item for finding {i+1}")
            
            logger.info(f"WATCHLIST_WRITER: Created {len(created_items)} watchlist items")
            return created_items
            
        except Exception as e:
            logger.error(f"WATCHLIST_WRITER: Error processing findings: {e}")
            return []
            
    def get_active_watchlist_items(self, status='open'):
        """
        Get active watchlist items from the database
        
        Args:
            status: Filter watchlist items by status
            
        Returns:
            list: Active watchlist items
        """
        if not self.supabase:
            logger.error("WATCHLIST_WRITER: No database connection")
            return []
            
        try:
            watchlist_result = self.supabase.table('watch_list')\
                .select('*')\
                .eq('status', status)\
                .eq('monitor_status', 'active')\
                .execute()
                
            if watchlist_result.data:
                logger.info(f"WATCHLIST_WRITER: Retrieved {len(watchlist_result.data)} active watchlist items")
                return watchlist_result.data
            else:
                logger.info(f"WATCHLIST_WRITER: No active watchlist items found")
                return []
        except Exception as e:
            logger.error(f"WATCHLIST_WRITER: Error retrieving watchlist items: {e}")
            return []

# For direct execution
if __name__ == '__main__':
    writer = WatchlistWriter()
    items = writer.create_watchlist_items_from_findings()
    
    logger.info(f"\nWatchlist Writer Summary:")
    logger.info(f"- Created {len(items)} watchlist items from findings")
    
    if items:
        logger.info("\nSample Watchlist Items:")
        for i, item in enumerate(items[:3]):  # Show first 3 items
            logger.info(f"{i+1}. ID {item['id']}: {item['title']}")
            logger.info(f"   Monitoring: {item['monitor_frequency']} until {item['monitor_end_date']}")