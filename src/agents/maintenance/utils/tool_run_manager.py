# src/agents/maintenance/utils/tool_run_manager.py

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import uuid

logger = logging.getLogger("tool_run_manager")

class ToolRunManager:
    """
    Standard utility for managing tool execution logging and frequency control.
    
    Used by all maintenance tools for consistent logging and frequency management.
    """
    
    def __init__(self, db_client):
        """Initialize with database client."""
        self.db = db_client
    
    def can_run_tool(self, tool_name: str, min_days: int = 30) -> Tuple[bool, Optional[datetime]]:
        """
        Check if enough time has passed since last tool run.
        
        Args:
            tool_name: Name of the tool (e.g., 'scheduled_maintenance', 'mechanic_performance')
            min_days: Minimum days between runs
            
        Returns:
            Tuple of (can_run, last_run_date)
        """
        try:
            # Query for most recent successful run
            results = self.db.query_table(
                table_name='tool_run_logs',
                columns='run_date',
                filters={
                    'tool_name': tool_name,
                    'status': 'completed'
                },
                limit=100  # Get all runs to sort properly
            )
            
            if not results:
                logger.info(f"No previous runs found for {tool_name}")
                return True, None
            
            # Sort by run_date to get most recent
            sorted_results = sorted(results, key=lambda x: x['run_date'], reverse=True)
            last_run_str = sorted_results[0]['run_date']
            last_run_date = datetime.fromisoformat(last_run_str.replace('Z', '+00:00'))
            
            days_since = (datetime.now() - last_run_date).days
            can_run = days_since >= min_days
            
            logger.info(f"{tool_name}: Last run {last_run_date.date()}, {days_since} days ago, min {min_days} days")
            return can_run, last_run_date
            
        except Exception as e:
            logger.error(f"Error checking tool frequency for {tool_name}: {e}")
            return True, None  # Allow run if check fails
    
    def log_tool_start(
        self, 
        tool_name: str, 
        period_start: datetime, 
        period_end: datetime,
        summary: str = ""
    ) -> str:
        """
        Log the start of a tool run.
        
        Args:
            tool_name: Name of the tool
            period_start: Analysis period start
            period_end: Analysis period end
            summary: Brief description
            
        Returns:
            run_id: UUID of the logged run
        """
        try:
            run_id = str(uuid.uuid4())
            
            log_entry = {
                'id': run_id,
                'tool_name': tool_name,
                'run_date': datetime.now().isoformat(),
                'period_start': period_start.isoformat(),
                'period_end': period_end.isoformat(),
                'status': 'in_progress',
                'items_processed': 0,
                'items_created': 0,
                'summary': summary,
                'metadata': {},
                'created_at': datetime.now().isoformat()
            }
            
            result = self.db.insert_data('tool_run_logs', log_entry)
            logger.info(f"Started tool run log: {tool_name} ({run_id})")
            return run_id
            
        except Exception as e:
            logger.error(f"Error logging tool start for {tool_name}: {e}")
            return str(uuid.uuid4())  # Return dummy ID so workflow continues
    
    def log_tool_complete(
        self,
        run_id: str,
        items_processed: int,
        items_created: int,
        summary: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Log the completion of a tool run.
        
        Args:
            run_id: The run ID from log_tool_start
            items_processed: Number of items analyzed
            items_created: Number of items created (tasks, watchlist items, etc.)
            summary: Brief summary of results
            metadata: Additional tool-specific data
        """
        try:
            update_data = {
                'id': run_id,  # Include the ID for matching
                'status': 'completed',
                'items_processed': items_processed,
                'items_created': items_created,
                'summary': summary,
                'metadata': metadata or {},
                'updated_at': datetime.now().isoformat()
            }
            
            # Use the SupabaseClient update_data method with 'id' as match column
            result = self.db.update_data('tool_run_logs', update_data, 'id')
            logger.info(f"Completed tool run log: {run_id}")
            
        except Exception as e:
            logger.error(f"Error logging tool completion for {run_id}: {e}")
    
    def log_tool_error(self, run_id: str, error_message: str) -> None:
        """
        Log a tool run error.
        
        Args:
            run_id: The run ID from log_tool_start
            error_message: Error description
        """
        try:
            update_data = {
                'id': run_id,  # Include the ID for matching
                'status': 'failed',
                'summary': f"Error: {error_message}",
                'updated_at': datetime.now().isoformat()
            }
            
            # Use the SupabaseClient update_data method with 'id' as match column
            result = self.db.update_data('tool_run_logs', update_data, 'id')
            logger.error(f"Tool run failed: {run_id} - {error_message}")
            
        except Exception as e:
            logger.error(f"Error logging tool failure for {run_id}: {e}")


# Convenience functions for easy integration
def check_tool_frequency(db_client, tool_name: str, min_days: int = 30) -> Tuple[bool, Optional[datetime]]:
    """Convenience function to check if tool can run."""
    manager = ToolRunManager(db_client)
    return manager.can_run_tool(tool_name, min_days)

def log_tool_run(
    db_client, 
    tool_name: str, 
    period_start: datetime, 
    period_end: datetime,
    items_processed: int,
    items_created: int,
    summary: str,
    metadata: Optional[Dict[str, Any]] = None
) -> str:
    """Convenience function for simple tool run logging."""
    manager = ToolRunManager(db_client)
    run_id = manager.log_tool_start(tool_name, period_start, period_end, summary)
    manager.log_tool_complete(run_id, items_processed, items_created, summary, metadata)
    return run_id
