# /src/cost_tracking/usage_tracker.py
"""
Database usage tracking for agent queries and sessions.

Handles writing to the agent_usage_logs table with comprehensive
query tracking including costs, performance, and outcomes.
"""

import logging
import json
import time
import os
from typing import Dict, Any, Optional, List
from datetime import datetime
import sys

# Optional psutil import for memory tracking
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

# Add project root to path for database imports
current_file = os.path.abspath(__file__)
project_root = os.path.abspath(os.path.join(current_file, "../../.."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from .cost_calculator import CostCalculator

logger = logging.getLogger("usage_tracker")

class UsageTracker:
    """
    Tracks and logs detailed usage information to the database.
    
    Handles writing to agent_usage_logs table with comprehensive
    tracking of costs, performance, and query outcomes.
    """
    
    def __init__(self, db_connection=None):
        """
        Initialize the usage tracker.
        
        Args:
            db_connection: Database connection (if None, will try to get from Supabase)
        """
        self.db_connection = db_connection
        self.cost_calculator = CostCalculator()
        self.active_queries = {}  # Track ongoing queries
        
        # Try to get database connection from existing Supabase setup
        if not self.db_connection:
            self._init_database_connection()
        
        logger.info("Usage tracker initialized")
    
    def _init_database_connection(self):
        """Initialize database connection using existing Supabase setup."""
        try:
            # Try to import and use existing Supabase connection
            from src.shared_services.supabase_client import SupabaseClient
            self.supabase_client = SupabaseClient()
            logger.info("Connected to Supabase for usage tracking")
        except ImportError:
            logger.warning("Could not connect to Supabase - usage tracking will be limited")
            self.supabase_client = None
    
    def start_query_tracking(self, 
                           session_id: str,
                           conversation_id: str,
                           query_text: str,
                           user_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Start tracking a new query.
        
        Args:
            session_id: Session identifier
            conversation_id: Unique conversation identifier
            query_text: The user's query text
            user_id: Optional user identifier
            
        Returns:
            Dictionary with tracking info
        """
        start_time = time.time()
        start_memory = self._get_current_memory_usage()
        
        tracking_data = {
            "session_id": session_id,
            "conversation_id": conversation_id,
            "query_text": query_text,
            "user_id": user_id,
            "start_time": start_time,
            "start_memory_mb": start_memory,
            "api_calls": [],
            "tools_used": [],
            "peak_memory_mb": start_memory,
            "started_at": datetime.now().isoformat()
        }
        
        self.active_queries[conversation_id] = tracking_data
        
        logger.debug(f"Started tracking query {conversation_id}")
        return tracking_data
    
    def track_api_call(self,
                      conversation_id: str,
                      model: str,
                      input_tokens: int,
                      output_tokens: int,
                      agent_type: str = "chatgpt") -> None:
        """
        Track an LLM API call within a query.
        
        Args:
            conversation_id: Conversation identifier
            model: LLM model name
            input_tokens: Number of input tokens
            output_tokens: Number of output tokens
            agent_type: Type of agent making the call
        """
        if conversation_id not in self.active_queries:
            logger.warning(f"No active query found for {conversation_id}")
            return
        
        # Calculate cost for this API call
        cost_data = self.cost_calculator.calculate_llm_cost(
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens
        )
        
        api_call_data = {
            "model": model,
            "agent_type": agent_type,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cost": cost_data["total_llm_cost"],
            "timestamp": time.time()
        }
        
        self.active_queries[conversation_id]["api_calls"].append(api_call_data)
        
        # Update peak memory
        current_memory = self._get_current_memory_usage()
        if current_memory > self.active_queries[conversation_id]["peak_memory_mb"]:
            self.active_queries[conversation_id]["peak_memory_mb"] = current_memory
        
        logger.debug(f"Tracked API call for {conversation_id}: {model}, ${cost_data['total_llm_cost']:.6f}")
    
    def track_tool_usage(self, conversation_id: str, tool_name: str) -> None:
        """
        Track tool usage within a query.
        
        Args:
            conversation_id: Conversation identifier
            tool_name: Name of the tool used
        """
        if conversation_id not in self.active_queries:
            logger.warning(f"No active query found for {conversation_id}")
            return
        
        if tool_name not in self.active_queries[conversation_id]["tools_used"]:
            self.active_queries[conversation_id]["tools_used"].append(tool_name)
        
        logger.debug(f"Tracked tool usage for {conversation_id}: {tool_name}")
    
    def complete_query_tracking(self,
                              conversation_id: str,
                              success: bool = True,
                              error_message: Optional[str] = None,
                              response_type: str = "text",
                              response_size_kb: int = 0,
                              requires_deepseek: bool = False,
                              handed_to_deepseek: bool = False) -> bool:
        """
        Complete query tracking and write to database.
        
        Args:
            conversation_id: Conversation identifier
            success: Whether the query was successful
            error_message: Error message if query failed
            response_type: Type of response ('table', 'list', 'text', 'error')
            response_size_kb: Size of response in KB
            requires_deepseek: Whether query requires DeepSeek
            handed_to_deepseek: Whether query was handed to DeepSeek
            
        Returns:
            True if successfully logged to database
        """
        if conversation_id not in self.active_queries:
            logger.warning(f"No active query found for {conversation_id}")
            return False
        
        tracking_data = self.active_queries[conversation_id]
        end_time = time.time()
        
        # Calculate totals
        processing_time_ms = int((end_time - tracking_data["start_time"]) * 1000)
        total_api_calls = len(tracking_data["api_calls"])
        total_input_tokens = sum(call["input_tokens"] for call in tracking_data["api_calls"])
        total_output_tokens = sum(call["output_tokens"] for call in tracking_data["api_calls"])
        total_llm_cost = sum(call["cost"] for call in tracking_data["api_calls"])
        
        # Calculate compute cost estimate
        compute_cost_data = self.cost_calculator.calculate_cloud_run_cost(
            processing_time_ms=processing_time_ms,
            memory_usage_mb=tracking_data["peak_memory_mb"]
        )
        
        # Determine query type based on tools used
        query_type = self._classify_query_type(tracking_data["tools_used"])
        
        # Determine agent used
        agents_used = set(call["agent_type"] for call in tracking_data["api_calls"])
        if len(agents_used) > 1:
            agent_used = "both"
        elif "deepseek" in agents_used:
            agent_used = "deepseek"
        else:
            agent_used = "chatgpt"
        
        # Prepare database record
        usage_record = {
            "session_id": tracking_data["session_id"],
            "conversation_id": conversation_id,
            "user_id": tracking_data["user_id"],
            "query_text": tracking_data["query_text"][:1000],  # Truncate long queries
            "query_type": query_type,
            "tools_used": json.dumps(tracking_data["tools_used"]),
            "processing_time_ms": processing_time_ms,
            "memory_usage_mb": tracking_data["peak_memory_mb"],
            "total_api_calls": total_api_calls,
            "total_input_tokens": total_input_tokens,
            "total_output_tokens": total_output_tokens,
            "llm_cost": total_llm_cost,
            "estimated_compute_cost": compute_cost_data["total_compute_cost"],
            "total_cost": total_llm_cost + compute_cost_data["total_compute_cost"],
            "success": success,
            "error_message": error_message,
            "response_type": response_type,
            "response_size_kb": response_size_kb,
            "agent_used": agent_used,
            "requires_deepseek": requires_deepseek,
            "handed_to_deepseek": handed_to_deepseek,
            "started_at": tracking_data["started_at"],
            "completed_at": datetime.now().isoformat()
        }
        
        # Write to database
        success_logged = self._write_to_database(usage_record)
        
        # Cleanup
        del self.active_queries[conversation_id]
        
        if success_logged:
            logger.info(f"Query {conversation_id} logged: {processing_time_ms}ms, ${usage_record['total_cost']:.6f}")
        
        return success_logged
    
    def _get_current_memory_usage(self) -> int:
        """Get current memory usage in MB."""
        if not PSUTIL_AVAILABLE:
            logger.debug("psutil not available, using fallback memory value")
            return 512  # Default fallback value in MB
            
        try:
            process = psutil.Process(os.getpid())
            memory_mb = process.memory_info().rss // (1024 * 1024)
            return memory_mb
        except Exception as e:
            logger.warning(f"Could not get memory usage: {e}")
            return 512  # Fallback value
    
    def _classify_query_type(self, tools_used: List[str]) -> str:
        """Classify query type based on tools used."""
        if not tools_used:
            return "simple_query"
        
        if "analyze_mechanic_performance" in tools_used:
            return "performance_analysis"
        elif "run_scheduled_maintenance" in tools_used:
            return "maintenance_workflow"
        elif "quick_query" in tools_used:
            return "data_query"
        elif any("watchlist" in tool for tool in tools_used):
            return "watchlist_query"
        else:
            return "complex_query"
    
    def _write_to_database(self, usage_record: Dict[str, Any]) -> bool:
        """
        Write usage record to the database.
        
        Args:
            usage_record: Complete usage record to write
            
        Returns:
            True if successfully written
        """
        if not self.supabase_client:
            logger.warning("No database connection available")
            return False
        
        try:
            # Write to agent_usage_logs table
            result = self.supabase_client.table("agent_usage_logs").insert(usage_record).execute()
            
            if result.data:
                logger.debug(f"Successfully wrote usage record for {usage_record['conversation_id']}")
                return True
            else:
                logger.error(f"Failed to write usage record: {result}")
                return False
                
        except Exception as e:
            logger.error(f"Error writing to database: {e}")
            return False
    
    def get_active_queries(self) -> Dict[str, Any]:
        """Get information about currently active queries."""
        return {
            "active_count": len(self.active_queries),
            "queries": list(self.active_queries.keys())
        }
    
    def cleanup_stale_queries(self, max_age_minutes: int = 30) -> int:
        """
        Clean up queries that have been active too long.
        
        Args:
            max_age_minutes: Maximum age before considering a query stale
            
        Returns:
            Number of stale queries cleaned up
        """
        current_time = time.time()
        stale_queries = []
        
        for conversation_id, tracking_data in self.active_queries.items():
            age_minutes = (current_time - tracking_data["start_time"]) / 60
            if age_minutes > max_age_minutes:
                stale_queries.append(conversation_id)
        
        # Complete tracking for stale queries with error
        for conversation_id in stale_queries:
            self.complete_query_tracking(
                conversation_id=conversation_id,
                success=False,
                error_message=f"Query timed out after {max_age_minutes} minutes"
            )
        
        if stale_queries:
            logger.info(f"Cleaned up {len(stale_queries)} stale queries")
        
        return len(stale_queries)
