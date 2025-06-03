# /src/cost_tracking/session_summarizer.py
"""
Session summarization and analytics for conversation tracking.

Handles aggregating query data into session summaries and writing
to the session_summaries table for business intelligence.
"""

import logging
import json
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import sys
import os

# Add project root to path
current_file = os.path.abspath(__file__)
project_root = os.path.abspath(os.path.join(current_file, "../../.."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from .cost_calculator import CostCalculator

logger = logging.getLogger("session_summarizer")

class SessionSummarizer:
    """
    Summarizes sessions and writes to session_summaries table.
    
    Aggregates data from multiple queries within a session to provide
    session-level analytics and conversation summaries.
    """
    
    def __init__(self, db_connection=None):
        """
        Initialize the session summarizer.
        
        Args:
            db_connection: Database connection (if None, will try to get from Supabase)
        """
        self.db_connection = db_connection
        self.cost_calculator = CostCalculator()
        
        # Try to get database connection from existing Supabase setup
        if not self.db_connection:
            self._init_database_connection()
        
        logger.info("Session summarizer initialized")
    
    def _init_database_connection(self):
        """Initialize database connection using existing Supabase setup."""
        try:
            from src.shared_services.supabase_client import SupabaseClient
            self.supabase_client = SupabaseClient()
            logger.info("Connected to Supabase for session summarization")
        except ImportError:
            logger.warning("Could not connect to Supabase - session summarization will be limited")
            self.supabase_client = None
    
    def create_session_summary(self,
                              session_id: str,
                              user_id: Optional[str] = None,
                              session_ended_reason: str = "timeout") -> bool:
        """
        Create a session summary by aggregating usage logs.
        
        Args:
            session_id: Session identifier to summarize
            user_id: Optional user identifier
            session_ended_reason: How the session ended
            
        Returns:
            True if summary was successfully created
        """
        if not self.supabase_client:
            logger.warning("No database connection available")
            return False
        
        try:
            # Get all usage logs for this session
            usage_logs = self._get_session_usage_logs(session_id)
            
            logger.info(f"Found {len(usage_logs) if usage_logs else 0} usage logs for session {session_id}")
            
            if not usage_logs:
                logger.warning(f"No usage logs found for session {session_id} - cannot create summary")
                return False
            
            # Calculate session statistics
            session_stats = self._calculate_session_statistics(usage_logs)
            
            # Generate conversation summary
            conversation_summary = self._generate_conversation_summary(usage_logs)
            
            # Classify session
            session_classification = self._classify_session(usage_logs)
            
            # Create session summary record
            session_summary = {
                "session_id": session_id,
                "user_id": user_id,
                "total_queries": session_stats["total_queries"],
                "successful_queries": session_stats["successful_queries"],
                "failed_queries": session_stats["failed_queries"],
                "session_duration_ms": session_stats["session_duration_ms"],
                "avg_query_time_ms": session_stats["avg_query_time_ms"],
                "total_processing_time_ms": session_stats["total_processing_time_ms"],
                "total_cost": session_stats["total_cost"],
                "avg_cost_per_query": session_stats["avg_cost_per_query"],
                "total_llm_cost": session_stats["total_llm_cost"],
                "total_compute_cost": session_stats["total_compute_cost"],
                "total_input_tokens": session_stats["total_input_tokens"],
                "total_output_tokens": session_stats["total_output_tokens"],
                "total_api_calls": session_stats["total_api_calls"],
                "chatgpt_queries": session_stats["chatgpt_queries"],
                "deepseek_queries": session_stats["deepseek_queries"],
                "tools_used": json.dumps(session_stats["unique_tools"]),
                "main_topics": session_classification["main_topics"],
                "session_type": session_classification["session_type"],
                "conversation_summary": conversation_summary,
                "key_outcomes": session_classification["key_outcomes"],
                "complexity_level": session_classification["complexity_level"],
                "requires_followup": session_classification["requires_followup"],
                "session_ended_reason": session_ended_reason,
                "session_started_at": session_stats["session_started_at"],
                "session_ended_at": datetime.now().isoformat()
            }
            
            # Write to database
            result = self.supabase_client.table("session_summaries").insert(session_summary).execute()
            
            if result.data:
                logger.info(f"Created session summary for {session_id}: {session_stats['total_queries']} queries, ${session_stats['total_cost']:.4f}")
                return True
            else:
                logger.error(f"Failed to create session summary: {result}")
                return False
                
        except Exception as e:
            logger.error(f"Error creating session summary: {e}")
            return False
    
    def _get_session_usage_logs(self, session_id: str) -> List[Dict[str, Any]]:
        """Get all usage logs for a specific session."""
        if not self.supabase_client:
            logger.error("No Supabase client available for getting session usage logs.")
            return []
        try:
            result = self.supabase_client.table("agent_usage_logs").select("*").eq("session_id", session_id).execute()
            return result.data if result.data else []
        except Exception as e:
            logger.error(f"Error getting session usage logs: {e}")
            return []
    
    def _calculate_session_statistics(self, usage_logs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate statistical summary of session usage."""
        if not usage_logs:
            return {}
        
        total_queries = len(usage_logs)
        successful_queries = sum(1 for log in usage_logs if log.get("success", True))
        failed_queries = total_queries - successful_queries
        
        # Time calculations
        start_times = [log.get("started_at") for log in usage_logs if log.get("started_at")]
        end_times = [log.get("completed_at") for log in usage_logs if log.get("completed_at")]
        # Filter out None values
        start_times = [t for t in start_times if t is not None]
        end_times = [t for t in end_times if t is not None]
        session_started_at = min(start_times) if start_times else datetime.now().isoformat()
        session_ended_at = max(end_times) if end_times else datetime.now().isoformat()
        
        # Calculate session duration
        try:
            start_dt = datetime.fromisoformat(session_started_at.replace('Z', '+00:00'))
            end_dt = datetime.fromisoformat(session_ended_at.replace('Z', '+00:00'))
            session_duration_ms = int((end_dt - start_dt).total_seconds() * 1000)
        except:
            session_duration_ms = 0
        
        # Processing time calculations
        processing_times = [log.get("processing_time_ms", 0) for log in usage_logs]
        total_processing_time_ms = sum(processing_times)
        avg_query_time_ms = int(total_processing_time_ms / total_queries) if total_queries > 0 else 0
        
        # Cost calculations
        total_cost = sum(log.get("total_cost", 0) for log in usage_logs)
        total_llm_cost = sum(log.get("llm_cost", 0) for log in usage_logs)
        total_compute_cost = sum(log.get("estimated_compute_cost", 0) for log in usage_logs)
        avg_cost_per_query = total_cost / total_queries if total_queries > 0 else 0
        
        # Token calculations
        total_input_tokens = sum(log.get("total_input_tokens", 0) for log in usage_logs)
        total_output_tokens = sum(log.get("total_output_tokens", 0) for log in usage_logs)
        total_api_calls = sum(log.get("total_api_calls", 0) for log in usage_logs)
        
        # Agent usage
        chatgpt_queries = sum(1 for log in usage_logs if log.get("agent_used") in ["chatgpt", "both"])
        deepseek_queries = sum(1 for log in usage_logs if log.get("agent_used") in ["deepseek", "both"])
        
        # Tools used
        unique_tools = set()
        for log in usage_logs:
            tools_used = log.get("tools_used")
            if tools_used:
                if isinstance(tools_used, str):
                    try:
                        tools_list = json.loads(tools_used)
                        unique_tools.update(tools_list)
                    except:
                        pass
                elif isinstance(tools_used, list):
                    unique_tools.update(tools_used)
        
        return {
            "total_queries": total_queries,
            "successful_queries": successful_queries,
            "failed_queries": failed_queries,
            "session_duration_ms": session_duration_ms,
            "avg_query_time_ms": avg_query_time_ms,
            "total_processing_time_ms": total_processing_time_ms,
            "total_cost": round(total_cost, 6),
            "avg_cost_per_query": round(avg_cost_per_query, 6),
            "total_llm_cost": round(total_llm_cost, 6),
            "total_compute_cost": round(total_compute_cost, 6),
            "total_input_tokens": total_input_tokens,
            "total_output_tokens": total_output_tokens,
            "total_api_calls": total_api_calls,
            "chatgpt_queries": chatgpt_queries,
            "deepseek_queries": deepseek_queries,
            "unique_tools": list(unique_tools),
            "session_started_at": session_started_at
        }
    
    def _generate_conversation_summary(self, usage_logs: List[Dict[str, Any]]) -> str:
        """Generate a human-readable conversation summary."""
        if not usage_logs:
            return "No conversation data available"
        
        total_queries = len(usage_logs)
        successful_queries = sum(1 for log in usage_logs if log.get("success", True))
        
        # Generate basic summary
        summary_parts = [f"Session with {total_queries} queries ({successful_queries} successful)"]
        
        return ". ".join(summary_parts) + "."
    
    def _classify_session(self, usage_logs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Classify session characteristics and outcomes."""
        if not usage_logs:
            return {
                "main_topics": [],
                "session_type": "unknown",
                "key_outcomes": "No data available",
                "complexity_level": "low",
                "requires_followup": False
            }
        
        # Basic classification
        total_queries = len(usage_logs)
        successful_queries = sum(1 for log in usage_logs if log.get("success", True))
        
        return {
            "main_topics": ["maintenance"],
            "session_type": "data_inquiry",
            "key_outcomes": f"{successful_queries} of {total_queries} queries completed successfully",
            "complexity_level": "medium" if total_queries > 5 else "low",
            "requires_followup": successful_queries < total_queries
        }
