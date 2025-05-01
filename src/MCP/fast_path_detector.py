# /Users/melville/Documents/Industrial_Engineering_Agent/src/MCP/fast_path_detector.py

import logging
import os
from typing import Optional, List, Tuple, Any, Dict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("fast_path_detector")

class FastPathDetector:
    """
    Detects queries that can be answered without invoking the full LLM process.
    
    This class handles:
    1. Pattern detection for simple queries (greetings, thanks, etc.)
    2. Response generation for common questions
    
    Database and tool decisions are delegated to the LLM.
    """
    
    def __init__(self):
        """Initialize the fast path detector with common patterns."""
        # Greeting patterns
        self.greetings = [
            "hi", "hello", "hey", "good morning", "good afternoon", 
            "good evening", "greetings", "what's up", "howdy"
        ]
        
        # Thank you patterns
        self.thanks_phrases = [
            "thank you", "thanks", "appreciate it", "thank", "got it"
        ]
        
        # Tool query patterns
        self.tool_queries = [
            "what tool", "which tool", "available tool", "what kind of tool", 
            "what type of tool", "tools do you have", "tools available"
        ]
        
        # Analysis query patterns
        self.analysis_queries = [
            "what analysis", "what analyses", "what type of analysis", 
            "what kind of analysis", "analyses can you do", "analytics"
        ]
        
        # Capability question patterns
        self.capability_questions = [
            "what can you do", "help me", "what are your capabilities", 
            "how can you help", "what do you do"
        ]
        
        logger.info("Fast path detector initialized with conversation patterns")
    
    def is_simple_query(self, query: str) -> bool:
        """
        Determine if a query can be handled via the fast path.
        Only handles simple conversational queries.
        
        Args:
            query: The user's query string
            
        Returns:
            Boolean indicating if this is a simple conversational query
        """
        query = query.lower().strip()
        
        # Check for greetings
        if any(query == greeting for greeting in self.greetings):
            return True
        
        # Check for thank you messages
        if any(phrase in query for phrase in self.thanks_phrases) and len(query.split()) < 8:
            return True
        
        # Check for tool queries
        if any(phrase in query for phrase in self.tool_queries):
            return True
        
        # Check for analysis queries
        if any(phrase in query for phrase in self.analysis_queries):
            return True
        
        # Check for capability questions
        if any(phrase in query for phrase in self.capability_questions) and len(query.split()) < 10:
            return True
        
        return False
    
    def get_direct_response(self, query: str) -> Optional[str]:
        """
        Generate a direct response for a simple query.
        
        Args:
            query: The user's query string
            
        Returns:
            Response string or None if no direct response is available
        """
        query = query.lower().strip()
        
        # Handle greetings
        if any(query == greeting for greeting in self.greetings):
            return "Hello! I'm your Maintenance Performance Analyst. How can I help you today?"
        
        # Handle thank you messages
        if any(phrase in query for phrase in self.thanks_phrases):
            return "You're welcome! Let me know if you need any other assistance with maintenance analysis or scheduling."
        
        # Handle tool queries
        if any(phrase in query for phrase in self.tool_queries):
            return """I have access to the following specialized tools:

1. **Database Query Tool** - Access records about mechanics, tasks, and schedules
2. **Raw Maintenance Data Tool** - Analyze historical maintenance records 
3. **Scheduled Maintenance Tool** - Create new maintenance schedules based on failure analysis
4. **Mechanic Performance Tool** - Analyze individual mechanic metrics
5. **Machine Analysis Tools** - Examine machine types and failure patterns

Which of these would you like me to use?"""
        
        # Handle analysis queries
        if any(phrase in query for phrase in self.analysis_queries):
            return """I can perform the following types of data analyses:

1. **Mechanic Performance Analysis** - Compare response times, repair times, and identify performance patterns
2. **Machine Performance Analysis** - Identify problematic machines and failure patterns
3. **Root Cause Analysis** - Determine common failure reasons and problematic combinations
4. **Preventative Maintenance Planning** - Use clustering to predict maintenance needs
5. **Statistical Benchmarking** - Z-score analysis and significance testing

Would you like me to perform any particular analysis?"""
        
        # Handle capability questions
        if any(phrase in query for phrase in self.capability_questions):
            return """I can help with:

1. Accessing database records (mechanics, tasks, schedules)
2. Analyzing mechanic performance data
3. Creating scheduled maintenance plans
4. Examining machine failure patterns
5. Providing statistical insights on maintenance operations

What would you like help with today?"""
        
        # No direct response available
        return None
    
    def get_tool_routing(self, query: str) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        """
        Determine if the query should be routed to a specific tool.
        
        This version no longer attempts to route database queries or analyze table names.
        Instead, it only identifies maintenance schedule creation requests.
        
        Args:
            query: The user's query string
            
        Returns:
            Tuple of (tool_name, tool_params) where tool_name is the name of the tool
            to use and tool_params is a dictionary of parameters for that tool.
            Returns (None, None) if no specific tool is needed.
        """
        query_lower = query.lower()
        
        # Only handle maintenance scheduling creation requests
        # All other database and analysis decisions are delegated to the LLM
        maintenance_keywords = [
            "create schedule", "generate schedule", "make schedule",
            "new maintenance", "create maintenance", "schedule service",
            "run maintenance workflow"
        ]
        
        if any(keyword in query_lower for keyword in maintenance_keywords):
            logger.info(f"Detected maintenance scheduler request: {query}")
            return "RunScheduledMaintenance", {}
        
        # All other tool decisions are delegated to the LLM
        return None, None
    
    # Enhanced FastPathDetector to handle more direct database queries

def needs_database_query(self, query: str) -> Tuple[bool, Optional[str]]:
    """
    Enhanced database query detection with direct routing for common queries.
    
    Args:
        query: The user's query string
        
    Returns:
        Tuple of (needs_database, query_params)
    """
    query_lower = query.lower()
    
    # Direct patterns for tasks table
    if any(pattern in query_lower for pattern in [
        "open task", "current task", "active task", "open maintenance task",
        "what task", "which task", "list task", "show task", "task status",
        "maintenance task", "performance task", "what are the task"
    ]):
        logger.info(f"Direct routing to tasks table for query: {query}")
        
        # Check for status filter
        if any(word in query_lower for word in ["open", "current", "active"]):
            return True, "tasks:*;status=open;limit=100"
        elif any(word in query_lower for word in ["closed", "completed", "done", "finished"]):
            return True, "tasks:*;status=completed;limit=100"
        else:
            return True, "tasks:*;limit=100"
    
    # Direct patterns for scheduled maintenance
    if any(pattern in query_lower for pattern in [
        "scheduled maintenance", "machine maintenance", "maintenance schedule",
        "service schedule", "due maintenance", "upcoming maintenance",
        "what maintenance", "show maintenance", "maintenance due", "machine service"
    ]):
        logger.info(f"Direct routing to scheduled_maintenance table for query: {query}")
        
        # Check for status filter
        if any(word in query_lower for word in ["open", "current", "active", "upcoming", "pending", "due"]):
            return True, "scheduled_maintenance:*;status=open;limit=100"
        elif any(word in query_lower for word in ["closed", "completed", "done", "finished", "past"]):
            return True, "scheduled_maintenance:*;status=completed;limit=100"
        else:
            return True, "scheduled_maintenance:*;limit=100"
    
    # Direct patterns for mechanics
    if any(pattern in query_lower for pattern in [
        "list mechanic", "show mechanic", "all mechanic", "available mechanic",
        "who are the mechanic", "which mechanic", "mechanic list", "technician", "engineer"
    ]):
        logger.info(f"Direct routing to mechanics table for query: {query}")
        
        # Check for active/inactive filter
        if "active" in query_lower:
            return True, "mechanics:*;active=true;limit=100"
        elif "inactive" in query_lower:
            return True, "mechanics:*;active=false;limit=100"
        else:
            return True, "mechanics:*;limit=100"
    
    # More general database query patterns - use this as a fallback
    return False, None