# /Users/melville/Documents/Industrial_Engineering_Agent/src/api/routes/chat.py

import os
import sys
import logging
import time
import json
from typing import Dict, Any
from fastapi import APIRouter, Body

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("chat_api")

router = APIRouter()

# Dynamically add the project root to the sys path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../.."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
    logger.info(f"Added {project_root} to Python path")

# Import the TwoTierOrchestrator and tool registry
try:
    from src.MCP.two_tier_orchestrator import TwoTierOrchestrator
    from src.MCP.token_tracker import token_tracker
    from src.MCP.tool_registry import tool_registry, register_query_tools_if_needed
    from src.MCP.session_manager import SessionManager
except ImportError:
    # Fallback for different import paths
    from MCP.two_tier_orchestrator import TwoTierOrchestrator
    from MCP.token_tracker import token_tracker
    from MCP.tool_registry import tool_registry, register_query_tools_if_needed
    from MCP.session_manager import SessionManager

# Ensure query tools are registered
try:
    register_query_tools_if_needed()
    logger.info("Query tools registration completed")
except Exception as e:
    logger.warning(f"Standard query tools registration failed: {e}")
    # Manual registration as fallback
    try:
        from src.MCP.query_manager import QueryManager
        from src.agents.maintenance.tools.query_tools.scheduled_maintenance_query import ScheduledMaintenanceQueryTool
        
        query_manager = QueryManager()
        query_manager.register_query_tool("scheduled_maintenance", ScheduledMaintenanceQueryTool())
        
        tool_registry.register_tool(
            name="quick_query",
            function=query_manager.execute_query,
            description="Execute quick database queries for maintenance tasks",
            category="data_retrieval",
            parameters={
                "query": {
                    "type": "string",
                    "description": "Natural language query about maintenance tasks",
                    "required": True
                }
            }
        )
        logger.info("Manual quick_query registration successful")
    except Exception as e2:
        logger.error(f"Manual query tools registration also failed: {e2}")

# Log current tools
current_tools = tool_registry.get_tool_names()
logger.info(f"Available tools: {current_tools}")
logger.info(f"quick_query available: {'quick_query' in current_tools}")

# Initialize the orchestrator with the tool registry
two_tier_orchestrator = TwoTierOrchestrator(tool_registry=tool_registry)
logger.info("Initialized TwoTierOrchestrator with tool_registry")

# Session management
active_sessions = {}

@router.post("/agent/chat")
def chat(payload: Dict[str, Any] = Body(...)):
    """
    Process a chat request through the two-tier architecture.
    
    Args:
        payload: Request payload with query and optional session_id
        
    Returns:
        Formatted response
    """
    start_time = time.time()
    query = payload.get("query", "")
    session_id = payload.get("session_id")
    user_id = payload.get("user_id")  # Get user_id from frontend
    logger.info(f"Received query: {query} from user: {user_id or 'anonymous'}")
    
    # Handle session management
    if not session_id:
        # Create new session
        session_id = f"sess_{int(time.time())}"
        logger.info(f"Created new session: {session_id} for user: {user_id or 'anonymous'}")
    
    # Get or create session manager with shorter timeout for testing
    if session_id not in active_sessions:
        active_sessions[session_id] = SessionManager(session_id, timeout_minutes=2)  # 2 minute timeout for testing
        logger.info(f"Initialized session manager for {session_id}")
    
    session_manager = active_sessions[session_id]
    
    # Check if session has expired
    if session_manager.is_session_expired():
        logger.info(f"Session {session_id} has expired, ending and creating summary")
        two_tier_orchestrator.end_session(session_id, reason="timeout", user_id=user_id)
        del active_sessions[session_id]
        
        # Create new session for this query with shorter timeout
        session_id = f"sess_{int(time.time())}"
        active_sessions[session_id] = SessionManager(session_id, timeout_minutes=2)  # 2 minute timeout for testing
        session_manager = active_sessions[session_id]
        logger.info(f"Created new session after timeout: {session_id} for user: {user_id or 'anonymous'}")
    
    # Update session activity
    session_manager.update_activity()
    
    # Set session ID in orchestrator
    two_tier_orchestrator.set_session_id(session_id)
    
    try:
        # Process through two-tier orchestrator
        response = two_tier_orchestrator.process_query(query, user_id=user_id)
        
        execution_time = time.time() - start_time
        logger.info(f"Query processed in {execution_time:.2f} seconds")
        
        # Add session ID and token usage statistics to response
        token_usage = two_tier_orchestrator.get_token_usage()
        response["session_id"] = session_id
        response["user_id"] = user_id  # Include user_id in response
        response["token_usage"] = token_usage
        response["execution_time_seconds"] = round(execution_time, 2)
        
        # Check if session should end due to goodbye
        should_end_session = response.get("session_will_end", False) or response.get("end_session", False)
        if should_end_session:
            response["sessionEnded"] = True
            logger.info(f"Session {session_id} will end after response")
            
            # End session AFTER usage tracking is complete (small delay to ensure DB write)
            import threading
            def delayed_session_end():
                import time
                time.sleep(0.5)  # Wait 500ms for usage log to be written
                try:
                    success = two_tier_orchestrator.end_session(session_id, reason="user_goodbye", user_id=user_id)
                    if success:
                        logger.info(f"Session {session_id} ended and summarized successfully")
                    else:
                        logger.warning(f"Session {session_id} end failed")
                        
                    # Remove from active sessions
                    if session_id in active_sessions:
                        del active_sessions[session_id]
                        
                except Exception as e:
                    logger.error(f"Error in delayed session end: {e}")
            
            # Start delayed session end in background
            threading.Thread(target=delayed_session_end, daemon=True).start()
        
        return response
    except Exception as e:
        logger.error(f"Error processing query: {e}", exc_info=True)
        return {
            "error": str(e), 
            "answer": f"I'm sorry, I encountered an error while processing your query: {str(e)}",
            "session_id": session_id,
            "user_id": user_id
        }

@router.get("/token-usage")
def get_token_usage():
    """Get token usage statistics for the current session."""
    try:
        token_usage = two_tier_orchestrator.get_token_usage()
        return token_usage
    except Exception as e:
        logger.error(f"Error getting token usage: {e}")
        return {"error": str(e)}

@router.post("/reset-token-usage")
def reset_token_usage():
    """Reset token usage statistics."""
    try:
        token_tracker.reset_session()
        return {"status": "Token usage statistics reset successfully"}
    except Exception as e:
        logger.error(f"Error resetting token usage: {e}")
        return {"error": str(e)}

@router.get("/debug/tools")
def debug_tools():
    """Debug endpoint to check available tools."""
    try:
        available_tools = tool_registry.get_all_tools()
        return {
            "tool_count": len(available_tools),
            "tools": available_tools,
            "chatgpt_has_registry": hasattr(two_tier_orchestrator.chatgpt_agent, 'tool_registry') and two_tier_orchestrator.chatgpt_agent.tool_registry is not None
        }
    except Exception as e:
        logger.error(f"Error getting tools: {e}")
        return {"error": str(e)}
@router.post("/agent/end_session")
def end_session(payload: Dict[str, Any] = Body(...)):
    """End a session manually and create summary."""
    session_id = payload.get("session_id")
    user_id = payload.get("user_id")  # Get user_id for session summary
    
    if not session_id:
        return {"error": "session_id is required"}
    
    try:
        # End session in orchestrator (creates summary)
        success = two_tier_orchestrator.end_session(session_id, reason="manual", user_id=user_id)
        
        # Remove from active sessions
        if session_id in active_sessions:
            del active_sessions[session_id]
        
        return {
            "status": "success" if success else "warning",
            "message": f"Session {session_id} ended {'successfully' if success else 'with issues'}",
            "session_id": session_id,
            "user_id": user_id
        }
    except Exception as e:
        logger.error(f"Error ending session: {e}")
        return {"error": str(e)}

@router.get("/agent/session_info/{session_id}")
def get_session_info(session_id: str):
    """Get information about a specific session."""
    try:
        if session_id in active_sessions:
            session_manager = active_sessions[session_id]
            return session_manager.get_session_info()
        else:
            return {"error": f"Session {session_id} not found or expired"}
    except Exception as e:
        logger.error(f"Error getting session info: {e}")
        return {"error": str(e)}

@router.get("/agent/active_sessions")
def get_active_sessions():
    """Get list of all active sessions."""
    try:
        session_info = {}
        for session_id, session_manager in active_sessions.items():
            session_info[session_id] = session_manager.get_session_duration()
        
        return {
            "active_session_count": len(active_sessions),
            "sessions": session_info
        }
    except Exception as e:
        logger.error(f"Error getting active sessions: {e}")
        return {"error": str(e)}

@router.post("/agent/force_session_summary")
def force_session_summary(payload: Dict[str, Any] = Body(...)):
    """Force create a session summary for testing."""
    session_id = payload.get("session_id")
    user_id = payload.get("user_id")
    
    if not session_id:
        return {"error": "session_id is required"}
    
    try:
        # Manually create session summary
        success = two_tier_orchestrator.session_summarizer.create_session_summary(
            session_id=session_id,
            user_id=user_id,
            session_ended_reason="manual_test"
        )
        
        return {
            "status": "success" if success else "failed",
            "message": f"Session summary {'created' if success else 'failed'} for {session_id}",
            "session_id": session_id,
            "user_id": user_id
        }
    except Exception as e:
        logger.error(f"Error creating session summary: {e}", exc_info=True)
        return {"error": str(e)}

@router.get("/agent/test_session_data/{session_id}")
def test_session_data(session_id: str):
    """Check if we have usage data for a session."""
    try:
        # Check if there are usage logs for this session
        from src.shared_services.supabase_client import get_shared_supabase_client
        supabase = get_shared_supabase_client()
        
        result = supabase.table("agent_usage_logs").select("*").eq("session_id", session_id).execute()
        
        usage_logs = result.data if result.data else []
        
        return {
            "session_id": session_id,
            "usage_logs_count": len(usage_logs),
            "usage_logs": usage_logs[:3] if usage_logs else [],  # First 3 for preview
            "has_data": len(usage_logs) > 0
        }
    except Exception as e:
        logger.error(f"Error checking session data: {e}")
        return {"error": str(e)}
