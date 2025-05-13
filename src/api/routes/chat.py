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
from MCP.two_tier_orchestrator import TwoTierOrchestrator
from MCP.token_tracker import token_tracker
from MCP.tool_registry import tool_registry

# Initialize the orchestrator with the tool registry
two_tier_orchestrator = TwoTierOrchestrator(tool_registry=tool_registry)
logger.info("Initialized TwoTierOrchestrator with tool_registry")

@router.post("/agent/chat")
def chat(payload: Dict[str, Any] = Body(...)):
    """
    Process a chat request through the two-tier architecture.
    
    Args:
        payload: Request payload with query
        
    Returns:
        Formatted response
    """
    start_time = time.time()
    query = payload.get("query", "")
    logger.info(f"Received query: {query}")
    
    try:
        # Process through two-tier orchestrator
        response = two_tier_orchestrator.process_query(query)
        
        execution_time = time.time() - start_time
        logger.info(f"Query processed in {execution_time:.2f} seconds")
        
        # Add token usage statistics
        token_usage = two_tier_orchestrator.get_token_usage()
        response["token_usage"] = token_usage
        
        return response
    except Exception as e:
        logger.error(f"Error processing query: {e}", exc_info=True)
        return {"error": str(e), "answer": f"I'm sorry, I encountered an error while processing your query: {str(e)}"}

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