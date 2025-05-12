# src/MCP/two_tier_orchestrator.py
import logging
import json
import time
from typing import Dict, Any, List, Optional, Tuple, Union

# Import the agents
from MCP.agents.chatgpt_agent import ChatGPTAgent
from MCP.agents.deepseek_agent import DeepSeekAgent
from MCP.token_tracker import token_tracker
from MCP.query_manager import QueryManager
from MCP.response_formatter import MCPResponseFormatter
from MCP.context_manager import ContextManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("two_tier_orchestrator")

class TwoTierOrchestrator:
    """
    Orchestrates the two-tier agent system with ChatGPT and DeepSeek.
    
    This class:
    1. Routes simple queries to the quick query system
    2. Manages conversations between the user and agents
    3. Routes complex queries to the appropriate agent
    4. Handles context sharing between agents
    5. Tracks conversation history
    """
    
    def __init__(self, tool_registry=None):
        """
        Initialize the orchestrator.
        
        Args:
            tool_registry: Optional tool registry for tool execution
        """
        self.tool_registry = tool_registry
        self.chatgpt_agent = ChatGPTAgent(tool_registry=tool_registry)
        self.deepseek_agent = None  # Will be initialized on first use
        self.context_manager = ContextManager()
        self.query_manager = QueryManager()
        self.response_formatter = MCPResponseFormatter(tool_registry=tool_registry)
        
        # Initialize query tools with the query manager
        self._initialize_query_tools()
        
        logger.info("Two-Tier Orchestrator initialized")
    
    def _initialize_query_tools(self):
        """Initialize query tools with the query manager."""
        try:
            from src.agents.maintenance.tools.query_tools.watchlist_query import WatchlistQueryTool
            from src.agents.maintenance.tools.query_tools.scheduled_maintenance_query import ScheduledMaintenanceQueryTool
            
            # Register query tools
            self.query_manager.register_query_tool("watchlist", WatchlistQueryTool())
            self.query_manager.register_query_tool("scheduled_maintenance", ScheduledMaintenanceQueryTool())
            
            logger.info("Query tools initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing query tools: {e}")
    
    def process_query(self, query: str) -> Dict[str, Any]:
        """
        Process a user query through the two-tier system.
        
        Args:
            query: The user's query
            
        Returns:
            Response dictionary with answer and metadata
        """
        start_time = time.time()
        logger.info(f"Processing query: {query}")
        
        # Add query to context manager
        self.context_manager.add_message("user", query)
        
        try:
            # Step 1: Check if this is a quick query
            quick_query_result = self._try_quick_query(query)
            if quick_query_result and quick_query_result.get("success"):
                logger.info("Query handled by quick query system")
                
                # Format the response
                formatted_response = self._format_quick_query_response(quick_query_result)
                
                # Add response to context
                self.context_manager.add_message("assistant", formatted_response["answer"])
                
                execution_time = time.time() - start_time
                logger.info(f"Quick query processed in {execution_time:.2f} seconds")
                
                return formatted_response
            
            # Step 2: Try with ChatGPT for other queries
            logger.info("Attempting to process with ChatGPT")
            
            # Get minimal context for ChatGPT
            recent_history = self.context_manager.get_recent_history(6)
            
            gpt_result = self.chatgpt_agent.process_query(query, conversation_history=recent_history)
            
            # Track token usage if available
            if "response" in gpt_result:
                token_tracker.track_openai_usage(gpt_result["response"])
            
            # Check if DeepSeek is needed
            if "requires_deepseek" in gpt_result and gpt_result["requires_deepseek"]:
                logger.info("Query requires DeepSeek, handing off")
                
                # Get context for handoff
                handoff_context = self.context_manager.get_summary_for_handoff()
                
                # Call DeepSeek
                deepseek_result = self._call_deepseek(query, handoff_context)
                
                # Track token usage if available
                if "response" in deepseek_result:
                    token_tracker.track_openai_usage(deepseek_result["response"])
                
                # Add DeepSeek response to context
                self.context_manager.add_message("assistant", deepseek_result["answer"])
                
                execution_time = time.time() - start_time
                logger.info(f"Query processed with DeepSeek in {execution_time:.2f} seconds")
                
                return deepseek_result
            
            # If ChatGPT handled it successfully
            if "answer" in gpt_result:
                # Add response to context
                self.context_manager.add_message("assistant", gpt_result["answer"])
                
                execution_time = time.time() - start_time
                logger.info(f"Query processed with ChatGPT in {execution_time:.2f} seconds")
                
                return gpt_result
            
            # If there was an error with ChatGPT
            if "error" in gpt_result:
                logger.error(f"Error with ChatGPT: {gpt_result['error']}")
                
                # Fall back to DeepSeek
                deepseek_result = self._call_deepseek(query, None)
                
                # Add DeepSeek response to context
                self.context_manager.add_message("assistant", deepseek_result["answer"])
                
                execution_time = time.time() - start_time
                logger.info(f"Query processed with DeepSeek (fallback) in {execution_time:.2f} seconds")
                
                return deepseek_result
            
            # If we get here, something unexpected happened
            return {
                "answer": "I apologize, but I encountered an unexpected error while processing your query.",
                "error": "Unexpected response format from ChatGPT"
            }
            
        except Exception as e:
            logger.error(f"Error processing query: {e}")
            return {
                "answer": f"I apologize, but I encountered an error while processing your query: {str(e)}",
                "error": str(e)
            }
    
    def _try_quick_query(self, query: str) -> Optional[Dict[str, Any]]:
        """
        Try to handle the query with the quick query system.
        
        Args:
            query: The user's query
            
        Returns:
            Query result or None if not a quick query
        """
        try:
            # Check if the query manager can handle this
            query_type, params = self.query_manager.classify_query(query)
            
            if query_type:
                logger.info(f"Query classified as '{query_type}' - using quick query system")
                result = self.query_manager.execute_query(query)
                return result
            
            return None
            
        except Exception as e:
            logger.error(f"Error in quick query system: {e}")
            return None
    
    def _format_quick_query_response(self, query_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format a quick query response for the user.
        
        Args:
            query_result: Result from the quick query system
            
        Returns:
            Formatted response dictionary
        """
        if not query_result.get("success"):
            return {
                "answer": f"I encountered an error with your query: {query_result.get('error', 'Unknown error')}",
                "error": query_result.get("error")
            }
        
        # Use the response formatter to format the data
        data = query_result.get("data", [])
        format_type = query_result.get("format", "table")
        query_type = query_result.get("query_type", "unknown")
        
        # Create a formatted response
        if format_type == "table" and data:
            # Format as a table
            formatted_answer = self.response_formatter.format_data_adaptively(
                data=data,
                query=f"Results for {query_type}",
                data_type=query_type
            )
        else:
            formatted_answer = query_result.get("message", "Query executed successfully")
        
        return {
            "answer": formatted_answer,
            "query_type": query_type,
            "format": format_type,
            "count": len(data) if isinstance(data, list) else 1
        }
    
    def _call_deepseek(self, query: str, context: Optional[str] = None) -> Dict[str, Any]:
        """
        Call the DeepSeek agent.
        
        Args:
            query: The user's query
            context: Optional context for the query
            
        Returns:
            Response from DeepSeek
        """
        # Initialize DeepSeek agent if not already done
        if self.deepseek_agent is None:
            try:
                self.deepseek_agent = DeepSeekAgent()
                logger.info("DeepSeek agent initialized")
            except Exception as e:
                logger.error(f"Error initializing DeepSeek agent: {e}")
                return {
                    "answer": "I'm sorry, I'm unable to analyze this deeply right now. There was an error initializing the advanced analytics module.",
                    "error": str(e)
                }
        
        try:
            # Prepare the query with context if available
            if context:
                enhanced_query = f"CONTEXT: {context}\n\nQUERY: {query}"
            else:
                enhanced_query = query
            
            # Run the DeepSeek agent
            result = self.deepseek_agent.run(enhanced_query)
            
            return {
                "answer": result,
                "used_deepseek": True
            }
        except Exception as e:
            logger.error(f"Error calling DeepSeek agent: {e}")
            return {
                "answer": f"I apologize, but I encountered an error while attempting to analyze this deeply: {str(e)}",
                "error": str(e)
            }

    def get_token_usage(self) -> Dict[str, Any]:
        """
        Get token usage statistics for the current session.
        
        Returns:
            Dictionary containing token usage statistics
        """
        return token_tracker.get_session_summary()