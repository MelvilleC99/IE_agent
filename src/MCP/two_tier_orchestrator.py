# src/MCP/two_tier_orchestrator.py
import logging
import json
import time
import uuid
from typing import Dict, Any, List, Optional, Tuple, Union

# Import the agents
from src.MCP.agents.chatgpt_agent import ChatGPTAgent
from src.MCP.agents.deepseek_agent import DeepSeekAgent
from src.MCP.token_tracker import token_tracker
from src.MCP.response_formatter import MCPResponseFormatter
from src.MCP.context_manager import ContextManager

# Import cost tracking
from src.cost_tracking.usage_tracker import UsageTracker
from src.cost_tracking.session_summarizer import SessionSummarizer

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
        self.chatgpt_agent = ChatGPTAgent(tool_registry=self.tool_registry)
        self.deepseek_agent = None  # Will be initialized on first use
        self.context_manager = ContextManager()
        self.response_formatter = MCPResponseFormatter(tool_registry=tool_registry)
        
        # Initialize cost tracking
        self.usage_tracker = UsageTracker()
        self.session_summarizer = SessionSummarizer()
        
        # Connect usage tracker to ChatGPT agent
        self.chatgpt_agent.set_usage_tracker(self.usage_tracker)
        
        logger.info("Two-Tier Orchestrator initialized with cost tracking")
    
    def process_query(self, query: str, user_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Process a user query through the two-tier system.
        
        Args:
            query: The user's query
            user_id: Optional user identifier for tracking
            
        Returns:
            Response dictionary with answer and metadata
        """
        start_time = time.time()
        logger.info(f"Processing query: {query} for user: {user_id or 'anonymous'}")
        
        # Generate tracking IDs
        session_id = getattr(self, '_current_session_id', f"sess_{int(time.time())}")
        conversation_id = f"conv_{uuid.uuid4().hex[:8]}"
        
        # Start cost tracking
        try:
            self.usage_tracker.start_query_tracking(
                session_id=session_id,
                conversation_id=conversation_id,
                query_text=query,
                user_id=user_id  # Pass user_id to tracking
            )
        except Exception as e:
            logger.warning(f"Failed to start usage tracking: {e}")
        
        # Add query to context manager
        self.context_manager.add_message("user", query)
        
        try:
            # Always send to ChatGPTAgent first
            recent_history = self.context_manager.get_recent_history(6)
            gpt_result = self.chatgpt_agent.process_query(
                query, 
                conversation_history=recent_history,
                conversation_id=conversation_id  # Pass tracking ID
            )
            
            # Track token usage if available
            if "response" in gpt_result:
                token_tracker.track_openai_usage(gpt_result["response"])
            
            # Check if DeepSeek is needed (skip DeepSeek tracking for now)
            if "requires_deepseek" in gpt_result and gpt_result["requires_deepseek"]:
                logger.info("Query requires DeepSeek, handing off")
                handoff_context = self.context_manager.get_summary_for_handoff()
                deepseek_result = self._call_deepseek(query, handoff_context)
                if "response" in deepseek_result:
                    token_tracker.track_openai_usage(deepseek_result["response"])
                self.context_manager.add_message("assistant", deepseek_result["answer"])
                
                # Complete tracking for DeepSeek handoff
                try:
                    self.usage_tracker.complete_query_tracking(
                        conversation_id=conversation_id,
                        success=True,
                        handed_to_deepseek=True
                    )
                except Exception as e:
                    logger.warning(f"Failed to complete usage tracking: {e}")
                
                execution_time = time.time() - start_time
                logger.info(f"Query processed with DeepSeek in {execution_time:.2f} seconds")
                return deepseek_result
            
            # If ChatGPT handled it successfully
            if "answer" in gpt_result:
                self.context_manager.add_message("assistant", gpt_result["answer"])
                
                # Check if user said goodbye (end session)
                if gpt_result.get("end_session", False):
                    logger.info(f"User said goodbye, ending session {session_id}")
                    # Don't end session immediately - let usage tracking complete first
                    gpt_result["session_will_end"] = True  # Flag for API route to handle
                
                # Complete tracking for successful ChatGPT query
                try:
                    self.usage_tracker.complete_query_tracking(
                        conversation_id=conversation_id,
                        success=True,
                        response_type=gpt_result.get("response_type", "text")
                    )
                except Exception as e:
                    logger.warning(f"Failed to complete usage tracking: {e}")
                
                execution_time = time.time() - start_time
                logger.info(f"Query processed with ChatGPT in {execution_time:.2f} seconds")
                return gpt_result
            
            # If there was an error with ChatGPT
            if "error" in gpt_result:
                logger.error(f"Error with ChatGPT: {gpt_result['error']}")
                
                # Complete tracking for failed query
                try:
                    self.usage_tracker.complete_query_tracking(
                        conversation_id=conversation_id,
                        success=False,
                        error_message=gpt_result["error"]
                    )
                except Exception as e:
                    logger.warning(f"Failed to complete usage tracking: {e}")
                
                deepseek_result = self._call_deepseek(query, None)
                self.context_manager.add_message("assistant", deepseek_result["answer"])
                execution_time = time.time() - start_time
                logger.info(f"Query processed with DeepSeek (fallback) in {execution_time:.2f} seconds")
                return deepseek_result
            
            # If we get here, something unexpected happened
            try:
                self.usage_tracker.complete_query_tracking(
                    conversation_id=conversation_id,
                    success=False,
                    error_message="Unexpected response format from ChatGPT"
                )
            except Exception as e:
                logger.warning(f"Failed to complete usage tracking: {e}")
            
            return {
                "answer": "I apologize, but I encountered an unexpected error while processing your query.",
                "error": "Unexpected response format from ChatGPT"
            }
            
        except Exception as e:
            logger.error(f"Error processing query: {e}")
            
            # Complete tracking for exception
            try:
                self.usage_tracker.complete_query_tracking(
                    conversation_id=conversation_id,
                    success=False,
                    error_message=str(e)
                )
            except Exception as tracking_error:
                logger.warning(f"Failed to complete usage tracking after exception: {tracking_error}")
            
            return {
                "answer": f"I apologize, but I encountered an error while processing your query: {str(e)}",
                "error": str(e)
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
    
    def set_session_id(self, session_id: str):
        """Set the current session ID for tracking."""
        self._current_session_id = session_id
        logger.info(f"Session ID set to: {session_id}")
    
    def end_session(self, session_id: Optional[str] = None, reason: str = "manual", user_id: Optional[str] = None):
        """
        End the current session and create summary.
        
        Args:
            session_id: Session ID to end (uses current if not provided)
            reason: Reason for session end ('manual', 'timeout', 'logout')
            user_id: User ID for the session summary
        """
        if not session_id:
            session_id_attr = getattr(self, '_current_session_id', None)
            if not session_id_attr or not isinstance(session_id_attr, str):
                logger.warning("No session ID provided for session end")
                return False
            session_id = session_id_attr
        
        try:
            # Create session summary
            success = self.session_summarizer.create_session_summary(
                session_id=session_id,
                user_id=user_id,  # Pass user_id to session summary
                session_ended_reason=reason
            )
            
            if success:
                logger.info(f"Session {session_id} ended and summarized for user: {user_id or 'anonymous'}")
                # Clear current session
                if hasattr(self, '_current_session_id') and self._current_session_id == session_id:
                    delattr(self, '_current_session_id')
            
            return success
            
        except Exception as e:
            logger.error(f"Error ending session {session_id}: {e}")
            return False