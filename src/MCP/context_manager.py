# /src/MCP/context_manager.py
import logging
from typing import Dict, Any, List, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("context_manager")

class ContextManager:
    """
    Simplified context manager for the two-tier architecture.
    
    This class maintains conversation history and recent context
    in a format that can be used by both OpenAI and DeepSeek.
    """
    
    def __init__(self, session_manager=None, max_history: int = 6):
        """
        Initialize the context manager.
        
        Args:
            session_manager: Optional session manager for persistence
            max_history: Maximum number of messages to keep in history (default: 6 for 3 exchanges)
        """
        self.session_manager = session_manager
        self.max_history = max_history  # Reduced from 10 to 6 for token savings
        self.conversation_history = []
        
        # Load history from session manager if available
        if session_manager:
            self._load_from_session()
            
        logger.info(f"ContextManager initialized (max_history={max_history})")
    
    def _load_from_session(self):
        """Load conversation history from session manager."""
        if self.session_manager:
            full_history = self.session_manager.get_conversation_history() or []
            # Only load the most recent messages
            self.conversation_history = full_history[-self.max_history:] if full_history else []
            logger.info(f"Loaded {len(self.conversation_history)} messages from session")
    
    def add_message(self, role: str, content: str, function_name: Optional[str] = None):
        """
        Add a message to the conversation history.
        
        Args:
            role: Message role ('user', 'assistant', or 'function')
            content: Message content
            function_name: Optional function name for function messages
        """
        # Truncate content if it's too long (for token savings)
        max_content_length = 2000  # Limit message size
        if len(content) > max_content_length:
            content = content[:max_content_length] + "... [truncated]"
            logger.info(f"Truncated {role} message to {max_content_length} characters")
        
        if role == 'function' and function_name:
            message = {
                "role": role,
                "name": function_name,
                "content": content
            }
        else:
            message = {
                "role": role,
                "content": content
            }
        
        self.conversation_history.append(message)
        
        # Trim history if needed
        if len(self.conversation_history) > self.max_history:
            # Remove oldest messages but try to keep user-assistant pairs
            excess = len(self.conversation_history) - self.max_history
            # Remove in pairs when possible to maintain context
            if excess >= 2:
                self.conversation_history = self.conversation_history[excess:]
            else:
                self.conversation_history = self.conversation_history[1:]
            
            logger.debug(f"Trimmed history to {len(self.conversation_history)} messages")
        
        # Save to session manager if available
        if self.session_manager:
            self.session_manager.save_conversation_history(self.conversation_history)
            
        logger.debug(f"Added {role} message to history. Total: {len(self.conversation_history)}")
    
    def get_recent_history(self, count: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get recent conversation history.
        
        Args:
            count: Number of recent messages to return (default: all)
            
        Returns:
            List of recent messages
        """
        if count is None:
            return self.conversation_history.copy()
        
        # Ensure we get complete exchanges (user-assistant pairs)
        if count % 2 != 0:
            count += 1  # Make it even to get complete exchanges
            
        return self.conversation_history[-count:].copy()
    
    def get_context_for_deepseek(self) -> str:
        """
        Get formatted context for DeepSeek agent.
        
        Returns:
            Context string (limited for token efficiency)
        """
        context_parts = []
        
        # Only use the most recent messages for context
        recent_messages = self.get_recent_history(4)  # Last 2 exchanges
        
        for msg in recent_messages:
            role = msg.get("role", "")
            content = msg.get("content", "")
            
            # Further truncate content for DeepSeek context
            if len(content) > 500:
                content = content[:500] + "..."
            
            if role == "user":
                context_parts.append(f"USER: {content}")
            elif role == "assistant":
                context_parts.append(f"ASSISTANT: {content}")
            elif role == "function":
                function_name = msg.get("name", "unknown")
                # Summarize function results instead of full content
                result_preview = content[:200] + "..." if len(content) > 200 else content
                context_parts.append(f"FUNCTION '{function_name}': {result_preview}")
        
        return "\n".join(context_parts)
    
    def clear_history(self):
        """Clear the conversation history."""
        self.conversation_history = []
        
        # Clear session if available
        if self.session_manager:
            self.session_manager.clear_conversation_history()
            
        logger.info("Conversation history cleared")
    
    def get_summary_for_handoff(self) -> str:
        """
        Get a summary of the conversation for handoff between agents.
        
        Returns:
            Summary string optimized for token usage
        """
        if not self.conversation_history:
            return "No previous conversation."
        
        # Get the last user query and any relevant function results
        last_user_query = None
        last_function_result = None
        
        for msg in reversed(self.conversation_history):
            if msg["role"] == "user" and not last_user_query:
                last_user_query = msg["content"]
            elif msg["role"] == "function" and not last_function_result:
                last_function_result = f"{msg.get('name', 'function')}: {msg['content'][:200]}..."
            
            if last_user_query and last_function_result:
                break
        
        summary_parts = []
        if last_user_query:
            summary_parts.append(f"Query: {last_user_query}")
        if last_function_result:
            summary_parts.append(f"Result: {last_function_result}")
            
        return "\n".join(summary_parts)