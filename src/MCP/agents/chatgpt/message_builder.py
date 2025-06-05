# /src/MCP/agents/chatgpt/message_builder.py
"""
Message Building and Formatting for ChatGPT Agent

Handles OpenAI message construction, date injection, and conversation formatting.
"""

import json
import logging
from typing import List, Dict, Optional
from openai.types.chat import ChatCompletionMessageParam

logger = logging.getLogger("message_builder")

class MessageBuilder:
    """
    Builds OpenAI-compatible messages from conversation history and user queries.
    
    This component handles:
    - Message formatting for OpenAI API
    - Date injection and acknowledgment
    - Conversation history management
    - Token optimization
    """
    
    def __init__(self, prompt_manager):
        """Initialize the message builder."""
        self.prompt_manager = prompt_manager
        logger.info("MessageBuilder initialized")
    
    def build_messages(self, query: str, conversation_history: Optional[List[Dict[str, str]]] = None) -> List[ChatCompletionMessageParam]:
        """
        Build OpenAI messages from query and conversation history.
        
        Args:
            query: The user's current query
            conversation_history: Optional conversation history
            
        Returns:
            List of ChatCompletionMessageParam for OpenAI API
        """
        messages: List[ChatCompletionMessageParam] = []
        
        # Start with system prompt
        system_prompt = self.prompt_manager.get_system_prompt()
        messages.append({"role": "system", "content": system_prompt})
        
        # ALWAYS inject the current date as a function message
        current_date = self.prompt_manager.get_current_date()
        date_message = {
            "role": "function",
            "name": "get_current_date",
            "content": json.dumps(current_date)
        }
        messages.append(date_message)  # type: ignore
        logger.debug(f"Injected date message: {date_message['content']}")
        
        # Add assistant acknowledgment of the date
        messages.append({
            "role": "assistant",
            "content": f"I understand that today is {current_date['formatted_date']} ({current_date['day_of_week']}). I'll use this date for all calculations."
        })
        
        # Add conversation history
        if conversation_history:
            recent_history = conversation_history[-6:]  # Last 3 user-assistant pairs
            for msg in recent_history:
                role = msg["role"]
                content = msg["content"]
                if role in ["user", "assistant", "system", "function"] and content is not None:
                    messages.append({"role": role, "content": str(content)})  # type: ignore
        
        # Add the current query
        messages.append({"role": "user", "content": query})  # type: ignore
        
        return messages
