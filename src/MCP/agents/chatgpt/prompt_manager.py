# /src/MCP/agents/chatgpt/prompt_manager.py
"""
Prompt and Date Management for ChatGPT Agent

Handles system prompt loading, date utilities, and prompt formatting.
"""

import os
import logging
from datetime import datetime
from typing import Dict

logger = logging.getLogger("prompt_manager")

class PromptManager:
    """
    Manages system prompts and date handling for the ChatGPT agent.
    
    This component handles:
    - System prompt loading from files
    - Date utilities and formatting
    - Prompt caching and refresh
    """
    
    def __init__(self):
        """Initialize the prompt manager."""
        self.current_date = self._get_current_date()
        self.system_prompt = self._load_system_prompt()
        logger.info("PromptManager initialized")
    
    def _get_current_date(self) -> Dict[str, str]:
        """Get the current date in multiple formats."""
        now = datetime.now()
        return {
            "current_date": now.strftime("%Y-%m-%d"),
            "current_datetime": now.isoformat(),
            "formatted_date": now.strftime("%B %d, %Y"),
            "day_of_week": now.strftime("%A"),
            "time": now.strftime("%H:%M:%S")
        }
    
    def refresh_date(self):
        """Refresh the cached date - useful for long-running sessions."""
        self.current_date = self._get_current_date()
        logger.info(f"Refreshed current date to: {self.current_date['formatted_date']}")
    
    def get_current_date(self) -> Dict[str, str]:
        """Get the current date information."""
        return self.current_date.copy()

    def _load_system_prompt(self) -> str:
        """Load the system prompt without any hardcoded dates."""
        try:
            # Try loading from the maintenance prompts directory
            prompt_path = "/Users/melville/Documents/Industrial_Engineering_Agent/src/agents/maintenance/prompts/system_prompt.txt"
            
            if os.path.exists(prompt_path):
                with open(prompt_path, 'r') as f:
                    prompt = f.read().strip()
                
                logger.info(f"Loaded system prompt from {prompt_path}")
                return prompt
            else:
                logger.warning(f"System prompt file not found at {prompt_path}")
                
        except Exception as e:
            logger.error(f"Error loading system prompt: {e}")
        
        # Fallback to enhanced minimal prompt without dates
        logger.info("Using fallback enhanced system prompt")
        return """You are a maintenance management assistant. 

Your goal is to help users with:
1. Scheduling and tracking maintenance tasks
2. Querying maintenance data
3. Analyzing performance metrics

When answering questions:
- ALWAYS use the current date provided by the get_current_date function result
- The current date is provided at the start of each conversation
- Use this date for all calculations and comparisons
- Never guess or use a date from training data
- Consider the context of previous messages
- For follow-up questions, analyze the previous response
- Be specific and helpful in your responses
- When maintenance tasks are past due, calculate overdue days based on the current date

For complex analysis, indicate that deeper investigation is needed."""
    
    def get_system_prompt(self) -> str:
        """Get the system prompt."""
        return self.system_prompt
