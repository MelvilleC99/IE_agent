# /src/MCP/agents/chatgpt/session_detector.py
"""
Session Detection and Management for ChatGPT Agent

Handles goodbye detection, follow-up queries, and session state management.
"""

import logging
import re
from typing import List, Dict, Optional

logger = logging.getLogger("session_detector")

class SessionDetector:
    """
    Detects session events and manages session state for the ChatGPT agent.
    
    This component handles:
    - Goodbye message detection
    - Follow-up query identification  
    - Session ending logic
    """
    
    def __init__(self):
        """Initialize the session detector."""
        logger.info("SessionDetector initialized")
    
    def is_goodbye_message(self, message: Optional[str]) -> bool:
        """
        Check if a message indicates the user wants to end the session.
        
        Args:
            message: Message to check
            
        Returns:
            Boolean indicating if this is a goodbye message
        """
        if not message:
            return False
        
        message_lower = message.lower().strip()
        
        # Goodbye phrases that should trigger session end
        goodbye_phrases = [
            "goodbye", "good bye", "bye", "farewell", "see you later", 
            "see ya", "talk to you later", "ttyl", "end session",
            "quit", "exit", "done", "that's all", "thank you goodbye",
            "thanks bye", "finished", "end chat", "log out", "logout"
        ]
        
        # Check if the message contains any goodbye phrase
        for phrase in goodbye_phrases:
            if phrase in message_lower:
                logger.info(f"Detected goodbye phrase: '{phrase}' in message")
                return True
        
        # Check for simple variations
        if message_lower in ["bye!", "goodbye!", "done!", "thanks!", "finished!"]:
            logger.info(f"Detected goodbye message: '{message_lower}'")
            return True
            
        return False

    def is_follow_up_query(self, query: str, conversation_history: Optional[List[Dict[str, str]]]) -> bool:
        """Check if this query is likely a follow-up to a previous response."""
        if not conversation_history:
            return False
        
        # Semantic follow-up indicators (much more specific)
        follow_up_patterns = [
            # Person/mechanic references
            r'\b(?:for|from|by)\s+[A-Za-z]+(?:\s+[A-Za-z]+)?\b',  # "for Duncan", "by Sarah"
            r'\b[A-Za-z]+(?:\s+[A-Za-z]+)?(?:\s+items?|\s+tasks?|\s+data)\b',  # "Duncan items"
            
            # Analysis requests on current data
            r'\bsummariz[e]?\b', r'\banalyze?\b', r'\btell me more\b',
            r'\bdetails?\s+(?:about|on|for)\b', r'\bmore\s+info\b',
            
            # Filtering current data
            r'\bshow\s+(?:me\s+)?(?:the\s+)?(?:ones?|items?|tasks?)\s+(?:for|from|by)\b',
            r'\b(?:filter|show)\s+(?:by|for)\b',
            
            # Reference to previous results
            r'\bthose\s+(?:items?|tasks?|results?)\b',
            r'\bthe\s+(?:list|data|results?)\s+(?:for|from|by)\b'
        ]
        
        query_lower = query.lower()
        for pattern in follow_up_patterns:
            if re.search(pattern, query_lower):
                logger.info(f"Detected semantic follow-up pattern: {pattern}")
                return True
        
        return False
    
    def requires_deepseek(self, message: Optional[str]) -> bool:
        """
        Check if a message indicates the need for DeepSeek.
        
        Args:
            message: Message to check
            
        Returns:
            Boolean indicating if DeepSeek is needed
        """
        if not message:
            return False
            
        # Check for indicator phrases
        indicators = [
            "I need to analyze this more deeply",
            "hand this over",
            "deep analysis",
            "complex analysis",
            "advanced analytics",
            "need deeper reasoning",
            "requires more extensive analysis",
            "this requires deeper investigation"
        ]
        
        for indicator in indicators:
            if indicator.lower() in message.lower():
                return True
                
        return False
