# /Users/melville/Documents/Industrial_Engineering_Agent/src/MCP/context_manager.py

import logging
import time
from typing import Dict, Any, List, Set, Optional, Tuple
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("mcp_context_manager")

class MCPContextManager:
    """
    Manages context for Model Context Protocol (MCP) interactions.
    
    The context manager tracks conversation history, entities mentioned,
    and relevant information to provide appropriate context to the LLM.
    """
    
    def __init__(self, max_history_items: int = 10, max_context_tokens: int = 2000):
        """
        Initialize the context manager.
        
        Args:
            max_history_items: Maximum number of previous messages to include
            max_context_tokens: Approximate maximum tokens for context
        """
        self.conversation_history = []
        self.known_entities = {
            "mechanics": set(),
            "machines": set(),
            "machine_types": set(),
            "issues": set()
        }
        self.max_history_items = max_history_items
        self.max_context_tokens = max_context_tokens
        self.recent_tools_used = []
        logger.info(f"Initialized MCP Context Manager (max_history={max_history_items}, max_tokens={max_context_tokens})")
    
    def add_message(self, role: str, content: str, tools_used: Optional[List[str]] = None) -> None:
        """
        Add a message to the conversation history.
        
        Args:
            role: The role of the message sender ('user' or 'assistant')
            content: The message content
            tools_used: Any tools used in this message
        """
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        
        message = {
            "role": role,
            "content": content,
            "timestamp": timestamp,
            "tools_used": tools_used or []
        }
        
        self.conversation_history.append(message)
        
        # Add tools to recent tools
        if tools_used:
            self.recent_tools_used.extend(tools_used)
            self.recent_tools_used = self.recent_tools_used[-5:]  # Keep only the 5 most recent
        
        # Extract entities from the message
        self._extract_entities(content)
        
        # Trim history if needed
        if len(self.conversation_history) > self.max_history_items:
            self.conversation_history = self.conversation_history[-self.max_history_items:]
            
        logger.debug(f"Added message from {role}. History size: {len(self.conversation_history)}")
    
    def _extract_entities(self, text: str) -> None:
        """
        Extract relevant entities from text.
        
        Args:
            text: The text to extract entities from
        """
        # Extract mechanics (names that look like people)
        potential_names = re.findall(r'\b[A-Z][a-z]+ [A-Z][a-z]+\b', text)
        for name in potential_names:
            self.known_entities["mechanics"].add(name)
        
        # Extract machine numbers
        machine_numbers = re.findall(r'\b[A-Z]+-\d+\b|\bMachine-\d+\b', text)
        for machine in machine_numbers:
            self.known_entities["machines"].add(machine)
        
        # Extract machine types
        machine_types = [
            "Cutter", "Loom", "Printer", "Weaver", "Dryer", 
            "Mixer", "Conveyor", "Packager", "Labeler", "Scanner"
        ]
        for m_type in machine_types:
            if m_type.lower() in text.lower():
                self.known_entities["machine_types"].add(m_type)
        
        # Extract common issue types
        issue_types = [
            "breakdown", "malfunction", "error", "jam", "failure",
            "overheating", "leak", "calibration", "alignment", "wear"
        ]
        for issue in issue_types:
            if issue.lower() in text.lower():
                self.known_entities["issues"].add(issue)
    
    def get_context(self) -> Dict[str, Any]:
        """
        Get the current context for an MCP message.
        
        Returns:
            A context dictionary
        """
        # Convert sets to lists for JSON serialization
        entities_dict = {
            k: list(v) for k, v in self.known_entities.items() if v
        }
        
        # Get summarized conversation history (limiting tokens)
        summarized_history = self._summarize_history()
        
        context = {
            "user_info": {
                "role": "maintenance_manager"
            },
            "conversation_history": summarized_history,
            "known_entities": entities_dict,
            "recent_tools": self.recent_tools_used
        }
        
        return context
    
    def _summarize_history(self) -> List[Dict[str, str]]:
        """
        Summarize conversation history to fit within token limits.
        
        Returns:
            Summarized conversation history
        """
        # This is a simplified version - a proper implementation would 
        # estimate tokens and truncate/summarize as needed
        
        # For now, just return the recent history
        return self.conversation_history[-min(5, len(self.conversation_history)):]
    
    def add_entity(self, entity_type: str, entity_value: str) -> None:
        """
        Explicitly add an entity to the known entities.
        
        Args:
            entity_type: Type of entity ('mechanics', 'machines', etc.)
            entity_value: The entity value to add
        """
        if entity_type in self.known_entities:
            self.known_entities[entity_type].add(entity_value)
            logger.debug(f"Added entity: {entity_type}:{entity_value}")
        else:
            logger.warning(f"Unknown entity type: {entity_type}")
    
    def reset(self) -> None:
        """Reset the context manager to its initial state."""
        self.conversation_history = []
        self.known_entities = {
            "mechanics": set(),
            "machines": set(),
            "machine_types": set(),
            "issues": set()
        }
        self.recent_tools_used = []
        logger.info("Context manager reset")
    
    def get_relevant_entities(self, query: str) -> Dict[str, List[str]]:
        """
        Get entities that are relevant to the current query.
        
        Args:
            query: The user's query
            
        Returns:
            Dictionary of relevant entities
        """
        # Convert query to lowercase for matching
        query_lower = query.lower()
        
        # Initialize relevant entities
        relevant = {}
        
        # Check which entity types are mentioned in the query
        for entity_type, entities in self.known_entities.items():
            # Entity type mentioned (e.g., "mechanics" or "machines")
            if entity_type.lower() in query_lower:
                relevant[entity_type] = list(entities)
                continue
                
            # Check for specific entity mentions
            relevant_entities = []
            for entity in entities:
                if entity.lower() in query_lower:
                    relevant_entities.append(entity)
            
            if relevant_entities:
                relevant[entity_type] = relevant_entities
        
        return relevant