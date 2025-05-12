# /src/MCP/session_manager.py
import logging
import json
import os
from typing import Dict, Any, List, Optional
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("session_manager")

class SessionManager:
    """
    Simple session manager for persisting conversations.
    
    This class handles saving and loading conversation history
    to/from persistent storage (file or database).
    """
    
    def __init__(self, session_id: str, storage_type: str = "file", storage_path: Optional[str] = None):
        """
        Initialize the session manager.
        
        Args:
            session_id: Unique session identifier
            storage_type: Storage type ('file' or 'db')
            storage_path: Path to storage directory or database connection string
        """
        self.session_id = session_id
        self.storage_type = storage_type
        
        # Set default storage path if not provided
        if storage_path is None:
            if storage_type == "file":
                storage_path = os.path.join(os.getcwd(), "sessions")
            else:
                # Default database connection (could be a config value)
                storage_path = "sqlite:///sessions.db"
                
        self.storage_path = storage_path
        
        # Create storage directory if needed
        if storage_type == "file" and not os.path.exists(storage_path):
            os.makedirs(storage_path)
            
        logger.info(f"SessionManager initialized for session {session_id} using {storage_type} storage")
    
    def get_conversation_history(self) -> Optional[List[Dict[str, Any]]]:
        """
        Get conversation history from storage.
        
        Returns:
            Conversation history or None if not found
        """
        try:
            if self.storage_type == "file":
                file_path = os.path.join(self.storage_path, f"{self.session_id}.json")
                
                if not os.path.exists(file_path):
                    return None
                    
                with open(file_path, 'r') as f:
                    session_data = json.load(f)
                    
                return session_data.get("conversation_history", [])
            else:
                # Database implementation would go here
                logger.warning("Database storage not implemented yet")
                return None
        except Exception as e:
            logger.error(f"Error loading conversation history: {e}")
            return None
    
    def save_conversation_history(self, conversation_history: List[Dict[str, Any]]) -> bool:
        """
        Save conversation history to storage.
        
        Args:
            conversation_history: Conversation history to save
            
        Returns:
            Success flag
        """
        try:
            if self.storage_type == "file":
                file_path = os.path.join(self.storage_path, f"{self.session_id}.json")
                
                session_data = {
                    "session_id": self.session_id,
                    "last_updated": datetime.now().isoformat(),
                    "conversation_history": conversation_history
                }
                
                with open(file_path, 'w') as f:
                    json.dump(session_data, f, indent=2)
                    
                logger.debug(f"Saved {len(conversation_history)} messages to {file_path}")
                return True
            else:
                # Database implementation would go here
                logger.warning("Database storage not implemented yet")
                return False
        except Exception as e:
            logger.error(f"Error saving conversation history: {e}")
            return False
    
    def clear_conversation_history(self) -> bool:
        """
        Clear conversation history from storage.
        
        Returns:
            Success flag
        """
        try:
            if self.storage_type == "file":
                file_path = os.path.join(self.storage_path, f"{self.session_id}.json")
                
                if os.path.exists(file_path):
                    os.remove(file_path)
                    
                logger.info(f"Cleared conversation history for session {self.session_id}")
                return True
            else:
                # Database implementation would go here
                logger.warning("Database storage not implemented yet")
                return False
        except Exception as e:
            logger.error(f"Error clearing conversation history: {e}")
            return False