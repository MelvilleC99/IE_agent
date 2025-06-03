# /src/MCP/session_manager.py
import logging
import json
import os
import time
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("session_manager")

class SessionManager:
    """
    Enhanced session manager for persisting conversations and tracking session lifecycle.
    
    This class handles saving and loading conversation history
    to/from persistent storage and manages session timeouts.
    """
    
    def __init__(self, session_id: str, storage_type: str = "file", storage_path: Optional[str] = None, timeout_minutes: int = 30):
        """
        Initialize the session manager.
        
        Args:
            session_id: Unique session identifier
            storage_type: Storage type ('file' or 'db')
            storage_path: Path to storage directory or database connection string
            timeout_minutes: Session timeout in minutes (default: 30)
        """
        self.session_id = session_id
        self.storage_type = storage_type
        self.timeout_minutes = timeout_minutes
        self.last_activity = time.time()
        self.session_started_at = datetime.now()
        
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
            
        logger.info(f"SessionManager initialized for session {session_id} using {storage_type} storage (timeout: {timeout_minutes}min)")
    
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
    def update_activity(self):
        """Update the last activity timestamp."""
        self.last_activity = time.time()
    
    def is_session_expired(self) -> bool:
        """Check if the session has expired due to inactivity."""
        current_time = time.time()
        elapsed_minutes = (current_time - self.last_activity) / 60
        return elapsed_minutes > self.timeout_minutes
    
    def get_session_duration(self) -> Dict[str, Any]:
        """Get session duration information."""
        current_time = datetime.now()
        duration = current_time - self.session_started_at
        
        return {
            "started_at": self.session_started_at.isoformat(),
            "current_time": current_time.isoformat(),
            "duration_seconds": int(duration.total_seconds()),
            "duration_minutes": round(duration.total_seconds() / 60, 1),
            "last_activity": datetime.fromtimestamp(self.last_activity).isoformat(),
            "minutes_since_activity": round((time.time() - self.last_activity) / 60, 1),
            "is_expired": self.is_session_expired()
        }
    
    def get_session_info(self) -> Dict[str, Any]:
        """Get comprehensive session information."""
        try:
            conversation_history = self.get_conversation_history() or []
            duration_info = self.get_session_duration()
            
            return {
                "session_id": self.session_id,
                "storage_type": self.storage_type,
                "storage_path": self.storage_path,
                "timeout_minutes": self.timeout_minutes,
                "message_count": len(conversation_history),
                "session_duration": duration_info,
                "status": "expired" if self.is_session_expired() else "active"
            }
        except Exception as e:
            logger.error(f"Error getting session info: {e}")
            return {
                "session_id": self.session_id,
                "error": str(e)
            }
