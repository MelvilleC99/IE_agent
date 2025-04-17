from typing import List, Dict, Any
from langchain.memory import ConversationBufferMemory
from langchain.schema import BaseChatMessageHistory
from langchain.schema.messages import BaseMessage, HumanMessage, AIMessage

class MaintenanceAgentMemory:
    """Memory class for the Maintenance Agent that stores conversation history and relevant data."""
    
    def __init__(self):
        """Initialize the memory with conversation buffer and data storage."""
        self.conversation_memory = ConversationBufferMemory(
            memory_key="chat_history",
            return_messages=True
        )
        self.data_memory: Dict[str, Any] = {}
    
    def add_message(self, message: str, is_user: bool = True) -> None:
        """Add a message to the conversation history."""
        if is_user:
            self.conversation_memory.chat_memory.add_user_message(message)
        else:
            self.conversation_memory.chat_memory.add_ai_message(message)
    
    def get_messages(self) -> List[BaseMessage]:
        """Get all messages from the conversation history."""
        return self.conversation_memory.chat_memory.messages
    
    def clear(self) -> None:
        """Clear both conversation and data memory."""
        self.conversation_memory.clear()
        self.data_memory.clear()
    
    def store_data(self, key: str, value: Any) -> None:
        """Store additional data in memory."""
        self.data_memory[key] = value
    
    def retrieve_data(self, key: str) -> Any:
        """Retrieve stored data from memory."""
        return self.data_memory.get(key)
    
    def get_memory_variables(self) -> Dict[str, Any]:
        """Get all memory variables including chat history and stored data."""
        memory_variables = self.conversation_memory.load_memory_variables({})
        memory_variables.update(self.data_memory)
        return memory_variables 