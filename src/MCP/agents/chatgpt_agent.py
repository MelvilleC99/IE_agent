# /src/MCP/agents/chatgpt_agent.py
"""
ChatGPT Agent - Compatibility Proxy

This file maintains backward compatibility by importing the new modular ChatGPT agent.
All existing imports and usage patterns continue to work exactly the same.
"""

# Import the new modular agent
from .chatgpt.core_agent import ChatGPTAgent

# Export for backward compatibility
__all__ = ['ChatGPTAgent']
