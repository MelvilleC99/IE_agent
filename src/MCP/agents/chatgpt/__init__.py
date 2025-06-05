# ChatGPT Agent Package
"""
Modular ChatGPT Agent implementation.

This package provides a clean, organized structure for the ChatGPT agent
while maintaining the same external interface for backward compatibility.
"""

from .core_agent import ChatGPTAgent

# Export the main class for backward compatibility
__all__ = ['ChatGPTAgent']
