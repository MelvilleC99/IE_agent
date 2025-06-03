# /Users/melville/Documents/Industrial_Engineering_Agent/src/MCP/__init__.py

# Core components that should always be available
from .tool_registry import MCPToolRegistry
from .response_formatter import MCPResponseFormatter
from .context_manager import ContextManager
from .query_handler import QueryHandler
from .query_manager import QueryManager

# Optional components that may have additional dependencies
try:
    from .two_tier_orchestrator import TwoTierOrchestrator
    ORCHESTRATOR_AVAILABLE = True
except ImportError as e:
    print(f"Warning: TwoTierOrchestrator not available due to missing dependencies: {e}")
    TwoTierOrchestrator = None
    ORCHESTRATOR_AVAILABLE = False

try:
    from .session_manager import SessionManager
    SESSION_MANAGER_AVAILABLE = True
except ImportError as e:
    print(f"Warning: SessionManager not available due to missing dependencies: {e}")
    SessionManager = None
    SESSION_MANAGER_AVAILABLE = False

__all__ = [
    'MCPToolRegistry',
    'MCPResponseFormatter', 
    'ContextManager',
    'QueryHandler',
    'QueryManager'
]

# Add optional components to __all__ if they're available
if ORCHESTRATOR_AVAILABLE:
    __all__.append('TwoTierOrchestrator')
    
if SESSION_MANAGER_AVAILABLE:
    __all__.append('SessionManager')