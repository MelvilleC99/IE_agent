# test_context_session.py
import logging
import json
import time
import inspect
from src.MCP.session_manager import MCPSessionManager
from src.MCP.context_manager import MCPContextManager

# Set up detailed logging
logging.basicConfig(
    level=logging.DEBUG, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("context_session_test.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("context_session_test")

# Debug the MCPContextManager class
logger.debug(f"MCPContextManager source: {inspect.getsourcefile(MCPContextManager)}")
logger.debug(f"MCPContextManager init params: {inspect.signature(MCPContextManager.__init__)}")

def test_session_creation():
    """Test session creation and persistence"""
    logger.info("=== Testing Session Creation ===")
    
    # Create a new session
    session = MCPSessionManager(session_id=f"test-{int(time.time())}")
    logger.info(f"Created session with ID: {session.session_id}")
    
    # Add some messages
    session.add_message("user", "Show me scheduled maintenance tasks")
    session.add_message("assistant", "Here are the maintenance tasks...")
    
    # Check if messages were stored
    logger.info(f"Session has {session.message_count} messages")
    
    # Check if session was persisted to database
    logger.info("Session should be in database now")
    
    return session

def test_context_with_session():
    """Test context manager working with session manager"""
    logger.info("=== Testing Context with Session ===")
    
    # Create session first
    session = test_session_creation()
    
    # Create context manager with session
    context = MCPContextManager(session)
    logger.info("Created context manager with session")
    
    # Add a message through context
    context.add_message("user", "Show me tasks for mechanic Patrick Jones")
    
    # Check if message count increased in session
    logger.info(f"Session message count after adding through context: {session.message_count}")
    
    # Get context data
    context_data = context.get_context()
    logger.info(f"Context data has {len(context_data.get('conversation_history', []))} messages")
    
    return context, session

def test_result_caching():
    """Test result caching in context manager"""
    logger.info("=== Testing Result Caching ===")
    
    # Get context from previous test
    context, session = test_context_with_session()
    
    # Simulate a query result
    sample_result = [
        {"id": 1, "machine_type": "Coverseam", "machine_id": "003", "mechanic_name": "Patrick Jones"},
        {"id": 2, "machine_type": "Button Sew", "machine_id": "006", "mechanic_name": "Duncan J"}
    ]
    
    # Add to context
    context.add_result("Show me all tasks", sample_result, "scheduled_maintenance")
    logger.info("Added query result to context")
    
    # Try to retrieve the result
    latest = context.get_latest_result()
    if latest:
        logger.info(f"Retrieved latest result with {len(latest.get('result', {}).get('data', []))} items")
    else:
        logger.error("No result retrieved from context")
    
    # Check if result is in session
    if hasattr(session, "result_cache") and session.result_cache:
        logger.info(f"Session has {len(session.result_cache)} cached results")
    else:
        logger.error("No results found in session cache")

def test_follow_up_detection():
    """Test follow-up query detection"""
    logger.info("=== Testing Follow-up Detection ===")
    
    # Set up context with existing results
    context, _ = test_context_with_session()
    
    # Add sample result
    sample_result = [
        {"id": 1, "machine_type": "Coverseam", "machine_id": "003", "mechanic_name": "Patrick Jones"},
        {"id": 2, "machine_type": "Button Sew", "machine_id": "006", "mechanic_name": "Duncan J"}
    ]
    context.add_result("Show me all tasks", sample_result, "scheduled_maintenance")
    
    # Check if a follow-up is detected
    follow_up_query = "Show me details for machine 003"
    
    # Try with the scheduled maintenance handler if available
    try:
        from src.MCP.query_handlers.scheduled_maintenance_handler import ScheduledMaintenanceHandler
        handler = ScheduledMaintenanceHandler()
        
        if hasattr(handler, "can_handle") and hasattr(handler, "_is_follow_up_query"):
            is_follow_up = handler._is_follow_up_query(follow_up_query, context.get_context())
            logger.info(f"Handler detected '{follow_up_query}' as follow-up? {is_follow_up}")
        else:
            logger.error("Handler doesn't have proper follow-up detection methods")
    except ImportError:
        logger.error("Couldn't import ScheduledMaintenanceHandler")

if __name__ == "__main__":
    logger.info("Starting context and session management tests")
    
    try:
        test_session_creation()
        test_context_with_session()
        test_result_caching()
        test_follow_up_detection()
        
        logger.info("All tests completed")
    except Exception as e:
        logger.error(f"Tests failed with error: {e}", exc_info=True)