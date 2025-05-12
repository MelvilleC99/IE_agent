import os
import sys

# Add the project root directory to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Now we can import and run the tests
from test_context_session import *

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