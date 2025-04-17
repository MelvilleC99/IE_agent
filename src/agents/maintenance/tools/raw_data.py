import json
from typing import Optional
import os

# Define the path to your context file
CONTEXT_FILE_PATH = os.path.join(
    os.path.dirname(__file__),
    "..", "..", "agents", "maintenance", "prompts", "raw_data_context.txt"
)

def load_raw_data_context() -> str:
    try:
        with open(CONTEXT_FILE_PATH, "r") as f:
            return f.read()
    except Exception as e:
        return f"Error loading raw data context: {str(e)}"

# File path where the maintenance data JSON is stored
MAINTENANCE_DATA_PATH = "/Users/melville/Documents/Industrial_Engineering_Agent/maintenance_data.json"

def get_raw_maintenance_data(query: Optional[str] = None) -> str:
    """
    Retrieve raw maintenance data from the JSON file.
    
    Args:
        query (Optional[str]): If 'sample', returns only the first 5 records.
    
    Returns:
        A JSON-formatted string of the maintenance data or an error message.
    """
    try:
        with open(MAINTENANCE_DATA_PATH, "r") as f:
            data = json.load(f)
        if query == "sample":
            data = data[:5]
        return json.dumps(data, indent=2)
    except Exception as e:
        return f"Error accessing raw maintenance data: {str(e)}"
