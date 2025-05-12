# File: src/agents/maintenance/tools/supabase_tool.py
import os
import sys
import json
import logging
from typing import Dict, Any, List, TYPE_CHECKING
from cachetools import TTLCache, cached

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("supabase_tool")

# Ensure project root on sys.path
current_file = os.path.abspath(__file__)
project_root = os.path.abspath(os.path.join(current_file, "../../../../../"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import client
from src.shared_services.supabase_client import SupabaseClient

# Import call_llm for type checking only
if TYPE_CHECKING:
    from MCP.agents.deepseek_agent import call_llm  # type: ignore
else:
    def call_llm(prompt: str) -> str:
        raise RuntimeError("LLM client function call_llm() not found. Please implement in maintenance_agent.py.")

# Initialize single client
try:
    supabase_instance = SupabaseClient()
    logger.info("Supabase client initialized")
except Exception as e:
    logger.error(f"Failed to initialize Supabase client: {e}")
    supabase_instance = None

# Helper getter
def _get_supabase() -> SupabaseClient:
    if supabase_instance is None:
        logger.error("Supabase client not initialized")
        raise RuntimeError("Supabase client not initialized")
    return supabase_instance

# --- Cache for open tasks ---
open_tasks_cache = TTLCache(maxsize=100, ttl=30)

@cached(cache=open_tasks_cache)
def get_open_tasks_cached() -> List[Dict[str, Any]]:
    client = _get_supabase()
    return client.query_table(
        table_name="tasks",
        columns="*",
        filters={"status": "open"},
        limit=100
    )


def fetch_open_scheduled_maintenance() -> List[Dict[str, Any]]:
    client = _get_supabase()
    return client.query_table(
        table_name="scheduled_maintenance",
        columns="*",
        filters={"status": "open"},
        limit=100
    )


def summarize_tasks(raw_rows: List[Dict[str, Any]]) -> str:
    prompt = (
        "Here are maintenance records in JSON:\n" +
        json.dumps(raw_rows, indent=2) +
        "\n\nPlease produce a concise, bulleted summary."
    )
    return call_llm(prompt)


def query_database(query_params: str) -> str:
    client = _get_supabase()
    try:
        parts = query_params.split(';')
        table_cols = parts[0].split(':', 1)
        table_name = table_cols[0].strip()
        columns = table_cols[1].strip() if len(table_cols) > 1 else "*"
        filters: Dict[str, Any] = {}
        # Strip quotes from filter values
        if len(parts) > 1 and parts[1]:
            for f in parts[1].split(','):
                if '=' in f:
                    k, v = f.split('=', 1)
                    clean = v.strip().strip('"').strip("'")
                    filters[k.strip()] = clean
        limit = 100
        if len(parts) > 2 and parts[2].startswith("limit="):
            try:
                limit = int(parts[2].split('=', 1)[1])
            except ValueError:
                pass
        rows = client.query_table(table_name, columns, filters, limit)
        return json.dumps(rows, indent=2)
    except Exception as e:
        logger.error(f"Error querying database: {e}")
        return f"Error querying database: {str(e)}"


def get_schema_info(query: str) -> str:
    client = _get_supabase()
    try:
        if not query or query.lower() == "all":
            info = client.get_schema_info()
            return json.dumps(info, indent=2)
        try:
            info = client.get_schema_info(table_name=query)
            if info:
                return json.dumps(info, indent=2)
        except Exception:
            pass
        from src.agents.maintenance.knowledge.schema_search import search_schema_context
        results = search_schema_context(query, n_results=3)
        return "\n\n".join(results)
    except Exception as e:
        logger.error(f"Error getting schema info: {e}")
        return f"Error getting schema info: {str(e)}"


def insert_or_update_data(operation_str: str) -> str:
    client = _get_supabase()
    try:
        parts = operation_str.split('|')
        if len(parts) < 3:
            return "Error: Invalid format. Use 'operation|table|json|match_column'"
        op = parts[0].lower().strip()
        table_name = parts[1].strip()
        data_str = parts[2].strip()
        try:
            data = json.loads(data_str)
        except json.JSONDecodeError:
            return "Error: Invalid JSON data"
        if op == "insert":
            res = client.insert_data(table_name, data)
            return f"Inserted: {json.dumps(res, indent=2)}"
        elif op == "update":
            if len(parts) < 4:
                return "Error: match_column required for update"
            match_col = parts[3].strip()
            res = client.update_data(table_name, data, match_col)
            return f"Updated: {json.dumps(res, indent=2)}"
        else:
            return f"Error: Unknown operation '{op}'"
    except Exception as e:
        logger.error(f"Error in data operation: {e}")
        return f"Error in data operation: {str(e)}"
