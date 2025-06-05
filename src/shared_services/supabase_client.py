# File: src/shared_services/supabase_client.py
import os
import logging
import time
from typing import Dict, Any, List, Optional, Union
from supabase.client import create_client

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("supabase_client")

class SupabaseClient:
    """Client for interacting with Supabase database."""
    
    def __init__(self):
        """Initialize the Supabase client using environment variables."""
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_KEY")
        
        if not self.supabase_url or not self.supabase_key:
            logger.error("Missing Supabase credentials in environment variables")
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in environment variables")
        
        try:
            self.client = create_client(self.supabase_url, self.supabase_key)
            logger.info("Supabase client initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing Supabase client: {e}")
            raise

    @property
    def table(self):
        """Expose the underlying client's table method for complex operations."""
        return self.client.table

    def query_table(
        self,
        table_name: str,
        columns: str = "*",
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Query a Supabase table with optional filters and return the result list.
        Logs actual elapsed time for the database call.
        
        Args:
            table_name: Name of the table to query
            columns: Columns to select (default: "*")
            filters: Dictionary of filters where keys can be:
                    - Simple column name for equality filter
                    - "column.operator" for other operators (gte, lte, gt, lt, etc.)
            limit: Maximum number of records to return
        """
        start = time.time()
        try:
            q = self.client.table(table_name).select(columns)
            if filters:
                for col, val in filters.items():
                    if '.' in col:
                        # Handle operators like gte, lte, etc.
                        col_name, operator = col.split('.')
                        if operator == 'gte':
                            q = q.gte(col_name, val)
                        elif operator == 'lte':
                            q = q.lte(col_name, val)
                        elif operator == 'gt':
                            q = q.gt(col_name, val)
                        elif operator == 'lt':
                            q = q.lt(col_name, val)
                        elif operator == 'like':
                            q = q.like(col_name, val)
                        elif operator == 'ilike':
                            q = q.ilike(col_name, val)
                        elif operator == 'eq':
                            q = q.eq(col_name, val)
                        elif operator == 'neq':
                            q = q.neq(col_name, val)
                        elif operator == 'in':
                            q = q.in_(col_name, val)
                        else:
                            logger.warning(f"Unsupported operator {operator} for column {col_name}")
                    else:
                        # Simple equality filter
                        q = q.eq(col, val)
            q = q.limit(limit)
            response = q.execute()

            elapsed_ms = (time.time() - start) * 1000
            logger.info(f"DB query for {table_name} took {elapsed_ms:.1f} ms")
            logger.info(f"Successfully queried {table_name}: {len(response.data)} records retrieved")
            return response.data
        except Exception as e:
            logger.error(f"Error querying table {table_name}: {e}")
            raise

    def insert_data(self, table_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Insert a record into a Supabase table."""
        try:
            response = self.client.table(table_name).insert(data).execute()
            logger.info(f"Successfully inserted record into {table_name}")
            return response.data[0] if response.data else {}
        except Exception as e:
            logger.error(f"Error inserting into {table_name}: {e}")
            raise

    def upsert_data(self, table_name: str, data: Dict[str, Any], on_conflict: Optional[str] = None) -> List[Dict[str, Any]]:
        """Upsert (insert or update) a record into a Supabase table. Only single dict supported."""
        if not isinstance(table_name, str) or not table_name:
            raise ValueError("table_name must be a non-empty string")
        if not isinstance(data, dict):
            raise ValueError("data must be a dictionary for upsert (Supabase Python client limitation)")
        try:
            query = self.client.table(table_name).upsert(data)
            # The following is commented out because on_conflict may not be supported by the client:
            # if on_conflict:
            #     query = query.on_conflict(on_conflict)
            response = query.execute()
            logger.info(f"Successfully upserted 1 record into {table_name}")
            return response.data if response.data else []
        except Exception as e:
            logger.error(f"Error upserting into {table_name}: {e}")
            raise

    def update_data(self, table_name: str, data: Dict[str, Any], match_column: str) -> Dict[str, Any]:
        """Update a record in a Supabase table based on a match column."""
        try:
            match_val = data.get(match_column)
            if not match_val:
                raise ValueError(f"Match column {match_column} not found in data")
            response = (
                self.client
                    .table(table_name)
                    .update(data)
                    .eq(match_column, match_val)
                    .execute()
            )
            logger.info(f"Successfully updated record in {table_name}")
            return response.data[0] if response.data else {}
        except Exception as e:
            logger.error(f"Error updating {table_name}: {e}")
            raise

    def get_schema_info(
        self,
        table_name: Optional[str] = None
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Retrieve schema info for a specific table or all tables via RPC."""
        try:
            if table_name:
                resp = self.client.rpc('get_table_schema', {'table_name': table_name}).execute()
                return resp.data[0] if resp.data else {}
            else:
                resp = self.client.rpc('get_all_tables_schema',{}).execute()
                return resp.data
        except Exception as e:
            logger.error(f"Error retrieving schema info: {e}")
            raise


# =============================================================================
# SHARED CONNECTION MANAGEMENT
# =============================================================================

# Shared instance for connection reuse across the application
_shared_instance = None

def get_shared_supabase_client():
    """
    Get a shared SupabaseClient instance.
    
    Creates the connection only once and reuses it across all parts of the application.
    This reduces connection overhead and improves performance.
    
    Returns:
        SupabaseClient: The shared database client instance
    """
    global _shared_instance
    if _shared_instance is None:
        logger.info("Creating shared Supabase connection...")
        _shared_instance = SupabaseClient()
        logger.info("✅ Shared Supabase connection established")
    return _shared_instance

def reset_shared_connection():
    """
    Reset the shared connection (useful for testing or reconnection).
    
    This will force the creation of a new connection the next time
    get_shared_supabase_client() is called.
    """
    global _shared_instance
    _shared_instance = None
    logger.info("Shared Supabase connection reset")
