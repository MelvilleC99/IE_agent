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
        """
        start = time.time()
        try:
            q = self.client.table(table_name).select(columns)
            if filters:
                for col, val in filters.items():
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
