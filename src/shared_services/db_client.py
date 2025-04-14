import sys
import os

# Add the project root to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

print(f"Project root: {project_root}")
print(f"Python path: {sys.path}")

from supabase import create_client, Client
from config.settings import SUPABASE_URL, SUPABASE_KEY

print(f"Imported SUPABASE_URL: {SUPABASE_URL}")
print(f"Imported SUPABASE_KEY: {SUPABASE_KEY}")

# Validate environment variables
if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Supabase credentials are not properly configured. Please check your .env.local file.")

# Initialize Supabase client
supabase: Client = create_client(str(SUPABASE_URL), str(SUPABASE_KEY))

def get_connection():
    """Get a connection to the Supabase database"""
    return supabase

def release_connection(conn):
    """Release the connection (no-op for Supabase as it's stateless)"""
    pass 