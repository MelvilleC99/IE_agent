import sys
import os
from sqlalchemy import create_engine, MetaData, text
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# Add the project root to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

print(f"Project root: {project_root}")
print(f"Python path: {sys.path}")

# Load database URL from environment variable
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL") or os.getenv("SUPABASE_SQLALCHEMY_URL")
if not SUPABASE_DB_URL:
    # Compose from Supabase credentials if needed
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    # You may need to construct the Postgres URL from your Supabase project settings
    # Example: 'postgresql+psycopg2://username:password@host:port/dbname'
    raise ValueError("SUPABASE_DB_URL or SUPABASE_SQLALCHEMY_URL must be set in environment variables.")

# Create SQLAlchemy engine
engine: Engine = create_engine(SUPABASE_DB_URL, pool_pre_ping=True)
SessionLocal = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
metadata = MetaData()


def get_engine() -> Engine:
    """Return the SQLAlchemy engine."""
    return engine


def get_session():
    """Return a new SQLAlchemy session."""
    return SessionLocal()


def reflect_schema():
    """Reflect and return the database schema metadata."""
    metadata.reflect(bind=engine)
    return metadata


def get_table_schema(table_name: str):
    """Return the SQLAlchemy Table object for a given table name."""
    metadata.reflect(bind=engine, only=[table_name])
    return metadata.tables.get(table_name)


def test_connection():
    """Test the database connection."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("Database connection successful.")
    except SQLAlchemyError as e:
        print(f"Database connection failed: {e}")
        raise 