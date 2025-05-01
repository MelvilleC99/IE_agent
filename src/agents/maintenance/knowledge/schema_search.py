import os
import logging
import chromadb
from sentence_transformers import SentenceTransformer
from typing import List, Optional, Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("schema_search")

# Get the directory of the current script and ChromaDB path
current_dir = os.path.dirname(os.path.abspath(__file__))
chroma_path = os.path.join(current_dir, "chroma_db")

# Initialize ChromaDB client
try:
    client = chromadb.PersistentClient(path=chroma_path)
    # Try loading the schema collection
    try:
        collection = client.get_collection(name="schema_knowledge")
    except Exception as e:
        logger.error(f"ChromaDB collection init failed: {e}")
        collection = None
except Exception as e:
    logger.error(f"Failed to initialize ChromaDB client: {e}")
    client = None
    collection = None

# Load the embedding model
try:
    model = SentenceTransformer("all-MiniLM-L6-v2")
except Exception as e:
    logger.error(f"Failed to load embedding model: {e}")
    model = None


def search_schema_context(query: str, n_results: int = 3) -> List[str]:
    """
    Search the vector database for schema context related to a query.

    Args:
        query (str): Natural language query
        n_results (int): Number of best matches to return

    Returns:
        List[str]: Matching schema descriptions with table name
    """
    # Guard if collection or model isn't ready
    if collection is None or model is None:
        return ["Schema vector DB not initialized or model unavailable."]

    try:
        embedding = model.encode([query]).tolist()
        results = collection.query(
            query_embeddings=embedding,
            n_results=n_results
        )

        if not isinstance(results, dict):
            return ["No matching schema information found."]

        documents = results.get("documents", []) or []
        metadatas = results.get("metadatas", []) or []

        if not documents or not metadatas or not documents[0] or not metadatas[0]:
            return ["No matching schema information found."]

        matches = []
        for doc, meta in zip(documents[0], metadatas[0]):
            if isinstance(doc, str) and isinstance(meta, dict) and "table_name" in meta:
                matches.append(f"🗂️ Table: {meta['table_name']}\n📄 Description:\n{doc}")

        return matches if matches else ["No matching schema information found."]

    except Exception as e:
        logger.error(f"Error searching schema: {e}")
        return [f"Error searching schema: {str(e)}"]
