import os
import json
from typing import Dict, List, Any, Optional
from datetime import datetime
import uuid

from langchain_community.embeddings.openai import OpenAIEmbeddings
from langchain_community.vectorstores.chroma import Chroma

class MaintenanceKnowledgeBase:
    """
    Knowledge base for storing important maintenance information.
    Uses vector embeddings for semantic retrieval.
    """
    
    def __init__(
        self,
        persist_directory: Optional[str] = None,
        collection_name: str = "maintenance_knowledge"
    ):
        """
        Initialize the knowledge base.
        
        Args:
            persist_directory: Directory to store the vector database
            collection_name: Name of the collection in the vector store
        """
        # Set up persist directory
        if not persist_directory:
            persist_directory = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "vector_stores",
                collection_name
            )
        
        self.persist_directory = persist_directory
        self.collection_name = collection_name
        
        # Ensure directory exists
        os.makedirs(self.persist_directory, exist_ok=True)
        
        # Initialize the knowledge base
        self._initialize_knowledge_base()
    
    def _initialize_knowledge_base(self):
        """Initialize the vector store and embeddings."""
        try:
            # Get API key from environment
            api_key = os.getenv("OPENAI_API_KEY")
            if not api_key:
                raise ValueError("OpenAI API key not found in environment variables")
            
            # Initialize embeddings
            self.embeddings = OpenAIEmbeddings(openai_api_key=api_key)
            
            # Initialize vector store
            self.vector_store = Chroma(
                persist_directory=self.persist_directory,
                embedding_function=self.embeddings,
                collection_name=self.collection_name
            )
            
            print(f"Knowledge base initialized at {self.persist_directory}")
        
        except Exception as e:
            print(f"Error initializing knowledge base: {e}")
            raise
    
    def store_finding(self, finding: Dict[str, Any]) -> str:
        """
        Store a maintenance finding in the knowledge base.
        
        Args:
            finding: A dictionary containing the finding details
            
        Returns:
            ID of the stored finding
        """
        # Extract key information
        finding_id = finding.get("finding_id", str(uuid.uuid4()))
        finding_summary = finding.get("finding_summary", "")
        finding_details = finding.get("finding_details", {})
        
        # Create text for vector embedding
        text = f"Finding: {finding_summary}\n\n"
        
        # Add detailed information
        if finding_details:
            mechanic_id = finding_details.get("mechanic_id", "Unknown")
            metric = finding_details.get("metric", "Unknown")
            value = finding_details.get("value", "Unknown")
            mean_value = finding_details.get("mean_value", "Unknown")
            z_score = finding_details.get("z_score", "Unknown")
            
            text += f"Mechanic: {mechanic_id}\n"
            text += f"Metric: {metric}\n"
            text += f"Value: {value}\n"
            text += f"Mean Value: {mean_value}\n"
            text += f"Z-Score: {z_score}\n"
            
            # Add machine info if available
            if "machine_type" in finding_details:
                text += f"Machine Type: {finding_details['machine_type']}\n"
            
            # Add reason info if available
            if "reason" in finding_details:
                text += f"Reason: {finding_details['reason']}\n"
        
        # Create metadata
        metadata = {
            "finding_id": finding_id,
            "type": "finding",
            "mechanic_id": finding_details.get("mechanic_id", "Unknown"),
            "employee_number": finding_details.get("employee_number", "Unknown"),
            "metric": finding_details.get("metric", "Unknown"),
            "timestamp": datetime.now().isoformat(),
            "z_score": finding_details.get("z_score", 0),
            "machine_type": finding_details.get("machine_type", ""),
            "reason": finding_details.get("reason", "")
        }
        
        # Store in vector database
        self.vector_store.add_texts(
            texts=[text],
            metadatas=[metadata],
            ids=[finding_id]
        )
        
        # Persist changes
        if hasattr(self.vector_store, "persist"):
            self.vector_store.persist()
        
        return finding_id
    
    def store_training_record(
        self,
        mechanic_id: str,
        machine_type: str,
        training_date: str,
        description: str,
        instructor: Optional[str] = None
    ) -> str:
        """
        Store a training record in the knowledge base.
        
        Args:
            mechanic_id: ID of the mechanic
            machine_type: Type of machine trained on
            training_date: Date of training
            description: Description of the training
            instructor: Name of instructor
            
        Returns:
            ID of the stored record
        """
        record_id = str(uuid.uuid4())
        
        # Create text for vector embedding
        text = f"Training Record: {mechanic_id} received training on {machine_type} machines\n\n"
        text += f"Date: {training_date}\n"
        text += f"Description: {description}\n"
        
        if instructor:
            text += f"Instructor: {instructor}\n"
        
        # Create metadata
        metadata = {
            "record_id": record_id,
            "type": "training",
            "mechanic_id": mechanic_id,
            "machine_type": machine_type,
            "training_date": training_date,
            "timestamp": datetime.now().isoformat()
        }
        
        # Store in vector database
        self.vector_store.add_texts(
            texts=[text],
            metadatas=[metadata],
            ids=[record_id]
        )
        
        # Persist changes
        if hasattr(self.vector_store, "persist"):
            self.vector_store.persist()
        
        return record_id
    
    def store_maintenance_note(
        self,
        mechanic_id: str,
        note: str,
        machine_type: Optional[str] = None,
        reason: Optional[str] = None
    ) -> str:
        """
        Store a maintenance note in the knowledge base.
        
        Args:
            mechanic_id: ID of the mechanic
            note: The note content
            machine_type: Optional machine type
            reason: Optional reason/issue
            
        Returns:
            ID of the stored note
        """
        note_id = str(uuid.uuid4())
        
        # Create text for vector embedding
        text = f"Maintenance Note about {mechanic_id}:\n\n{note}\n"
        
        if machine_type:
            text += f"Machine Type: {machine_type}\n"
        
        if reason:
            text += f"Issue: {reason}\n"
        
        # Create metadata
        metadata = {
            "note_id": note_id,
            "type": "note",
            "mechanic_id": mechanic_id,
            "machine_type": machine_type or "",
            "reason": reason or "",
            "timestamp": datetime.now().isoformat()
        }
        
        # Store in vector database
        self.vector_store.add_texts(
            texts=[text],
            metadatas=[metadata],
            ids=[note_id]
        )
        
        # Persist changes
        if hasattr(self.vector_store, "persist"):
            self.vector_store.persist()
        
        return note_id
    
    def search(
        self, 
        query: str,
        filter_metadata: Optional[Dict[str, Any]] = None,
        k: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Search the knowledge base for relevant information.
        
        Args:
            query: The search query
            filter_metadata: Optional metadata filter
            k: Number of results to return
            
        Returns:
            List of relevant documents
        """
        try:
            results = self.vector_store.similarity_search_with_relevance_scores(
                query=query,
                k=k,
                filter=filter_metadata
            )
            
            formatted_results = []
            for doc, score in results:
                formatted_results.append({
                    "content": doc.page_content,
                    "metadata": doc.metadata,
                    "relevance_score": score
                })
            
            return formatted_results
        
        except Exception as e:
            print(f"Error searching knowledge base: {e}")
            return []
    
    def get_mechanic_knowledge(self, mechanic_id: str, k: int = 10) -> List[Dict[str, Any]]:
        """
        Get all knowledge about a specific mechanic.
        
        Args:
            mechanic_id: ID of the mechanic
            k: Maximum number of results to return
            
        Returns:
            List of documents about the mechanic
        """
        filter_metadata = {"mechanic_id": mechanic_id}
        
        # A generic query about the mechanic
        query = f"Information about {mechanic_id} performance and training"
        
        return self.search(query, filter_metadata, k)
    
    def get_machine_knowledge(self, machine_type: str, k: int = 10) -> List[Dict[str, Any]]:
        """
        Get all knowledge about a specific machine type.
        
        Args:
            machine_type: Type of machine
            k: Maximum number of results to return
            
        Returns:
            List of documents about the machine type
        """
        filter_metadata = {"machine_type": machine_type}
        
        # A generic query about the machine type
        query = f"Information about {machine_type} machines performance and issues"
        
        return self.search(query, filter_metadata, k)


# Guidelines for using the Maintenance Knowledge Base

"""
WHAT TO SAVE IN THE KNOWLEDGE BASE:

1. Performance Findings:
   - Statistical outliers identified in mechanic performance
   - Significant trends in repair or response times
   - Machine-specific performance issues
   - Reason-specific performance issues

2. Training Records:
   - When mechanics receive training on specific machines
   - Certification information
   - Skills development notes

3. Maintenance Notes:
   - Important observations about mechanics' performance
   - Feedback from supervisors
   - Recurring issues with specific mechanics or machines
   - Special circumstances affecting performance

4. Machine Information:
   - Known issues with specific machine types
   - Common failure reasons for different machines
   - Expected repair times for different machine/reason combinations

HOW TO USE THE KNOWLEDGE BASE:

1. When analyzing new findings:
   - Search for previous issues with the same mechanic
   - Look for patterns across machine types or reasons
   - Check if training has been provided for problem areas

2. When making recommendations:
   - Reference relevant previous findings
   - Consider past interventions and their effectiveness
   - Take into account training history

3. For trend analysis:
   - Compare current findings with historical data
   - Look for improvements after interventions
   - Identify recurring issues

4. For decision support:
   - Provide context from historical data
   - Highlight patterns that may not be obvious from current data
   - Support recommendations with evidence from the knowledge base
"""


# Example usage
if __name__ == "__main__":
    # Create a knowledge base
    kb = MaintenanceKnowledgeBase()
    
    # Store a finding
    finding = {
        "finding_id": "f123",
        "finding_summary": "Duncan J has significantly higher repair times on Coverseam machines",
        "finding_details": {
            "mechanic_id": "Duncan J",
            "employee_number": "003",
            "metric": "repair_time_by_machine",
            "value": 9.9,
            "mean_value": 4.9,
            "z_score": 1.65,
            "machine_type": "Coverseam",
            "context": "Machine Type Coverseam Repair Time Analysis"
        }
    }
    
    kb.store_finding(finding)
    
    # Store a training record
    kb.store_training_record(
        mechanic_id="Duncan J",
        machine_type="Coverseam",
        training_date="2023-04-15",
        description="Basic maintenance and troubleshooting training",
        instructor="Sarah Thompson"
    )
    
    # Store a maintenance note
    kb.store_maintenance_note(
        mechanic_id="Duncan J",
        note="Duncan mentioned difficulty with the tension adjustment on Coverseam machines. May need additional hands-on training.",
        machine_type="Coverseam",
        reason="Tension"
    )
    
    # Search the knowledge base
    results = kb.search("Duncan's issues with Coverseam machines")
    
    print("Search Results:")
    for i, result in enumerate(results):
        print(f"\n--- Result {i+1} (Score: {result['relevance_score']:.2f}) ---")
        print(result['content'])
    
    # Get all knowledge about a mechanic
    print("\nAll Knowledge about Duncan J:")
    mechanic_info = kb.get_mechanic_knowledge("Duncan J")
    for i, info in enumerate(mechanic_info):
        print(f"\n--- Item {i+1} ---")
        print(info['content'])