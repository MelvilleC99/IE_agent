# src/api/main.py
import os
import sys
from fastapi import FastAPI, Body
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Any
import logging

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, project_root)

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("api")

# Create FastAPI app
app = FastAPI(
    title="Industrial Engineering Agent API",
    description="API for maintenance scheduling and analytics",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Make sure src is in the path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(current_dir, '..'))
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

@app.get("/")
async def root():
    return {"message": "Welcome to the Industrial Engineering Agent API"}

@app.post("/api/agent/chat")
async def chat_endpoint(payload: Dict[str, Any] = Body(...)):
    query = payload.get("query", "")
    logger.info(f"Received query: {query}")
    
    try:
        # Import the chat route
        from api.routes.chat import chat
        
        # Process the query using the chat route
        result = chat(payload)
        logger.info("Query processed successfully")
        
        return result
    except Exception as e:
        logger.error(f"Error in chat endpoint: {e}", exc_info=True)
        return {"answer": f"Error processing your request: {str(e)}"}

# Import scheduled maintenance tool
from agents.maintenance.tools.scheduled_maintenance_tool import scheduled_maintenance_tool

@app.post("/api/agent/scheduled_maintenance")
async def maintenance_endpoint(payload: Dict[str, Any] = Body(...)):
    action = payload.get("action", "run")
    start_date = payload.get("start_date")
    end_date = payload.get("end_date")
    mode = payload.get("mode", "interactive")
    use_database = payload.get("use_database", True)
    
    logger.info(f"Running maintenance workflow: action={action}")
    
    try:
        # Run the maintenance workflow
        summary = scheduled_maintenance_tool(
            action=action,
            start_date=start_date,
            end_date=end_date,
            mode=mode,
            use_database=use_database
        )
        return {"result": summary}
    except Exception as e:
        logger.error(f"Error in maintenance endpoint: {e}", exc_info=True)
        return {"result": f"Error running maintenance workflow: {str(e)}"}

# Add a diagnostic route to list all available endpoints
@app.get("/routes")
async def get_routes():
    routes = []
    for route in app.routes:
        routes.append({
            "path": getattr(route, "path", "unknown"),
            "methods": getattr(route, "methods", ["unknown"]),
            "name": getattr(route, "name", "unnamed")
        })
    return {"routes": routes}

# Run the app directly if this file is executed
if __name__ == "__main__":
    import uvicorn
    logger.info("Starting Industrial Engineering Agent API...")
    uvicorn.run(app, host="127.0.0.1", port=8000)