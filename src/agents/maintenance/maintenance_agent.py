import os
import sys
from dotenv import load_dotenv
from langchain.agents import initialize_agent, AgentType, Tool
from langchain.llms import OpenAI

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(project_root)

# Import the chat memory class from its location
from src.agents.maintenance.memory.chat_memory import MaintenanceAgentMemory

# --- Load Environment Variables ---
env_path = os.path.join(project_root, '.env.local')
print(f"Loading .env.local from: {env_path}")
print(f"File exists: {os.path.exists(env_path)}")
load_dotenv(dotenv_path=env_path)

api_key = os.getenv("DEEPSEEK_API_KEY")
if api_key:
    print(f"API key loaded: {api_key[:5]}...")
else:
    print("API key not found in environment variables")

# --- Load the System Prompt from External File ---
prompt_path = os.path.join(project_root, "src", "agents", "maintenance", "prompts", "system_prompt.txt")
try:
    with open(prompt_path, 'r') as f:
        SYSTEM_PROMPT = f.read()
    print("System prompt loaded successfully.")
except Exception as e:
    print(f"Error loading system prompt: {e}")
    SYSTEM_PROMPT = "You are a Maintenance Performance Analyst AI."

# --- Initialize the Chat Model ---
llm = OpenAI(
    model="deepseek-chat",
    temperature=0.7,
    api_key=api_key,
    base_url="https://api.deepseek.com/v1"
)

# --- Import the Raw Maintenance Data Tool ---
from src.agents.maintenance.tools.raw_data import get_raw_maintenance_data

# --- Define the Tools ---
# Here we include the raw maintenance data tool with its description
tools = [
    Tool(
        name="RawMaintenanceData",
        func=get_raw_maintenance_data,
        description="Access the raw maintenance incident records. Use 'sample' to retrieve a small sample (first 5 records)."
    )
]

# --- Initialize the Memory ---
maintenance_memory = MaintenanceAgentMemory()

# --- Initialize the Agent with Memory, Tools, and the System Prompt ---
agent = initialize_agent(
    tools,
    llm,
    agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
    verbose=True,
    handle_parsing_errors=True,
    agent_kwargs={"prefix": SYSTEM_PROMPT},
    memory=maintenance_memory.conversation_memory  # Using the conversation_memory attribute
)

# --- Interactive Chat Loop ---
def run_interactive_agent():
    print("\n=== Maintenance Performance Analyst Chat ===")
    print("Ask questions about maintenance incidents, or type 'exit' to quit.")
    
    while True:
        query = input("\nYou: ")
        if query.lower() in ["exit", "quit", "bye"]:
            print("Goodbye!")
            break
        
        try:
            # The agent automatically uses the tool when necessary
            result = agent.run(query)
            print("\nAgent: " + result)
        except Exception as e:
            print(f"\nError: {str(e)}")
            print("Let's try another question.")

if __name__ == "__main__":
    run_interactive_agent()
