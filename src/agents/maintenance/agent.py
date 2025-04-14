from langchain.chat_models import ChatOpenAI
from langchain.schema import SystemMessage, HumanMessage
from config.settings import DEEPSEEK_API_KEY

# Define system prompt for tone and behavior
system_prompt = "You are a maintenance assistant. Keep answers practical, concise, and based on industrial engineering best practices."

# Set up the LLM (DeepSeek)
llm = ChatOpenAI(
    openai_api_key=DEEPSEEK_API_KEY,
    openai_api_base="https://api.deepseek.com/v1",
    model_name="deepseek-chat",
    temperature=0.3
)

def run_maintenance_agent(user_input: str) -> str:
    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=user_input)
    ]
    response = llm(messages)  # Returns AI message object
    return response.content