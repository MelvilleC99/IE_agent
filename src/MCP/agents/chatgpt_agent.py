import os
import sys
import logging
import json
import time
from typing import Dict, Any, List, Optional, Tuple, Union
from datetime import datetime
from openai import OpenAI
from openai.types.chat import ChatCompletionMessageParam
from openai.types.chat.chat_completion import ChatCompletion

# Add project root to path
current_file = os.path.abspath(__file__)
project_root = os.path.abspath(os.path.join(current_file, "../../../.."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("chatgpt_agent")

# Import utilities
from src.MCP.agents.utils.date_utils import date_utils
from src.MCP.context_manager import ContextManager
from src.MCP.token_tracker import token_tracker
from src.MCP.response_formatter import MCPResponseFormatter

class ChatGPTAgent:
    """
    Agent that uses OpenAI's GPT models with function calling.
    
    This agent handles direct responses and tool execution for simpler queries,
    and can determine when a query should be handed off to DeepSeek.
    """
    
    def __init__(self, tool_registry=None, context_manager=None):
        """
        Initialize the ChatGPT agent.
        
        Args:
            tool_registry: Optional tool registry for tool execution
            context_manager: Optional context manager for conversation history
        """
        self.tool_registry = tool_registry
        self.context_manager = context_manager or ContextManager()
        self.response_formatter = MCPResponseFormatter(tool_registry=tool_registry)
        
        # Initialize usage tracker (will be set by orchestrator)
        self.usage_tracker = None
        
        # Initialize OpenAI client
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            logger.error("OPENAI_API_KEY not found in environment variables")
            raise ValueError("OPENAI_API_KEY not found in environment variables")
        
        try:
            self.client = OpenAI(api_key=api_key)
        except ImportError:
            logger.error("OpenAI package not installed. Run 'pip install openai'")
            raise ImportError("OpenAI package not installed. Run 'pip install openai'")
            
        self.model = "gpt-4o-mini"  # Using the specified model
        
        # Cache the current date at initialization
        self.current_date = self._get_current_date()
        
        # Load prompts with correct path
        self.system_prompt = self._load_system_prompt()
        
        logger.info(f"ChatGPT Agent initialized with model {self.model} and current date: {self.current_date['formatted_date']}")
    
    def set_usage_tracker(self, usage_tracker):
        """Set the usage tracker for cost tracking."""
        self.usage_tracker = usage_tracker
    
    def _get_current_date(self) -> Dict[str, str]:
        """Get the current date in multiple formats."""
        now = datetime.now()
        return {
            "current_date": now.strftime("%Y-%m-%d"),
            "current_datetime": now.isoformat(),
            "formatted_date": now.strftime("%B %d, %Y"),
            "day_of_week": now.strftime("%A"),
            "time": now.strftime("%H:%M:%S")
        }
    
    def _load_system_prompt(self) -> str:
        """Load the system prompt without any hardcoded dates."""
        try:
            # Try loading from the maintenance prompts directory
            prompt_path = "/Users/melville/Documents/Industrial_Engineering_Agent/src/agents/maintenance/prompts/system_prompt.txt"
            
            if os.path.exists(prompt_path):
                with open(prompt_path, 'r') as f:
                    prompt = f.read().strip()
                
                # Remove any hardcoded dates or date references
                # Don't add the current date here - we'll inject it as a message
                logger.info(f"Loaded system prompt from {prompt_path}")
                return prompt
            else:
                logger.warning(f"System prompt file not found at {prompt_path}")
                
        except Exception as e:
            logger.error(f"Error loading system prompt: {e}")
        
        # Fallback to enhanced minimal prompt without dates
        logger.info("Using fallback enhanced system prompt")
        return """You are a maintenance management assistant. 

Your goal is to help users with:
1. Scheduling and tracking maintenance tasks
2. Querying maintenance data
3. Analyzing performance metrics

When answering questions:
- ALWAYS use the current date provided by the get_current_date function result
- The current date is provided at the start of each conversation
- Use this date for all calculations and comparisons
- Never guess or use a date from training data
- Consider the context of previous messages
- For follow-up questions, analyze the previous response
- Be specific and helpful in your responses
- When maintenance tasks are past due, calculate overdue days based on the current date

For complex analysis, indicate that deeper investigation is needed."""
    
    def define_functions(self) -> List[Dict[str, Any]]:
        """
        Define the functions that will be available to the GPT model.
        
        Returns:
            List of function definitions (minimal for token savings)
        """
        functions = []
        
        # Quick query function
        functions.append({
            "name": "quick_query",
            "description": "Execute database queries for maintenance tasks, watchlist items, and performance data",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural language query about maintenance tasks, mechanics, or performance measurements"
                    }
                },
                "required": ["query"]
            }
        })
        
        # Watchlist details function
        functions.append({
            "name": "get_watchlist_details",
            "description": "Get detailed information about a specific watchlist item when user asks for more details about a previous result",
            "parameters": {
                "type": "object",
                "properties": {
                    "mechanic_name": {
                        "type": "string",
                        "description": "Name of the mechanic (e.g., 'Duncan J')"
                    },
                    "issue_type": {
                        "type": "string",
                        "description": "Issue type: 'response_time' or 'repair_time'"
                    }
                },
                "required": ["mechanic_name", "issue_type"]
            }
        })
        
        # ScheduledMaintenance function (simplified)
        functions.append({
            "name": "run_scheduled_maintenance",
            "description": "Run scheduled maintenance workflow with clustering analysis",
            "parameters": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["run", "check", "preview"]
                    },
                    "start_date": {"type": "string"},
                    "end_date": {"type": "string"},
                    "force": {
                        "type": "boolean", 
                        "description": "Override 30-day frequency limit"
                    }
                },
                "required": ["action"]
            }
        })
        
        # MechanicPerformance function
        functions.append({
            "name": "analyze_mechanic_performance",
            "description": "Analyze mechanic performance including response and repair times",
            "parameters": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["analyze", "run"]
                    },
                    "start_date": {"type": "string"},
                    "end_date": {"type": "string"},
                    "force": {
                        "type": "boolean",
                        "description": "Override 30-day frequency limit"
                    }
                },
                "required": ["action"]
            }
        })
        
        # QueryDatabase function (for complex queries)
        functions.append({
            "name": "query_database",
            "description": "Query database for complex requests requiring interpretation",
            "parameters": {
                "type": "object",
                "properties": {
                    "query_params": {
                        "type": "string",
                        "description": "Query parameters in format: table:columns;filters;limit"
                    }
                },
                "required": ["query_params"]
            }
        })
        
        # Tool details function (for detailed explanations)
        functions.append({
            "name": "get_tool_details",
            "description": "Get detailed explanation about a specific tool when user asks for more information",
            "parameters": {
                "type": "object",
                "properties": {
                    "tool_name": {
                        "type": "string",
                        "description": "Name of the tool to get details about (e.g., 'schedule maintenance', 'quick query', 'analyze performance')"
                    }
                },
                "required": ["tool_name"]
            }
        })
        
        return functions
    
    def process_query(self, query: str, conversation_history: Optional[List[Dict[str, str]]] = None, conversation_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Process a user query with reduced token usage and better context awareness.
        
        Args:
            query: The user's query
            conversation_history: Optional conversation history
            conversation_id: Optional conversation ID for tracking
            
        Returns:
            Response dictionary with answer and metadata
        """
        # Always refresh the current date for each query
        self.current_date = self._get_current_date()
        logger.info(f"Processing query with current date: {self.current_date['formatted_date']}")
        
        try:
            # Prepare messages with minimal context
            messages: List[ChatCompletionMessageParam] = [
                {"role": "system", "content": self.system_prompt}
            ]
            
            # ALWAYS inject the current date as a function message right after system prompt
            date_message = {
                "role": "function",
                "name": "get_current_date",
                "content": json.dumps(self.current_date)
            }
            messages.append(date_message)  # type: ignore
            logger.debug(f"Injected date message: {date_message['content']}")
            
            # Add a brief assistant acknowledgment of the date
            messages.append({
                "role": "assistant",
                "content": f"I understand that today is {self.current_date['formatted_date']} ({self.current_date['day_of_week']}). I'll use this date for all calculations."
            })
            
            def build_message(role: str, content: str) -> ChatCompletionMessageParam:
                return {"role": role, "content": content}  # type: ignore

            if conversation_history:
                recent_history = conversation_history[-6:]  # Last 3 user-assistant pairs
                for msg in recent_history:
                    role = msg["role"]
                    content = msg["content"]
                    if role in ["user", "assistant", "system", "function"] and content is not None:
                        messages.append(build_message(role, str(content)))
            
            # Add the current query
            messages.append(build_message("user", query))
            
            # Get function definitions
            functions = self.define_functions()
            
            # Log token estimate before sending
            message_contents = [str(m.get("content", "")) for m in messages if m.get("content")]
            message_text = " ".join(message_contents)
            estimated_tokens = len(message_text) // 4
            logger.info(f"Estimated input tokens: {estimated_tokens}")
            
            # Always try with functions available
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                functions=functions,  # type: ignore
                function_call="auto",
                max_tokens=500
            )
            
            # Track token usage
            token_tracker.track_openai_usage(response)
            
            # Track API call for cost tracking
            if conversation_id and self.usage_tracker:
                try:
                    self.usage_tracker.track_api_call(
                        conversation_id=conversation_id,
                        model=self.model,
                        input_tokens=(response.usage.prompt_tokens if hasattr(response, 'usage') and response.usage and hasattr(response.usage, 'prompt_tokens') and response.usage.prompt_tokens is not None else 0),
                        output_tokens=(response.usage.completion_tokens if hasattr(response, 'usage') and response.usage and hasattr(response.usage, 'completion_tokens') and response.usage.completion_tokens is not None else 0),
                        agent_type="chatgpt"
                    )
                except Exception as e:
                    logger.warning(f"Failed to track API call: {e}")
            
            message = response.choices[0].message
            
            # Handle function calls
            if message.function_call:
                function_name = message.function_call.name
                function_args = json.loads(message.function_call.arguments)
                
                logger.info(f"Executing function: {function_name} with args: {function_args}")
                
                # Track tool usage for cost tracking
                if conversation_id and self.usage_tracker:
                    try:
                        self.usage_tracker.track_tool_usage(conversation_id, function_name)
                    except Exception as e:
                        logger.warning(f"Failed to track tool usage: {e}")
                
                # Execute the function
                result = self._execute_function(function_name, function_args)
                
                # Log the result
                logger.info(f"Function {function_name} result: {json.dumps(result)[:200]}...")
                
                # Check if the function failed
                if isinstance(result, dict) and "error" in result:
                    # If function failed, return a helpful message
                    return {
                        "answer": f"I encountered an error while trying to {function_name}: {result['error']}. Let me try to help you another way.",
                        "response": response
                    }
                
                # Format the result using the response formatter
                formatted_result = self.response_formatter.format_tool_result(result, function_name)
                
                # Check if result is structured table data
                if isinstance(formatted_result, dict) and formatted_result.get("type") == "table":
                    # Convert table data to markdown for frontend display
                    table_data = formatted_result
                    headers = table_data.get("headers", [])
                    rows = table_data.get("rows", [])
                    
                    # Build markdown table
                    if headers and rows:
                        # Create header row
                        header_row = "| " + " | ".join([h.get("label", h.get("key", "")) for h in headers]) + " |"
                        separator = "|" + "|".join(["---" for _ in headers]) + "|"
                        
                        # Create data rows
                        data_rows = []
                        for row in rows:
                            row_values = []
                            for header in headers:
                                key = header.get("key", "")
                                value = str(row.get(key, ""))
                                row_values.append(value)
                            data_rows.append("| " + " | ".join(row_values) + " |")
                        
                        # Combine into markdown table
                        markdown_table = "\n".join([header_row, separator] + data_rows)
                        final_answer = f"Here are the {table_data.get('title', 'results')}:\n\n{markdown_table}"
                    else:
                        final_answer = f"Here are the {table_data.get('title', 'results')}, but no data was found."
                    
                    return {
                        "answer": final_answer,
                        "response": response
                    }
                
                # Handle regular text responses
                formatted_content = formatted_result if isinstance(formatted_result, str) else str(formatted_result)
                
                # Add function result to messages
                messages.append({
                    "role": "assistant",
                    "function_call": {
                        "name": function_name,
                        "arguments": json.dumps(function_args)
                    },
                    "content": None
                })
                messages.append({
                    "role": "function",
                    "name": function_name,
                    "content": formatted_content[:1000]  # Limit function result size
                })
                
                # Get final response with minimal context
                final_response = self.client.chat.completions.create(
                    model=self.model,
                    messages=messages[-6:],  # Only send recent messages
                    functions=functions,  # type: ignore
                    function_call="none",  # Force it to respond without calling another function
                    max_tokens=500
                )
                
                # Track token usage
                token_tracker.track_openai_usage(final_response)
                
                # Track final API call for cost tracking
                if conversation_id and self.usage_tracker:
                    try:
                        self.usage_tracker.track_api_call(
                            conversation_id=conversation_id,
                            model=self.model,
                            input_tokens=(final_response.usage.prompt_tokens if hasattr(final_response, 'usage') and final_response.usage and hasattr(final_response.usage, 'prompt_tokens') and final_response.usage.prompt_tokens is not None else 0),
                            output_tokens=(final_response.usage.completion_tokens if hasattr(final_response, 'usage') and final_response.usage and hasattr(final_response.usage, 'completion_tokens') and final_response.usage.completion_tokens is not None else 0),
                            agent_type="chatgpt"
                        )
                    except Exception as e:
                        logger.warning(f"Failed to track final API call: {e}")
                
                message = final_response.choices[0].message
            
            # Check if DeepSeek is needed
            if message.content and self._requires_deepseek(message.content):
                return {
                    "answer": message.content,
                    "requires_deepseek": True,
                    "response": response
                }
            
            # Check if user is saying goodbye (end session trigger)
            if message.content and self._is_goodbye_message(message.content):
                final_answer = message.content + "\n\n*Session ended - summary will be created*"
                return {
                    "answer": final_answer,
                    "response": response,
                    "end_session": True  # Signal to orchestrator to end session
                }
            
            # Make sure we have a proper response
            final_answer = message.content
            if not final_answer or final_answer.strip() == "":
                final_answer = "I apologize, but I couldn't analyze the data properly. Could you please rephrase your question?"
            
            return {
                "answer": final_answer,
                "response": response
            }
            
        except Exception as e:
            logger.error(f"Error processing query: {e}", exc_info=True)
            return {
                "error": str(e),
                "answer": f"I apologize, but I encountered an error while processing your query: {str(e)}"
            }
    
    def refresh_date(self):
        """Refresh the cached date - useful for long-running sessions that span midnight."""
        self.current_date = self._get_current_date()
        logger.info(f"Refreshed current date to: {self.current_date['formatted_date']}")
    
    def _is_follow_up_query(self, query: str, conversation_history: Optional[List[Dict[str, str]]]) -> bool:
        """Check if this query is likely a follow-up to a previous response."""
        if not conversation_history:
            return False
        
        # Semantic follow-up indicators (much more specific)
        follow_up_patterns = [
            # Person/mechanic references
            r'\b(?:for|from|by)\s+[A-Za-z]+(?:\s+[A-Za-z]+)?\b',  # "for Duncan", "by Sarah"
            r'\b[A-Za-z]+(?:\s+[A-Za-z]+)?(?:\s+items?|\s+tasks?|\s+data)\b',  # "Duncan items", "Sarah tasks"
            
            # Analysis requests on current data
            r'\bsummariz[e]?\b', r'\banalyze?\b', r'\btell me more\b',
            r'\bdetails?\s+(?:about|on|for)\b', r'\bmore\s+info\b',
            
            # Filtering current data
            r'\bshow\s+(?:me\s+)?(?:the\s+)?(?:ones?|items?|tasks?)\s+(?:for|from|by)\b',
            r'\b(?:filter|show)\s+(?:by|for)\b',
            
            # Reference to previous results
            r'\bthose\s+(?:items?|tasks?|results?)\b',
            r'\bthe\s+(?:list|data|results?)\s+(?:for|from|by)\b'
        ]
        
        query_lower = query.lower()
        for pattern in follow_up_patterns:
            if re.search(pattern, query_lower):
                logger.info(f"Detected semantic follow-up pattern: {pattern}")
                return True
        
        return False
    
    def _needs_function_call(self, content: str) -> bool:
        """Check if a response suggests it needs to call a function."""
        indicators = [
            "I need to query",
            "Let me check",
            "I'll look up",
            "need to find out",
            "database query",
            "I need to see",
            "let me search"
        ]
        
        content_lower = content.lower()
        for indicator in indicators:
            if indicator in content_lower:
                return True
        
        return False
    
    def _requires_deepseek(self, message: Optional[str]) -> bool:
        """
        Check if a message indicates the need for DeepSeek.
        
        Args:
            message: Message to check
            
        Returns:
            Boolean indicating if DeepSeek is needed
        """
        if not message:
            return False
            
        # Check for indicator phrases
        indicators = [
            "I need to analyze this more deeply",
            "hand this over",
            "deep analysis",
            "complex analysis",
            "advanced analytics",
            "need deeper reasoning",
            "requires more extensive analysis",
            "this requires deeper investigation"
        ]
        
        for indicator in indicators:
            if indicator.lower() in message.lower():
                return True
                
        return False
    
    def _is_goodbye_message(self, message: Optional[str]) -> bool:
        """
        Check if a message indicates the user wants to end the session.
        
        Args:
            message: Message to check
            
        Returns:
            Boolean indicating if this is a goodbye message
        """
        if not message:
            return False
        
        message_lower = message.lower().strip()
        
        # Goodbye phrases that should trigger session end
        goodbye_phrases = [
            "goodbye", "good bye", "bye", "farewell", "see you later", 
            "see ya", "talk to you later", "ttyl", "end session",
            "quit", "exit", "done", "that's all", "thank you goodbye",
            "thanks bye", "finished", "end chat", "log out", "logout"
        ]
        
        # Check if the message contains any goodbye phrase
        for phrase in goodbye_phrases:
            if phrase in message_lower:
                logger.info(f"Detected goodbye phrase: '{phrase}' in message")
                return True
        
        # Check for simple variations
        if message_lower in ["bye!", "goodbye!", "done!", "thanks!", "finished!"]:
            logger.info(f"Detected goodbye message: '{message_lower}'")
            return True
            
        return False
    
    def _execute_function(self, function_name: str, function_args: Dict[str, Any]) -> Any:
        """
        Execute a function call with better error handling.
        
        Args:
            function_name: Name of the function to call
            function_args: Arguments for the function
            
        Returns:
            Result of the function call
        """
        try:
            logger.info(f"Executing {function_name} with args: {json.dumps(function_args)}")
            
            if function_name == "quick_query":
                # Extract the query string from function_args
                query_string = function_args.get("query", "")
                logger.info(f"Quick query string: {query_string}")
                
                if self.tool_registry:
                    result = self.tool_registry.execute_tool("quick_query", {"query": query_string})
                    logger.info(f"Quick query execution result: {json.dumps(result)[:200]}...")
                    return result
                else:
                    logger.error("Tool registry not available for quick_query")
                    return {"error": "Tool registry not available"}
                    
            elif function_name == "get_watchlist_details":
                if self.tool_registry:
                    result = self.tool_registry.execute_tool("get_watchlist_details", function_args)
                    logger.info(f"Watchlist details result: {json.dumps(result)[:200]}...")
                    return result
                else:
                    logger.error("Tool registry not available for get_watchlist_details")
                    return {"error": "Tool registry not available"}
                    
            elif function_name == "run_scheduled_maintenance":
                if self.tool_registry:
                    result = self.tool_registry.execute_tool("run_scheduled_maintenance", function_args)
                    logger.info(f"Scheduled maintenance result: {json.dumps(result)[:200]}...")
                    return result
                else:
                    logger.error("Tool registry not available for run_scheduled_maintenance")
                    return {"error": "Tool registry not available"}
                    
            elif function_name == "analyze_mechanic_performance":
                if self.tool_registry:
                    result = self.tool_registry.execute_tool("analyze_mechanic_performance", function_args)
                    logger.info(f"Mechanic performance result: {json.dumps(result)[:200]}...")
                    return result
                else:
                    logger.error("Tool registry not available for analyze_mechanic_performance")
                    return {"error": "Tool registry not available"}
                    
            elif function_name == "query_database":
                if self.tool_registry:
                    result = self.tool_registry.execute_tool("query_database", function_args)
                    logger.info(f"Database query result: {json.dumps(result)[:200]}...")
                    return result
                else:
                    logger.error("Tool registry not available for query_database")
                    return {"error": "Tool registry not available"}
                    
            elif function_name == "get_tool_details":
                # Load detailed tool explanation from catalog file
                tool_name = function_args.get("tool_name", "").lower()
                try:
                    result = self._load_tool_details(tool_name)
                    logger.info(f"Tool details loaded for: {tool_name}")
                    return {"detailed_explanation": result, "tool_requested": tool_name}
                except Exception as e:
                    logger.error(f"Error loading tool details: {e}")
                    return {"error": f"Could not load details for '{tool_name}'. Available tools: view data, schedule maintenance, analyze performance, get details"}
                    
            else:
                logger.error(f"Unknown function: {function_name}")
                return {"error": f"Unknown function '{function_name}'"}
                
        except Exception as e:
            logger.error(f"Error executing function {function_name}: {e}", exc_info=True)
            return {"error": f"Error executing {function_name}: {str(e)}"}
    
    def _store_query_metadata(self, function_name: str, function_args: Dict[str, Any], result: Any):
        """
        Store simplified metadata about the executed query for context-aware follow-ups.
        
        Args:
            function_name: Name of the function that was executed
            function_args: Arguments passed to the function
            result: Result returned by the function
        """
        try:
            # Only store metadata for successful query operations
            if function_name == "quick_query" and isinstance(result, dict) and result.get("success"):
                query_type = result.get("query_type", "unknown")
                query_text = function_args.get("query", "").lower()
                
                # Extract simple filters from the query text
                mechanic_filter = self._extract_mechanic_from_query(query_text)
                
                # Store simple context
                self.context_manager.add_query_metadata(
                    query_type=query_type,
                    tool_name=function_name,
                    filters={"mechanic_name": mechanic_filter} if mechanic_filter else {},
                    table_shown=query_type
                )
                logger.info(f"Stored simple context: {query_type} query, mechanic: {mechanic_filter or 'none'}")
            
            # Clear context for non-query operations to avoid confusion
            elif function_name in ["get_tool_details", "run_scheduled_maintenance", "analyze_mechanic_performance"]:
                # These operations change context, so clear follow-up state
                logger.info(f"Clearing context after {function_name} operation")
                self.context_manager.clear_query_metadata()
        
        except Exception as e:
            logger.warning(f"Failed to store query metadata: {e}")
    
    def _extract_mechanic_from_query(self, query_text: str) -> Optional[str]:
        """Extract mechanic name from query text using simple patterns."""
        import re
        
        # Simple patterns for mechanic names
        patterns = [
            r'(?:for|by|from)\s+([A-Za-z]+(?:\s+[A-Za-z]+)?)',
            r'([A-Za-z]+(?:\s+[A-Za-z]+)?)\s+(?:items?|tasks?|data)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, query_text, re.IGNORECASE)
            if match:
                name = match.group(1).strip().lower()
                # Basic validation - avoid common words
                if name not in ['the', 'all', 'any', 'some', 'show', 'list', 'view']:
                    return name
        
        return None
    
    def _load_tool_details(self, tool_name: str) -> str:
        """Load detailed tool explanation from catalog file."""
        try:
            catalog_path = "/Users/melville/Documents/Industrial_Engineering_Agent/src/agents/maintenance/prompts/tool_catalog.txt"
            
            if not os.path.exists(catalog_path):
                return f"Tool catalog not found at {catalog_path}. Please check the file exists."
            
            with open(catalog_path, 'r') as f:
                content = f.read()
            
            # Let ChatGPT handle the matching - just provide the catalog with clear instructions
            return f"User asked about: '{tool_name}'\n\nPlease find the matching section in this catalog and provide the detailed explanation:\n\n{content}"
            
        except Exception as e:
            logger.error(f"Error loading tool details: {e}")
            return f"Error loading tool details: {str(e)}"