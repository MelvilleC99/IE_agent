Maintenance Agent Codebase Analysis & README
Executive Summary
This is a Maintenance Management System built on a Two-Tier Agent Architecture using ChatGPT for quick queries and tool execution, with DeepSeq integration planned for complex analysis. The system provides database querying, maintenance scheduling, performance analytics, and cost tracking.
Code Quality Rating: 7.5/10
Core Architecture
Two-Tier Agent System
The system uses a sophisticated orchestration pattern:

ChatGPT Agent (/src/MCP/agents/chatgpt_agent.py) - Primary agent for:

Quick database queries and data retrieval
Tool execution and function calling
Session memory management
Follow-up query handling


DeepSeek Agent (/src/MCP/agents/deepseek_agent.py) - Secondary agent for:

Complex analysis requiring deeper reasoning
Advanced analytics and insights
Currently integrated but not heavily utilized


Two-Tier Orchestrator (/src/MCP/two_tier_orchestrator.py) - Central coordinator:

Routes queries between agents
Manages conversation flow
Handles cost tracking integration
Session management



Key Components & File Structure
Core Files and Paths
1. Agent Layer (/src/MCP/agents/)

chatgpt_agent.py - Main ChatGPT agent implementation
deepseek_agent.py - Secondary analysis agent

2. Orchestration Layer (/src/MCP/)

two_tier_orchestrator.py - Central query routing and session management
tool_registry.py - Tool registration and execution framework
context_manager.py - Conversation history and context tracking
token_tracker.py - Token usage monitoring
response_formatter.py - Response formatting utilities

3. API Layer (/src/api/)

main.py - FastAPI application entry point
routes/chat.py - Chat endpoint and session management

4. Tools & Functionality (/src/agents/maintenance/tools/)

supabase_tool.py - Database connection and caching
scheduled_maintenance_tool.py - Maintenance scheduling
mechanic_performance_tool.py - Performance analytics
query_tools/ - Specialized query handlers

scheduled_maintenance_query.py
watchlist_query.py



5. Analytics (/src/agents/maintenance/analytics/)

Mechanic_performance_tool/ - Mechanic performance analysis
Scheduled_Maintenance/ - Maintenance clustering and scheduling
pareto/ - Pareto analysis tools
time_series_tool/ - Time series pattern analysis

6. Cost Tracking (/src/cost_tracking/)

usage_tracker.py - API call and tool usage tracking
session_summarizer.py - Session analytics and summaries
cost_calculator.py - Cost calculation utilities

7. Shared Services (/src/shared_services/)

supabase_client.py - Database client
firebase_client.py - Firebase integration
deepseek_client.py - DeepSeek API client

8. Memory & Context (/src/agents/maintenance/memory/)

chat_memory.py - Conversation memory management
memory_manager.py - Memory persistence

Query Processing Flow
1. Request Processing
Frontend → FastAPI (/api/main.py) → Chat Route (/api/routes/chat.py) → TwoTierOrchestrator
2. Query Routing
TwoTierOrchestrator → ChatGPTAgent → Function Calling → Tool Registry → Specific Tools
3. Tool Execution
Tool Registry → Database Tools → Supabase Client → Database → Response Formatting
4. Follow-up Handling
Context Manager → Previous Query Metadata → Enhanced Follow-up Processing
Session Memory Implementation
Context Management

Short-term Memory: Last 6 messages maintained in ContextManager
Query Metadata: Tracks last executed queries for follow-up context
Session Persistence: Managed through SessionManager with timeout handling

Memory Flow

Message Storage: All user/assistant exchanges stored in conversation history
Query Context: Metadata about executed tools and filters preserved
Follow-up Detection: Semantic analysis of queries to detect follow-up intentions
Context Injection: Previous context automatically included in follow-up queries

Cost Tracking & Analytics
Usage Tracking

API Calls: Token usage, model costs, execution time
Tool Usage: Tool execution frequency and success rates
Session Analytics: Conversation length, user patterns, session summaries

Session Summarization

Automated Summaries: Generated when sessions end
Business Intelligence: Usage patterns and cost analysis
Performance Metrics: Response times and user satisfaction