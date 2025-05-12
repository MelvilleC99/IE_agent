# /Users/melville/Documents/Industrial_Engineering_Agent/src/token_tracker.py

import logging
import json
import time
from typing import Dict, Any, List, Optional, Union
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("token_tracker")

class TokenTracker:
    """
    Tracks token usage and costs for different LLM models.
    
    This class helps monitor token consumption and estimated costs
    for different models to optimize usage and budget.
    """
    
    # Pricing per 1000 tokens (as of May 2024)
    PRICING = {
        # OpenAI models
        "gpt-4o-mini": {"input": 0.00015, "output": 0.0006},  # Newest, cheapest GPT-4 class model
        "gpt-3.5-turbo-0125": {"input": 0.0005, "output": 0.0015},
        "gpt-4-turbo-preview": {"input": 0.01, "output": 0.03},
        "gpt-4o": {"input": 0.005, "output": 0.015},
        # DeepSeek models
        "deepseek-chat": {"input": 0.0003, "output": 0.0006},
        "deepseek-coder": {"input": 0.00015, "output": 0.0006},
    }
    
    def __init__(self, log_file: Optional[str] = None):
        """
        Initialize the token tracker.
        
        Args:
            log_file: Optional file path to log token usage
        """
        self.log_file = log_file or "token_usage.log"
        self.session_usage = {
            "openai": {"total_tokens": 0, "input_tokens": 0, "output_tokens": 0, "cost": 0.0},
            "deepseek": {"total_tokens": 0, "input_tokens": 0, "output_tokens": 0, "cost": 0.0}
        }
        self.requests = []
        logger.info("Token tracker initialized")
    
    def track_openai_usage(self, response: Any) -> Dict[str, Any]:
        """
        Track token usage from an OpenAI API response.
        
        Args:
            response: OpenAI API response object
            
        Returns:
            Dictionary with token usage statistics
        """
        usage = {}
        model = "unknown"
        
        try:
            if hasattr(response, "model"):
                model = response.model
                
            if hasattr(response, "usage"):
                usage = {
                    "total_tokens": response.usage.total_tokens,
                    "input_tokens": response.usage.prompt_tokens,
                    "output_tokens": response.usage.completion_tokens,
                    "model": model,
                    "timestamp": datetime.now().isoformat()
                }
                
                # Calculate cost
                model_pricing = self.PRICING.get(model, {"input": 0.0, "output": 0.0})
                cost = (
                    (usage["input_tokens"] / 1000) * model_pricing["input"] +
                    (usage["output_tokens"] / 1000) * model_pricing["output"]
                )
                usage["cost"] = cost
                
                # Update session totals
                self.session_usage["openai"]["total_tokens"] += usage["total_tokens"]
                self.session_usage["openai"]["input_tokens"] += usage["input_tokens"]
                self.session_usage["openai"]["output_tokens"] += usage["output_tokens"]
                self.session_usage["openai"]["cost"] += cost
                
                # Log the usage
                self._log_usage("openai", model, usage)
                
                logger.info(f"OpenAI usage: {usage['total_tokens']} tokens, ${cost:.4f}")
            else:
                logger.warning("Unable to extract token usage from OpenAI response")
        except Exception as e:
            logger.error(f"Error tracking OpenAI token usage: {e}")
        
        return usage
    
    def track_deepseek_usage(self, 
                             prompt_tokens: int, 
                             completion_tokens: int, 
                             model: str = "deepseek-chat") -> Dict[str, Any]:
        """
        Track token usage for DeepSeek API calls.
        
        Args:
            prompt_tokens: Number of input tokens
            completion_tokens: Number of output tokens
            model: DeepSeek model name
            
        Returns:
            Dictionary with token usage statistics
        """
        try:
            total_tokens = prompt_tokens + completion_tokens
            
            usage = {
                "total_tokens": total_tokens,
                "input_tokens": prompt_tokens,
                "output_tokens": completion_tokens,
                "model": model,
                "timestamp": datetime.now().isoformat()
            }
            
            # Calculate cost
            model_pricing = self.PRICING.get(model, {"input": 0.0, "output": 0.0})
            cost = (
                (prompt_tokens / 1000) * model_pricing["input"] +
                (completion_tokens / 1000) * model_pricing["output"]
            )
            usage["cost"] = cost
            
            # Update session totals
            self.session_usage["deepseek"]["total_tokens"] += total_tokens
            self.session_usage["deepseek"]["input_tokens"] += prompt_tokens
            self.session_usage["deepseek"]["output_tokens"] += completion_tokens
            self.session_usage["deepseek"]["cost"] += cost
            
            # Log the usage
            self._log_usage("deepseek", model, usage)
            
            logger.info(f"DeepSeek usage: {total_tokens} tokens, ${cost:.4f}")
            
            return usage
        except Exception as e:
            logger.error(f"Error tracking DeepSeek token usage: {e}")
            return {}
    
    def _log_usage(self, provider: str, model: str, usage: Dict[str, Any]) -> None:
        """
        Log token usage to file.
        
        Args:
            provider: LLM provider name
            model: Model name
            usage: Usage statistics
        """
        if self.log_file:
            try:
                log_entry = {
                    "timestamp": datetime.now().isoformat(),
                    "provider": provider,
                    "model": model,
                    "usage": usage
                }
                
                self.requests.append(log_entry)
                
                with open(self.log_file, "a") as f:
                    f.write(json.dumps(log_entry) + "\n")
            except Exception as e:
                logger.error(f"Error logging token usage: {e}")
    
    def get_session_summary(self) -> Dict[str, Any]:
        """
        Get a summary of token usage for the current session.
        
        Returns:
            Dictionary with session usage statistics
        """
        total_cost = (
            self.session_usage["openai"]["cost"] +
            self.session_usage["deepseek"]["cost"]
        )
        
        return {
            "openai": self.session_usage["openai"],
            "deepseek": self.session_usage["deepseek"],
            "total_cost": total_cost,
            "request_count": len(self.requests),
            "session_start": self.requests[0]["timestamp"] if self.requests else datetime.now().isoformat()
        }
    
    def reset_session(self) -> None:
        """Reset the session usage statistics."""
        self.session_usage = {
            "openai": {"total_tokens": 0, "input_tokens": 0, "output_tokens": 0, "cost": 0.0},
            "deepseek": {"total_tokens": 0, "input_tokens": 0, "output_tokens": 0, "cost": 0.0}
        }
        self.requests = []
        logger.info("Token tracker session reset")

# Global token tracker instance
token_tracker = TokenTracker()