# /src/cost_tracking/cost_calculator.py
"""
Cost calculation utilities for LLM usage and compute resources.

Calculates costs for:
- OpenAI and DeepSeek API usage
- Estimated cloud compute costs
- Resource usage (CPU, memory, processing time)
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger("cost_calculator")

class CostCalculator:
    """
    Calculates costs for LLM usage and compute resources.
    
    Provides cost calculations for current development (estimates) and 
    future cloud hosting (actual costs).
    """
    
    def __init__(self):
        """Initialize the cost calculator with current pricing."""
        
        # LLM API Pricing (per 1000 tokens) - Updated May 2024
        self.llm_pricing = {
            # OpenAI models
            "gpt-4o-mini": {"input": 0.00015, "output": 0.0006},
            "gpt-3.5-turbo-0125": {"input": 0.0005, "output": 0.0015},
            "gpt-4-turbo-preview": {"input": 0.01, "output": 0.03},
            "gpt-4o": {"input": 0.005, "output": 0.015},
            
            # DeepSeek models  
            "deepseek-chat": {"input": 0.0003, "output": 0.0006},
            "deepseek-coder": {"input": 0.00015, "output": 0.0006},
        }
        
        # Cloud Compute Pricing (estimates for Google Cloud)
        self.compute_pricing = {
            # Cloud Run (Serverless) - per second
            "cloud_run": {
                "cpu_per_second": 0.00002400,      # $0.024 per vCPU-hour / 3600
                "memory_per_gb_second": 0.00000250  # $0.0025 per GB-hour / 3600
            },
            
            # Compute Engine (Dedicated) - per second  
            "compute_engine": {
                "n1_standard_1": 0.000013194,      # $0.0475/hour / 3600
                "n1_standard_2": 0.000026389,      # $0.0950/hour / 3600
                "n1_standard_4": 0.000052778       # $0.1900/hour / 3600
            }
        }
        
        logger.info("Cost calculator initialized with current pricing")
    
    def calculate_llm_cost(self, 
                          model: str, 
                          input_tokens: int, 
                          output_tokens: int) -> Dict[str, Any]:
        """
        Calculate LLM API cost based on token usage.
        
        Args:
            model: Model name (e.g., 'gpt-4o-mini', 'deepseek-chat')
            input_tokens: Number of input tokens
            output_tokens: Number of output tokens
            
        Returns:
            Dictionary with cost breakdown
        """
        pricing = self.llm_pricing.get(model, {"input": 0.0, "output": 0.0})
        
        input_cost = (input_tokens / 1000) * pricing["input"]
        output_cost = (output_tokens / 1000) * pricing["output"]
        total_cost = input_cost + output_cost
        
        return {
            "input_cost": round(input_cost, 6),
            "output_cost": round(output_cost, 6),
            "total_llm_cost": round(total_cost, 6),
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "model": model
        }
    
    def calculate_cloud_run_cost(self, 
                                processing_time_ms: int, 
                                memory_usage_mb: int = 512) -> Dict[str, Any]:
        """
        Calculate estimated Cloud Run (serverless) compute cost.
        
        Args:
            processing_time_ms: Processing time in milliseconds
            memory_usage_mb: Memory usage in MB (default 512MB)
            
        Returns:
            Dictionary with compute cost breakdown
        """
        processing_time_seconds = processing_time_ms / 1000
        memory_usage_gb = memory_usage_mb / 1024
        
        pricing = self.compute_pricing["cloud_run"]
        
        cpu_cost = processing_time_seconds * pricing["cpu_per_second"]
        memory_cost = processing_time_seconds * memory_usage_gb * pricing["memory_per_gb_second"]
        total_compute_cost = cpu_cost + memory_cost
        
        return {
            "cpu_cost": round(cpu_cost, 8),
            "memory_cost": round(memory_cost, 8),
            "total_compute_cost": round(total_compute_cost, 8),
            "processing_time_ms": processing_time_ms,
            "memory_usage_mb": memory_usage_mb,
            "hosting_type": "cloud_run"
        }
    
    def calculate_dedicated_server_cost(self, 
                                      processing_time_ms: int,
                                      server_type: str = "n1_standard_1") -> Dict[str, Any]:
        """
        Calculate estimated dedicated server compute cost.
        
        Args:
            processing_time_ms: Processing time in milliseconds
            server_type: Server type (n1_standard_1, n1_standard_2, n1_standard_4)
            
        Returns:
            Dictionary with compute cost breakdown
        """
        processing_time_seconds = processing_time_ms / 1000
        
        pricing = self.compute_pricing["compute_engine"].get(
            server_type, 
            self.compute_pricing["compute_engine"]["n1_standard_1"]
        )
        
        total_compute_cost = processing_time_seconds * pricing
        
        return {
            "total_compute_cost": round(total_compute_cost, 8),
            "processing_time_ms": processing_time_ms,
            "server_type": server_type,
            "hosting_type": "dedicated_server"
        }
    
    def calculate_total_query_cost(self,
                                  model: str,
                                  input_tokens: int,
                                  output_tokens: int,
                                  processing_time_ms: int,
                                  memory_usage_mb: int = 512,
                                  hosting_type: str = "cloud_run") -> Dict[str, Any]:
        """
        Calculate total cost for a query including LLM and compute costs.
        
        Args:
            model: LLM model name
            input_tokens: Number of input tokens
            output_tokens: Number of output tokens  
            processing_time_ms: Processing time in milliseconds
            memory_usage_mb: Memory usage in MB
            hosting_type: 'cloud_run' or 'dedicated_server'
            
        Returns:
            Dictionary with complete cost breakdown
        """
        # Calculate LLM cost
        llm_cost = self.calculate_llm_cost(model, input_tokens, output_tokens)
        
        # Calculate compute cost based on hosting type
        if hosting_type == "cloud_run":
            compute_cost = self.calculate_cloud_run_cost(processing_time_ms, memory_usage_mb)
        else:
            compute_cost = self.calculate_dedicated_server_cost(processing_time_ms)
        
        # Combine costs
        total_cost = llm_cost["total_llm_cost"] + compute_cost["total_compute_cost"]
        
        return {
            "llm_costs": llm_cost,
            "compute_costs": compute_cost,
            "total_cost": round(total_cost, 6),
            "cost_breakdown": {
                "llm_percentage": round((llm_cost["total_llm_cost"] / total_cost) * 100, 1) if total_cost > 0 else 0,
                "compute_percentage": round((compute_cost["total_compute_cost"] / total_cost) * 100, 1) if total_cost > 0 else 0
            },
            "timestamp": datetime.now().isoformat()
        }
    
    def estimate_monthly_cost(self, 
                            daily_queries: int,
                            avg_tokens_per_query: int = 1000,
                            avg_processing_time_ms: int = 2000,
                            model: str = "gpt-4o-mini") -> Dict[str, float]:
        """
        Estimate monthly costs based on usage patterns.
        
        Args:
            daily_queries: Average queries per day
            avg_tokens_per_query: Average tokens per query (input + output)
            avg_processing_time_ms: Average processing time per query
            model: Primary LLM model used
            
        Returns:
            Dictionary with monthly cost estimates
        """
        # Assume 70% input, 30% output tokens
        input_tokens = int(avg_tokens_per_query * 0.7)
        output_tokens = int(avg_tokens_per_query * 0.3)
        
        # Calculate cost per query
        query_cost = self.calculate_total_query_cost(
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            processing_time_ms=avg_processing_time_ms
        )
        
        # Monthly estimates
        monthly_queries = daily_queries * 30
        monthly_cost = query_cost["total_cost"] * monthly_queries
        
        return {
            "cost_per_query": query_cost["total_cost"],
            "daily_cost": round(query_cost["total_cost"] * daily_queries, 2),
            "monthly_cost": round(monthly_cost, 2),
            "monthly_queries": monthly_queries,
            "llm_monthly_cost": round(query_cost["llm_costs"]["total_llm_cost"] * monthly_queries, 2),
            "compute_monthly_cost": round(query_cost["compute_costs"]["total_compute_cost"] * monthly_queries, 2)
        }
    
    def get_pricing_info(self) -> Dict[str, Any]:
        """
        Get current pricing information for all models and compute options.
        
        Returns:
            Dictionary with all pricing data
        """
        return {
            "llm_pricing": self.llm_pricing,
            "compute_pricing": self.compute_pricing,
            "last_updated": "2024-05-01",
            "currency": "USD"
        }
