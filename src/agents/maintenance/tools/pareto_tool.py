import os
import sys
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from langchain.tools import BaseTool
from langchain.pydantic_v1 import BaseModel, Field

# Ensure src/ directory is on sys.path for absolute imports
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "../../.."))
src_root = os.path.join(project_root, "src")
if src_root not in sys.path:
    sys.path.insert(0, src_root)

from agents.maintenance.workflows.pareto_workflow import ParetoAnalysisWorkflow

# Configure logging
logger = logging.getLogger(__name__)

class ParetoAnalysisInput(BaseModel):
    """Input schema for Pareto Analysis Tool"""
    threshold: float = Field(
        default=80.0, 
        description="The cumulative percentage threshold for Pareto analysis (default: 80%)"
    )
    period_start: Optional[str] = Field(
        default=None,
        description="Start date for the analysis period in ISO format (YYYY-MM-DD)"
    )
    period_end: Optional[str] = Field(
        default=None,
        description="End date for the analysis period in ISO format (YYYY-MM-DD)"
    )

class ParetoAnalysisTool(BaseTool):
    """
    LangChain tool wrapper for Pareto Analysis Workflow.
    
    This tool performs Pareto analysis on maintenance data to identify:
    - Machines with highest downtime
    - Most common failure reasons
    - Production lines with most issues
    - Product categories with highest impact
    """
    
    name: str = "pareto_analysis"
    description: str = """
    Perform Pareto analysis on maintenance data to identify the most significant factors 
    contributing to downtime. The tool analyzes machines, failure reasons, production lines, 
    and product categories to determine where 80% of problems originate.
    
    Use this tool when you need to:
    - Identify top causes of downtime
    - Find which machines need the most attention
    - Determine common failure patterns
    - Analyze maintenance trends over specific time periods
    """
    args_schema = ParetoAnalysisInput  # type: ignore
    return_direct: bool = False
    
    def _run(
        self, 
        threshold: float = 80.0,
        period_start: Optional[str] = None,
        period_end: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Execute the Pareto analysis workflow.
        
        Args:
            threshold: The cumulative percentage threshold for Pareto analysis
            period_start: Start date for the analysis period (YYYY-MM-DD)
            period_end: End date for the analysis period (YYYY-MM-DD)
            
        Returns:
            Dict containing analysis results, findings count, and any errors
        """
        try:
            # Convert string dates to datetime objects if provided
            start_date = None
            end_date = None
            
            if period_start:
                try:
                    start_date = datetime.strptime(period_start, "%Y-%m-%d")
                except ValueError:
                    return {
                        "success": False,
                        "error": f"Invalid period_start format. Expected YYYY-MM-DD, got: {period_start}"
                    }
            
            if period_end:
                try:
                    end_date = datetime.strptime(period_end, "%Y-%m-%d")
                    # Set to end of day to include all records from that day
                    end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
                except ValueError:
                    return {
                        "success": False,
                        "error": f"Invalid period_end format. Expected YYYY-MM-DD, got: {period_end}"
                    }
            
            # Initialize and run the workflow
            workflow = ParetoAnalysisWorkflow()
            result = workflow.run(
                threshold=threshold,
                period_start=start_date,
                period_end=end_date
            )
            
            # Format the response
            response = {
                "success": result.get("pareto_analysis_success", False),
                "findings_count": result.get("findings_count", 0),
                "analysis_id": result.get("analysis_id"),
                "summary": result.get("summary", "No summary available"),
                "period": {
                    "start": period_start,
                    "end": period_end
                },
                "threshold": threshold
            }
            
            # Add errors if any occurred
            if result.get("errors"):
                response["errors"] = result["errors"]
            
            return response
            
        except Exception as e:
            logger.error(f"Error running Pareto analysis tool: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _arun(
        self,
        threshold: float = 80.0,
        period_start: Optional[str] = None,
        period_end: Optional[str] = None
    ) -> Dict[str, Any]:
        """Async version of _run (not implemented, falls back to sync)"""
        return self._run(threshold, period_start, period_end)


# Convenience function to create the tool instance
def create_pareto_tool() -> ParetoAnalysisTool:
    """
    Factory function to create a ParetoAnalysisTool instance.
    
    Returns:
        ParetoAnalysisTool: Configured tool instance ready for use in LangChain agents
    """
    return ParetoAnalysisTool()


# Example usage with LangChain
if __name__ == "__main__":
    # Create the tool
    pareto_tool = create_pareto_tool()
    
    # Example: Run analysis for a specific date range
    result = pareto_tool.run({
        "threshold": 80.0,
        "period_start": "2024-01-01",
        "period_end": "2024-01-31"
    })
    
    print("Pareto Analysis Result:")
    print(f"Success: {result.get('success')}")
    print(f"Findings Count: {result.get('findings_count')}")
    print(f"Analysis ID: {result.get('analysis_id')}")
    
    if result.get('errors'):
        print("Errors:")
        for error in result['errors']:
            print(f"  - {error}")