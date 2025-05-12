import logging
import traceback
from datetime import datetime
from typing import Dict, Any, Optional, List
from pathlib import Path
from dotenv import load_dotenv
import os
import sys

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(current_dir, "../../../"))
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

# Load environment
load_dotenv(Path(__file__).resolve().parents[3] / ".env.local")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

from shared_services.supabase_client import SupabaseClient

class ParetoWriter:
    """Handles storing Pareto analysis results to the database"""
    
    def __init__(self, db_client=None):
        """Initialize with database client or create new one"""
        try:
            self.db_client = db_client if db_client else SupabaseClient()
            logger.info("ParetoWriter initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing ParetoWriter: {e}")
            logger.error(traceback.format_exc())
            raise
    
    def save_analysis(self, 
                     analysis_results: Dict[str, Any],
                     interpreted_findings: Dict[str, Any],
                     summary_text: str,
                     period_start: Optional[datetime] = None,
                     period_end: Optional[datetime] = None,
                     run_by: str = "system") -> Optional[str]:
        """
        Save Pareto analysis results to the database
        
        Args:
            analysis_results: Raw analysis results
            interpreted_findings: Interpreted findings with dimensions
            summary_text: Formatted summary text
            period_start: Optional override for start date
            period_end: Optional override for end date
            run_by: Who/what initiated the analysis
            
        Returns:
            Optional[str]: The ID of the saved analysis, or None if saving failed
        """
        try:
            # Extract key metrics
            key_metrics = self._extract_key_metrics(interpreted_findings)
            
            # Extract cross-dimensional patterns
            cross_patterns = interpreted_findings.get('interpreted_findings', {}).get('cross_dimensional', [])
            
            # Extract dimension findings
            dimension_findings = {}
            for dim, findings in interpreted_findings.get('interpreted_findings', {}).items():
                if dim != 'cross_dimensional':
                    dimension_findings[dim] = findings
            
            # Get date range from analysis records
            if 'records' in analysis_results:
                records = analysis_results['records']
                if records:
                    # Convert string dates to datetime objects
                    dates = []
                    for record in records:
                        if 'resolved_at' in record:
                            try:
                                # Handle both ISO format and PostgreSQL timestamp format
                                date_str = record['resolved_at']
                                if 'Z' in date_str:
                                    date_str = date_str.replace('Z', '+00:00')
                                dates.append(datetime.fromisoformat(date_str))
                            except (ValueError, TypeError) as e:
                                logger.warning(f"Could not parse date {record.get('resolved_at')}: {e}")
                                continue
                    
                    if dates:
                        # Use earliest and latest dates from records if not overridden
                        if not period_start:
                            period_start = min(dates)
                        if not period_end:
                            period_end = max(dates)
            
            # Create entry to save
            analysis_entry = {
                "period_start": period_start.isoformat() if period_start else None,
                "period_end": period_end.isoformat() if period_end else None,
                "period_description": self._format_period_description(period_start, period_end),
                "parameters": {
                    "threshold": analysis_results.get('threshold', 80.0),
                    "dimensions": list(dimension_findings.keys())
                },
                "key_metrics": key_metrics,
                "dimension_findings": dimension_findings,
                "cross_dimensional_patterns": cross_patterns,
                "summary_text": summary_text,
                "run_by": run_by,
                "record_count": analysis_results.get('record_count', 0),
                "dimensions_analyzed": list(dimension_findings.keys()),
                "run_duration": analysis_results.get('run_duration', 0)
            }
            
            # Save to database
            result = self.db_client.insert_data(
                table_name="pareto_analyses",
                data=analysis_entry
            )
            
            if result and 'id' in result:
                logger.info(f"Saved Pareto analysis with ID: {result['id']}")
                return result['id']
            else:
                logger.error("Failed to save analysis - no ID returned")
                return None
            
        except Exception as e:
            logger.error(f"Error saving Pareto analysis: {e}")
            logger.error(traceback.format_exc())
            return None
    
    def _extract_key_metrics(self, interpreted_findings):
        """Extract key metrics from interpreted findings"""
        metrics = {
            "total_downtime_minutes": 0,
            "total_incidents": 0,
            "dimension_metrics": {}
        }
        
        findings = interpreted_findings.get('interpreted_findings', {})
        
        # Extract metrics for each dimension
        for dim, dim_findings in findings.items():
            if dim == 'cross_dimensional':
                continue
                
            dim_metrics = {
                "pareto_count": dim_findings.get('pareto_count', 0),
                "total_count": dim_findings.get('total_categories', 0)
            }
            
            # Include all contributors that make up 80%
            if dim_findings.get('findings'):
                # List of all contributors in the 80% group
                pareto_contributors = []
                for contributor in dim_findings['findings']:
                    pareto_contributors.append({
                        "category": contributor.get('category'),
                        "percentage": contributor.get('percentage'),
                        "incident_count": contributor.get('incident_count', 0),
                        "value": contributor.get('value', 0)
                    })
                
                dim_metrics["pareto_contributors"] = pareto_contributors
                
                # Also include top contributor for quick reference
                if pareto_contributors:
                    dim_metrics["top_contributor"] = pareto_contributors[0]["category"]
                    dim_metrics["top_percentage"] = pareto_contributors[0]["percentage"]
            
            metrics["dimension_metrics"][dim] = dim_metrics
        
        # Add overall metrics if available
        if findings and 'machine' in findings:
            # Estimate total incidents from machine dimension
            total_incidents = 0
            for finding in findings['machine'].get('findings', []):
                total_incidents += finding.get('incident_count', 0)
            metrics["total_incidents"] = total_incidents
            
            # Estimate total downtime from machine dimension
            total_downtime = 0
            for finding in findings['machine'].get('findings', []):
                total_downtime += finding.get('value', 0)
            metrics["total_downtime_minutes"] = total_downtime
        
        return metrics
    
    def _format_period_description(self, start, end):
        """Format a human-readable period description"""
        if not start or not end:
            return "All time"
            
        if start.year == end.year and start.month == end.month:
            return start.strftime("%B %Y")
            
        if start.year == end.year:
            return f"{start.strftime('%B')} - {end.strftime('%B %Y')}"
            
        return f"{start.strftime('%b %Y')} - {end.strftime('%b %Y')}"
    
    def get_analysis(self, analysis_id):
        """
        Retrieve a specific analysis from the database
        
        Args:
            analysis_id: ID of the analysis to retrieve
            
        Returns:
            Dictionary with the analysis data
        """
        try:
            result = self.db_client.query_table(
                table_name="pareto_analyses",
                columns="*",
                filters={"id": analysis_id},
                limit=1
            )
            
            if result and len(result) > 0:
                return result[0]
            else:
                logger.warning(f"No analysis found with ID: {analysis_id}")
                return None
                
        except Exception as e:
            logger.error(f"Error retrieving analysis: {e}")
            logger.error(traceback.format_exc())
            return None
    
    def list_analyses(self, limit=10, offset=0, start_date=None, end_date=None):
        """
        List available analyses with pagination and filtering
        
        Args:
            limit: Maximum number of results to return
            offset: Offset for pagination
            start_date: Filter to analyses after this date
            end_date: Filter to analyses before this date
            
        Returns:
            List of analysis summaries
        """
        try:
            filters = {}
            
            if start_date:
                filters["created_at"] = {"gte": start_date.isoformat()}
                
            if end_date:
                filters["created_at"] = {"lte": end_date.isoformat()}
            
            results = self.db_client.query_table(
                table_name="pareto_analyses",
                columns="id, created_at, period_description, key_metrics, run_by",
                filters=filters if filters else None,
                limit=limit
            )
            
            return results
            
        except Exception as e:
            logger.error(f"Error listing analyses: {e}")
            logger.error(traceback.format_exc())
            return []