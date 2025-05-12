import logging
import traceback
from typing import List, Dict, Any, Tuple
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def format_date_range(start_date, end_date):
    """Format date range for display in summary"""
    if start_date and end_date:
        try:
            if isinstance(start_date, str):
                start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
            if isinstance(end_date, str):
                end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
                
            start_str = start_date.strftime("%b %d, %Y")
            end_str = end_date.strftime("%b %d, %Y")
            
            if start_str == end_str:
                return start_str
            else:
                return f"{start_str} - {end_str}"
        except Exception as e:
            logger.error(f"Error formatting date range: {e}")
            return "Date range unspecified"
    else:
        return "All time"

def dimension_summary(dimension_name: str, findings: Dict[str, Any]) -> str:
    """Generate summary text for a specific dimension"""
    if 'error' in findings:
        return ""
    
    # Get display name for the dimension
    dimension_display = {
        'machine': 'MACHINES', 
        'reason': 'REASONS',
        'line': 'PRODUCTION LINES',
        'product_category': 'PRODUCT CATEGORIES',
        'fabric_type': 'FABRIC TYPES',
        'machine_type': 'MACHINE TYPES',
        'supervisor': 'SUPERVISORS',
        'mechanic': 'MECHANICS'
    }
    
    display_name = dimension_display.get(dimension_name, dimension_name.upper())
    
    # Start the summary section
    summary = f"TOP {display_name} ({findings.get('threshold', 80)}% of downtime):\n"
    
    # Process each finding
    dimension_findings = findings.get('findings', [])
    if not dimension_findings:
        return ""
    
    for finding in dimension_findings:
        category = finding.get('category', 'Unknown')
        percentage = finding.get('percentage', 0)
        
        # Start the bullet point
        summary += f"* {category} ({percentage:.1f}%)"
        
        # Add related factors if available
        related_factors = finding.get('related_factors', {})
        related_info = []
        
        for rel_dim, factors in related_factors.items():
            if factors and len(factors) > 0:
                rel_display = dimension_display.get(rel_dim, rel_dim.title())
                factor_str = ", ".join([f"{f['name']} ({f['percentage']:.0f}%)" for f in factors[:2]])
                related_info.append(f"{rel_display}: {factor_str}")
        
        if related_info:
            summary += f" - Main issues: {'; '.join(related_info)}"
            
        summary += "\n"
    
    return summary + "\n"

def cross_dimensional_summary(patterns: List[Dict[str, Any]]) -> str:
    """Generate summary text for cross-dimensional patterns"""
    if not patterns:
        return ""
    
    summary = "CROSS-DIMENSIONAL HOTSPOTS:\n"
    
    # Group patterns by primary dimension and category
    grouped_patterns = {}
    for pattern in patterns:
        key = (pattern['primary_dimension'], pattern['primary_category'])
        if key not in grouped_patterns:
            grouped_patterns[key] = []
        grouped_patterns[key].append(pattern)
    
    # Process each group of patterns
    for (dim, cat), dim_patterns in grouped_patterns.items():
        if len(dim_patterns) > 0:
            # Get the primary pattern info
            primary_pattern = dim_patterns[0]
            dim_display = dim.title() if dim != 'line' else 'Line'
            
            # Find the strongest relationship
            strongest = max(dim_patterns, key=lambda p: p['relationship_strength'])
            rel_dim = strongest['related_dimension'].title() if strongest['related_dimension'] != 'line' else 'Line'
            
            summary += f"* {dim_display} '{cat}' is strongly associated with {rel_dim} '{strongest['related_category']}' "
            summary += f"({strongest['relationship_strength']:.0f}% of cases)\n"
    
    # Add overall insights if there are more than 2 patterns
    if len(patterns) > 2:
        # Find the strongest overall pattern
        strongest_overall = max(patterns, key=lambda p: p['relationship_strength'])
        prim_dim = strongest_overall['primary_dimension'].title() if strongest_overall['primary_dimension'] != 'line' else 'Line'
        rel_dim = strongest_overall['related_dimension'].title() if strongest_overall['related_dimension'] != 'line' else 'Line'
        
        summary += f"* The combination of {prim_dim} '{strongest_overall['primary_category']}' + "
        summary += f"{rel_dim} '{strongest_overall['related_category']}' appears in "
        summary += f"{strongest_overall['relationship_strength']:.0f}% of relevant cases\n"
    
    return summary + "\n"

def generate_summary(interpreted_findings: Dict[str, Any], period_start=None, period_end=None) -> str:
    """
    Generate a human-readable summary of Pareto analysis findings.
    
    Args:
        interpreted_findings: Dictionary with interpreted findings
        period_start: Start date of analysis period
        period_end: End date of analysis period
        
    Returns:
        String with formatted summary text
    """
    try:
        logger.info("Generating Pareto analysis summary")
        
        # Check if there was an error in interpretation
        if 'error' in interpreted_findings:
            return f"ERROR: {interpreted_findings['error']}\nUnable to generate summary."
        
        # Format date range for display
        date_range = format_date_range(period_start, period_end)
        
        # Start the summary
        summary = f"PARETO ANALYSIS SUMMARY ({date_range})\n"
        summary += "------------------------------------------\n\n"
        
        # Get findings
        findings = interpreted_findings.get('interpreted_findings', {})
        if not findings:
            return summary + "No findings to report."
        
        # Add dimension-specific summaries
        # Define order of dimensions
        dimension_priorities = [
            'machine', 'reason', 'product_category', 'line', 
            'fabric_type', 'machine_type', 'supervisor'
        ]
        
        # Add summaries for available dimensions in priority order
        for dimension in dimension_priorities:
            if dimension in findings:
                summary += dimension_summary(dimension, findings[dimension])
        
        # Add cross-dimensional insights
        if 'cross_dimensional' in findings:
            summary += cross_dimensional_summary(findings['cross_dimensional'])
        
        logger.info("Summary generation completed successfully")
        return summary
        
    except Exception as e:
        logger.error(f"Error generating summary: {e}")
        logger.error(traceback.format_exc())
        return f"ERROR: {str(e)}\nUnable to generate summary."

# For testing purposes
if __name__ == "__main__":
    import json
    
    # Example interpreted findings
    test_findings = {
        'interpreted_findings': {
            'machine': {
                'findings': [
                    {
                        'category': '015',
                        'percentage': 28.5,
                        'related_factors': {
                            'reason': [
                                {'name': 'Timing', 'percentage': 65.0}
                            ],
                            'fabric_type': [
                                {'name': 'Single Jersey', 'percentage': 80.0}
                            ]
                        }
                    }
                ]
            }
        }
    }
    
    summary = generate_summary(test_findings, "2025-03-01", "2025-03-15")
    print(summary)