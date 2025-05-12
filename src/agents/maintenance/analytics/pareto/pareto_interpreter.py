import logging
import traceback
from typing import List, Dict, Any, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def interpret_dimension(dimension: str, results: Dict[str, Any]) -> Dict[str, Any]:
    """
    Interpret Pareto analysis results for a specific dimension.
    
    Args:
        dimension: The dimension name being interpreted
        results: Analysis results for this dimension
        
    Returns:
        Dictionary with interpreted findings
    """
    try:
        # Check if there was an error in analysis
        if 'error' in results:
            return {
                'dimension': dimension,
                'error': results['error'],
                'findings': []
            }
        
        # Get the pareto records
        pareto_records = results.get('pareto_records', [])
        
        if not pareto_records:
            return {
                'dimension': dimension,
                'error': 'No pareto records found',
                'findings': []
            }
        
        # Get total metric value and column name
        total_value = results.get('total', 0)
        column_name = results.get('column', dimension)
        
        # Create findings for each pareto contributor
        findings = []
        
        for record in pareto_records:
            category = record.get(column_name)
            percentage = record.get('percentage', 0)
            incident_count = record.get('incident_count', 0)
            display_value = record.get('display_value', 0)
            display_unit = record.get('display_unit', 'units')
            
            # Get related factors
            related_factors = record.get('related_factors', {})
            
            finding = {
                'dimension': dimension,
                'category': category,
                'percentage': percentage,
                'value': display_value,
                'unit': display_unit,
                'incident_count': incident_count,
                'related_factors': related_factors
            }
            
            findings.append(finding)
        
        # Return interpreted results
        return {
            'dimension': dimension,
            'total_value': total_value,
            'pareto_count': len(pareto_records),
            'total_categories': results.get('total_count', 0),
            'findings': findings
        }
    
    except Exception as e:
        logger.error(f"Error interpreting dimension {dimension}: {e}")
        logger.error(traceback.format_exc())
        return {
            'dimension': dimension,
            'error': str(e),
            'findings': []
        }

def find_cross_dimensional_patterns(analysis_results: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Find patterns across dimensions in Pareto analysis results.
    
    Args:
        analysis_results: Complete Pareto analysis results
        
    Returns:
        List of cross-dimensional patterns
    """
    try:
        patterns = []
        
        # Get dimension results
        dimensions = analysis_results.get('dimensions', {})
        
        # Skip if less than 2 dimensions 
        if len(dimensions) < 2:
            return patterns
        
        # Look for common related factors
        for dim1_name, dim1_results in dimensions.items():
            if 'error' in dim1_results:
                continue
                
            for record in dim1_results.get('pareto_records', []):
                col_name = dim1_results.get('column', dim1_name)
                category = record.get(col_name)
                percentage = record.get('percentage', 0)
                
                # Only consider high impact contributors (>10%)
                if percentage < 10:
                    continue
                
                related_factors = record.get('related_factors', {})
                
                # Look for strong relationships (>50%)
                for rel_dim, factors in related_factors.items():
                    for factor in factors:
                        factor_name = factor.get('name')
                        factor_pct = factor.get('percentage', 0)
                        
                        if factor_pct >= 50:
                            # Check if this factor is also a pareto contributor
                            if rel_dim in dimensions:
                                rel_results = dimensions[rel_dim]
                                if 'error' not in rel_results:
                                    rel_col = rel_results.get('column', rel_dim)
                                    
                                    for rel_record in rel_results.get('pareto_records', []):
                                        if rel_record.get(rel_col) == factor_name:
                                            rel_pct = rel_record.get('percentage', 0)
                                            
                                            # Found a significant relationship
                                            pattern = {
                                                'primary_dimension': dim1_name,
                                                'primary_category': category,
                                                'primary_percentage': percentage,
                                                'related_dimension': rel_dim,
                                                'related_category': factor_name,
                                                'related_percentage': rel_pct,
                                                'relationship_strength': factor_pct,
                                                'pattern_description': (
                                                    f"{factor_pct:.1f}% of issues with {dim1_name} '{category}' "
                                                    f"involve {rel_dim} '{factor_name}'"
                                                )
                                            }
                                            patterns.append(pattern)
        
        return patterns
    
    except Exception as e:
        logger.error(f"Error finding cross-dimensional patterns: {e}")
        logger.error(traceback.format_exc())
        return []

def interpret_findings(analysis_results: Dict[str, Any]) -> Dict[str, Any]:
    """
    Interpret Pareto analysis results and generate actionable insights.
    
    Args:
        analysis_results: Dictionary with analysis results by dimension
        
    Returns:
        Dictionary with interpreted findings and cross-dimensional insights
    """
    try:
        logger.info("Interpreting Pareto analysis findings")
        
        # Check if there was an error in analysis
        if 'error' in analysis_results:
            return {
                'error': analysis_results['error'],
                'interpreted_findings': {}
            }
        
        # Process findings for each dimension
        interpreted_findings = {}
        
        dimensions = analysis_results.get('dimensions', {})
        for dimension, results in dimensions.items():
            interpreted_findings[dimension] = interpret_dimension(dimension, results)
        
        # Add cross-dimensional insights
        cross_dimensional = find_cross_dimensional_patterns(analysis_results)
        if cross_dimensional:
            interpreted_findings['cross_dimensional'] = cross_dimensional
        
        logger.info("Interpretation of findings completed successfully")
        return {
            'record_count': analysis_results.get('record_count', 0),
            'metric': analysis_results.get('metric', 'total_downtime'),
            'threshold': analysis_results.get('threshold', 80.0),
            'interpreted_findings': interpreted_findings
        }
        
    except Exception as e:
        logger.error(f"Error interpreting findings: {e}")
        logger.error(traceback.format_exc())
        return {
            'error': str(e),
            'interpreted_findings': {}
        }

# For testing purposes
if __name__ == "__main__":
    import json
    
    # Example analysis results
    test_results = {
        'dimensions': {
            'machine': {
                'pareto_records': [
                    {
                        'machine_number': '015',
                        'percentage': 28.5,
                        'incident_count': 10,
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
    
    result = interpret_findings(test_results)
    print(json.dumps(result, indent=2))