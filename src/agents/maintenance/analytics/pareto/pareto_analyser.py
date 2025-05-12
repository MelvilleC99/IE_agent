import pandas as pd
import numpy as np
import logging
import traceback
from typing import List, Dict, Any, Tuple, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def analyze_dimension(records: List[Dict], dimension: str, metric: str = 'total_downtime', 
                      threshold: float = 80.0) -> Dict[str, Any]:
    """
    Perform Pareto analysis on a specific dimension of the data.
    
    Args:
        records: List of maintenance record dictionaries
        dimension: Dimension to analyze (e.g., 'machine_number', 'reason')
        metric: Column to sum for the analysis (default: 'total_downtime')
        threshold: Cumulative percentage threshold for the Pareto subset (default: 80%)
        
    Returns:
        Dictionary with analysis results
    """
    try:
        # Convert to DataFrame
        df = pd.DataFrame(records)
        
        # Based on the actual schema from downtime_detail_rows.sql
        dimension_columns = {
            'machine': 'machine_number',
            'machine_type': 'machine_type',
            'reason': 'reason',
            'line': 'production_line_name',
            'product_category': 'product_category',
            'fabric_type': 'fabric_type',
            'supervisor': 'supervisor_name',
            'mechanic': 'mechanic_name'
        }
        
        column = dimension_columns.get(dimension, dimension)
        
        # Check if column exists
        if column not in df.columns:
            logger.warning(f"Column {column} not found in data for dimension {dimension}")
            return {
                'error': f"Column {column} not found in data",
                'dimension': dimension
            }
        
        # Check if metric exists and convert to numeric
        if metric not in df.columns:
            logger.warning(f"Metric {metric} not found in data")
            return {
                'error': f"Metric {metric} not found in data",
                'dimension': dimension
            }
        
        # Convert metric to float
        try:
            df[metric] = pd.to_numeric(df[metric], errors='coerce')
            df = df.dropna(subset=[metric])
        except Exception as e:
            logger.error(f"Error converting metric to numeric: {e}")
            return {
                'error': f"Error converting metric {metric} to numeric: {e}",
                'dimension': dimension
            }
        
        # Group by dimension and sum metric
        grouped = df.groupby(column)[metric].sum().reset_index()
        
        # Get incident counts
        counts = df.groupby(column).size()
        counts = counts.reset_index()
        counts.columns = [column, 'incident_count']
        grouped = pd.merge(grouped, counts, on=column)
        
        # Sort by metric in descending order
        grouped = grouped.sort_values(by=metric, ascending=False)
        
        # Calculate percentages
        total = grouped[metric].sum()
        if total > 0:
            grouped['percentage'] = 100 * grouped[metric] / total
            grouped['cumulative'] = grouped[metric].cumsum()
            grouped['cumulative_percentage'] = 100 * grouped['cumulative'] / total
            
            # Format metric for display based on your actual data format
            # Assuming total_downtime is in seconds based on your SQL example
            grouped['display_value'] = grouped[metric] / 60  # Convert seconds to minutes
            grouped['display_unit'] = 'minutes'
        else:
            grouped['percentage'] = 0
            grouped['cumulative'] = 0
            grouped['cumulative_percentage'] = 0
            grouped['display_value'] = grouped[metric]
            grouped['display_unit'] = 'units'
        
        # Find the Pareto subset
        pareto_subset = grouped[grouped['cumulative_percentage'] <= threshold].copy()
        
        # If no rows meet the threshold, include at least one row
        if len(pareto_subset) == 0 and len(grouped) > 0:
            pareto_subset = grouped.iloc[[0]].copy()
        
        # Add is_pareto_contributor flag
        grouped['is_pareto_contributor'] = grouped.index.isin(pareto_subset.index)
        
        # Convert to records
        all_records = grouped.to_dict(orient='records')
        pareto_records = pareto_subset.to_dict(orient='records')
        
        return {
            'dimension': dimension,
            'column': column,
            'metric': metric,
            'threshold': threshold,
            'total': float(total),
            'all_records': all_records,
            'pareto_records': pareto_records,
            'pareto_count': len(pareto_records),
            'total_count': len(all_records)
        }
    
    except Exception as e:
        logger.error(f"Error in analyze_dimension for {dimension}: {e}")
        logger.error(traceback.format_exc())
        return {
            'error': str(e),
            'dimension': dimension,
            'traceback': traceback.format_exc()
        }

def get_related_factors(records: List[Dict], dimension: str, category: str, 
                        other_dimensions: List[str]) -> Dict[str, List]:
    """
    Find related factors for a specific category in a dimension.
    
    Args:
        records: List of maintenance record dictionaries
        dimension: Primary dimension (e.g., 'machine_number')
        category: Category within dimension (e.g., '015')
        other_dimensions: Other dimensions to analyze for relationships
        
    Returns:
        Dictionary with related factors by dimension
    """
    try:
        # Convert to DataFrame
        df = pd.DataFrame(records)
        
        # Map dimensions to actual column names based on the downtime_detail schema
        dimension_columns = {
            'machine': 'machine_number',
            'machine_type': 'machine_type',
            'reason': 'reason',
            'line': 'production_line_name',
            'product_category': 'product_category',
            'fabric_type': 'fabric_type',
            'supervisor': 'supervisor_name',
            'mechanic': 'mechanic_name'
        }
        
        primary_column = dimension_columns.get(dimension, dimension)
        
        # Filter to records for this category
        filtered_df = df[df[primary_column] == category]
        
        if filtered_df.empty:
            return {}
        
        related = {}
        
        for other_dim in other_dimensions:
            other_column = dimension_columns.get(other_dim, other_dim)
            
            if other_column not in filtered_df.columns:
                continue
            
            # Get distribution of values for this dimension
            counts = filtered_df[other_column].value_counts()
            total = counts.sum()
            
            if total > 0:
                percentages = 100 * counts / total
                
                # Keep top 3 factors
                top_factors = []
                for val, pct in list(zip(percentages.index, percentages))[:3]:
                    if pd.notna(val) and str(val).strip() != '':
                        top_factors.append({
                            'name': str(val),
                            'percentage': float(pct)
                        })
                
                if top_factors:
                    related[other_dim] = top_factors
        
        return related
    
    except Exception as e:
        logger.error(f"Error in get_related_factors for {dimension}={category}: {e}")
        return {}

def run_analysis(records: List[Dict], dimensions: Optional[List[str]] = None, 
                 metric: str = 'total_downtime', threshold: float = 80.0) -> Dict[str, Any]:
    """
    Run Pareto analysis on maintenance records.
    
    Args:
        records: List of maintenance record dictionaries
        dimensions: List of dimensions to analyze (default: all supported dimensions)
        metric: Column to sum for the analysis (default: 'total_downtime')
        threshold: Cumulative percentage threshold for the Pareto subset (default: 80%)
        
    Returns:
        Dictionary with analysis results for each dimension
    """
    try:
        # Default dimensions if none specified
        if dimensions is None:
            dimensions = ['machine', 'reason', 'line', 'product_category']
        
        results = {
            'dimensions': {},
            'threshold': threshold,
            'metric': metric,
            'record_count': len(records),
            'records': records  # Include the records for period calculation
        }
        
        # Analyze each dimension
        for dimension in dimensions:
            dimension_results = analyze_dimension(records, dimension, metric, threshold)
            results['dimensions'][dimension] = dimension_results
            
            # If there was an error, log it but continue with other dimensions
            if 'error' in dimension_results:
                logger.warning(f"Error analyzing dimension {dimension}: {dimension_results['error']}")
        
        # Add cross-dimensional patterns
        cross_patterns = []
        for dimension in dimensions:
            # Get Pareto contributors for this dimension
            if dimension in results['dimensions'] and 'pareto_records' in results['dimensions'][dimension]:
                for record in results['dimensions'][dimension]['pareto_records']:
                    category = record.get('machine_number' if dimension == 'machine' else dimension)
                    if category:
                        # Get related factors for this category
                        other_dimensions = [d for d in dimensions if d != dimension]
                        related = get_related_factors(records, dimension, category, other_dimensions)
                        
                        if related:
                            pattern = {
                                "primary_dimension": dimension,
                                "primary_category": category,
                                "related_factors": related
                            }
                            cross_patterns.append(pattern)
        
        results['cross_dimensional'] = cross_patterns
        
        return results
        
    except Exception as e:
        logger.error(f"Error in run_analysis: {e}")
        logger.error(traceback.format_exc())
        return {
            'error': str(e),
            'traceback': traceback.format_exc()
        }

# For testing purposes
if __name__ == "__main__":
    import json
    
    # Example based on actual schema from downtime_detail_rows.sql
    test_records = [
        {
            "id": "0emkNoe1KQqp9JB9rCcl",
            "created_at": "2025-03-11 08:23:23.068+00",
            "resolved_at": "2025-03-11 08:25:57.377+00",
            "updated_at": "2025-03-11 08:25:57.377+00",
            "machine_number": "015",
            "machine_type": "Plain Machine",
            "machine_make": "Juki",
            "machine_model": "445",
            "machine_purchase_date": "2016-06-17",
            "mechanic_id": "019",
            "mechanic_name": "Steven Smith",
            "mechanic_acknowledged": "true",
            "mechanic_acknowledged_at": "2025-03-11 08:24:14.93+00",
            "supervisor_id": "010",
            "supervisor_name": "Elizabeth September",
            "production_line_id": "996EvfO9b23DC2FwsYJp",
            "production_line_name": "Line 2",
            "style_id": "t6J1zf9hEnCRMUI4Hhjw",
            "style_number": "TEST4",
            "product_category": "Basic Men's T",
            "product_type": "Tops",
            "fabric_type": "Single Jersey",
            "reason": "Timing",
            "status": "Closed",
            "total_downtime": "154.308",
            "total_repair_time": "102.446",
            "total_response_time": "51.862"
        }
    ]
    
    result = run_analysis(test_records)
    print(json.dumps(result, indent=2))