import pandas as pd
import numpy as np
from scipy import stats
from datetime import datetime

# --- Helper functions ---
def safe_pct(current, best):
    """Return percentage worse than best safely, with improved type handling."""
    try:
        current_float = float(current)
        best_float = float(best)
        
        if best_float > 0:
            return ((current_float - best_float) / best_float) * 100.0
        else:
            return None
    except (TypeError, ValueError):
        print(f"Warning: Could not calculate percentage difference between {current} and {best}")
        return None

def calculate_z_scores(data_series):
    """
    Calculate Z-scores for a series of values, with robust type handling.
    Returns z_scores list, mean, and standard deviation as floats.
    """
    try:
        # Convert input to a list of floats, handling any type conversion issues
        float_values = []
        for val in data_series:
            try:
                float_values.append(float(val))
            except (TypeError, ValueError):
                # Skip any values that can't be converted to float
                print(f"Warning: Skipping non-numeric value in z-score calculation: {val}")
                
        if not float_values:
            return [0.0] * len(data_series), 0.0, 0.0
            
        # Calculate mean and standard deviation using basic statistics to avoid numpy type issues
        mean_val = sum(float_values) / len(float_values)
        
        # Calculate variance manually
        squared_diffs = [(x - mean_val) ** 2 for x in float_values]
        variance = sum(squared_diffs) / len(float_values)
        std_val = variance ** 0.5  # Square root for standard deviation
        
        # Calculate z-scores
        if std_val > 0:
            z_scores = [(x - mean_val) / std_val for x in float_values]
        else:
            z_scores = [0.0] * len(float_values)
            
        # Make sure we return the same number of z-scores as input data points
        # If we skipped any values, fill with zeros
        result_z_scores = [0.0] * len(data_series)
        z_idx = 0
        for i in range(len(data_series)):
            try:
                float(data_series[i])  # Test if this value was included
                result_z_scores[i] = z_scores[z_idx]
                z_idx += 1
            except (TypeError, ValueError):
                result_z_scores[i] = 0.0
                
        return result_z_scores, float(mean_val), float(std_val)
    
    except Exception as e:
        print(f"Error in z-score calculation: {e}")
        # Return safe defaults
        return [0.0] * len(data_series), 0.0, 0.0

def calculate_trend(time_series_data, time_field='time_period', value_field='avg_repair_time_min'):
    """
    Calculate trend statistics for time series data.
    Returns slope, percentage change per period, and p-value.
    """
    if len(time_series_data) < 3:  # Need at least 3 points for meaningful trend
        return None
    
    # Sort by time period
    time_series_data = time_series_data.sort_values(by=time_field)
    
    # Extract values as explicit numpy arrays of float type
    x_values = np.arange(len(time_series_data), dtype=float)
    y_values = np.array(time_series_data[value_field].tolist(), dtype=float)
    
    # Calculate regression statistics
    slope, intercept, r_value, p_value, std_err = stats.linregress(x_values, y_values)
    
    # Convert numpy scalar types to Python floats using convert_to_native_types and ensure float type
    def safe_float_convert(value):
        try:
            converted = convert_to_native_types(value)
            if isinstance(converted, (int, float)):
                return float(converted)
            return 0.0
        except (TypeError, ValueError):
            return 0.0

    slope_float = safe_float_convert(slope)
    intercept_float = safe_float_convert(intercept)
    p_value_float = safe_float_convert(p_value)
    r_value_float = safe_float_convert(r_value)
    
    # Calculate percentage change with explicit float operations
    if intercept_float > 0:
        starting_value = intercept_float
    else:
        starting_value = float(time_series_data[value_field].iloc[0])
    
    # Ensure starting_value is positive for percentage calculation
    if starting_value > 0:
        total_change = slope_float * float(len(time_series_data) - 1)
        total_pct_change = (total_change / starting_value) * 100.0
        per_period_pct_change = total_pct_change / float(len(time_series_data) - 1)
    else:
        per_period_pct_change = 0.0
    
    # Create result dictionary with explicit types
    result = {
        'slope': slope_float,
        'pct_change_per_period': float(per_period_pct_change),
        'p_value': p_value_float,
        'is_significant': bool(p_value_float < 0.05),
        'periods_analyzed': int(len(time_series_data)),
        'r_squared': float(r_value_float ** 2)
    }
    
    return result

def mechanic_by_category_summary(df, category_field):
    """
    Creates summaries of each mechanic's performance within each category
    (e.g., machine type)
    """
    try:
        # Get unique values of the category
        categories = df[category_field].unique()
        result = {}
        
        # For each category, analyze how mechanics perform
        for category in categories:
            category_data = df[df[category_field] == category]
            
            # Group by mechanic within this category
            mechanic_stats = category_data.groupby("mechanic_name").agg(
                avg_repair_time_min=("repair_time_min", "mean"),
                avg_response_time_min=("response_time_min", "mean"),
                count=("repair_time_min", "count")
            ).reset_index()
            
            if mechanic_stats.empty:
                continue
            
            # Calculate traditional best-performer comparison
            best_idx = mechanic_stats["avg_repair_time_min"].idxmin()
            if best_idx is not None:
                best_row = mechanic_stats.loc[best_idx].copy()
                best_val = float(best_row["avg_repair_time_min"])
                
                # Calculate percentage differences
                mechanic_stats["pct_worse_than_best"] = mechanic_stats["avg_repair_time_min"].apply(
                    lambda x: safe_pct(float(x), best_val)
                )
            else:
                mechanic_stats["pct_worse_than_best"] = None
            
            # Calculate Z-scores for repair times
            repair_times = [float(rt) for rt in mechanic_stats["avg_repair_time_min"]]
            z_scores, mean_repair_time, std_repair_time = calculate_z_scores(repair_times)
            mechanic_stats["repair_z_score"] = z_scores
            
            # Calculate Z-scores for response times
            response_times = [float(rt) for rt in mechanic_stats["avg_response_time_min"]]
            resp_z_scores, mean_response_time, std_response_time = calculate_z_scores(response_times)
            mechanic_stats["response_z_score"] = resp_z_scores
            
            # Create the summary for this category
            if best_idx is not None:
                best_mechanic = mechanic_stats.loc[best_idx].to_dict()
            else:
                best_mechanic = {}
                
            category_summary = {
                "mechanic_stats": mechanic_stats.to_dict(orient="records"),
                "best": best_mechanic,
                "statistical_measures": {
                    "mean_repair_time": mean_repair_time,
                    "std_dev_repair_time": std_repair_time,
                    "mean_response_time": mean_response_time,
                    "std_dev_response_time": std_response_time
                }
            }
            
            # Store in result dictionary using category as key
            result[str(category)] = category_summary
            
        return result
    except Exception as e:
        print(f"Error in mechanic_by_category_summary: {e}")
        return {}

def mechanic_by_machine_reason(df):
    """
    Creates summaries of each mechanic's performance for machine + reason combinations.
    """
    try:
        # Get unique combinations of machine type and reason
        combinations = df.groupby(["machine_type", "reason"]).size().reset_index()
        result = {}
        
        # For each combination, analyze how mechanics perform
        for _, combo in combinations.iterrows():
            machine_type = combo["machine_type"]
            reason = combo["reason"]
            
            # Filter data for this combination
            combo_data = df[(df["machine_type"] == machine_type) & (df["reason"] == reason)]
            
            # Group by mechanic within this combination
            mechanic_stats = combo_data.groupby("mechanic_name").agg(
                avg_repair_time_min=("repair_time_min", "mean"),
                avg_response_time_min=("response_time_min", "mean"),
                count=("repair_time_min", "count")
            ).reset_index()
            
            if mechanic_stats.empty:
                continue
                
            # Calculate Z-scores (if there's more than one mechanic for comparison)
            if len(mechanic_stats) > 1:
                repair_times = [float(rt) for rt in mechanic_stats["avg_repair_time_min"]]
                z_scores, mean_repair_time, std_repair_time = calculate_z_scores(repair_times)
                mechanic_stats["repair_z_score"] = z_scores
                
                response_times = [float(rt) for rt in mechanic_stats["avg_response_time_min"]]
                resp_z_scores, mean_response_time, std_response_time = calculate_z_scores(response_times)
                mechanic_stats["response_z_score"] = resp_z_scores
            else:
                # Only one mechanic, so no meaningful Z-score
                mechanic_stats["repair_z_score"] = 0
                mechanic_stats["response_z_score"] = 0
                mean_repair_time = float(mechanic_stats["avg_repair_time_min"].iloc[0])
                std_repair_time = 0
                mean_response_time = float(mechanic_stats["avg_response_time_min"].iloc[0])
                std_response_time = 0
            
            # Find the best performer
            best_idx = mechanic_stats["avg_repair_time_min"].idxmin()
            if best_idx is not None:
                best_row = mechanic_stats.loc[best_idx].copy()
                best_val = float(best_row["avg_repair_time_min"])
                
                # Calculate percentage differences
                mechanic_stats["pct_worse_than_best"] = mechanic_stats["avg_repair_time_min"].apply(
                    lambda x: safe_pct(float(x), best_val)
                )
                
                best_mechanic = best_row.to_dict()
            else:
                mechanic_stats["pct_worse_than_best"] = None
                best_mechanic = {}
            
            # Create the summary for this combination
            combo_key = f"{machine_type}_{reason}"
            combo_summary = {
                "mechanic_stats": mechanic_stats.to_dict(orient="records"),
                "machine_type": machine_type,
                "reason": reason,
                "best": best_mechanic,
                "statistical_measures": {
                    "mean_repair_time": mean_repair_time,
                    "std_dev_repair_time": std_repair_time,
                    "mean_response_time": mean_response_time,
                    "std_dev_response_time": std_response_time,
                    "mechanic_count": len(mechanic_stats)
                }
            }
            
            # Store in result dictionary
            result[combo_key] = combo_summary
            
        return result
    except Exception as e:
        print(f"Error in mechanic_by_machine_reason: {e}")
        return {}

def convert_to_native_types(obj):
    """Convert numpy types to native Python types for JSON serialization."""
    try:
        if obj is None:
            return None
        elif isinstance(obj, (int, np.integer)):
            return int(obj)
        elif isinstance(obj, (float, np.floating)):
            return float(obj)
        elif isinstance(obj, dict):
            return {str(k): convert_to_native_types(v) for k, v in obj.items()}
        elif isinstance(obj, list) or isinstance(obj, np.ndarray):
            return [convert_to_native_types(item) for item in obj]
        elif hasattr(obj, 'tolist'):  # Handle other numpy array-like objects
            return convert_to_native_types(obj.tolist())
        elif hasattr(obj, '__dict__'):  # Handle custom objects
            return convert_to_native_types(obj.__dict__)
        else:
            # For other types, try direct conversion or use string representation
            try:
                return float(obj) if '.' in str(obj) else int(obj)
            except (ValueError, TypeError):
                return str(obj)
    except Exception as e:
        print(f"Warning in convert_to_native_types: {e}, returning string representation")
        return str(obj)

def run_mechanic_analysis(start_date=None, end_date=None) -> dict:
    """
    Run mechanic performance analysis using data from the database.
    
    Args:
        start_date: Start date for analysis period (optional)
        end_date: End date for analysis period (optional)
        
    Returns:
        dict: Analysis results
    """
    try:
        # Get database connection
        from shared_services.db_client import get_connection
        supabase = get_connection()
        
        if not supabase:
            print("Error: Could not connect to database")
            return {}
            
        # Build query filters
        filters = []
        if start_date:
            if isinstance(start_date, str):
                start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
            filters.append({"column": "resolved_at", "operator": "gte", "value": start_date.isoformat()})
            
        if end_date:
            if isinstance(end_date, str):
                end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
            filters.append({"column": "resolved_at", "operator": "lte", "value": end_date.isoformat()})
        
        # Query the database
        query = supabase.table("downtime_detail").select("*")
        
        # Apply filters if they exist
        for filter_item in filters:
            query = query.filter(filter_item["column"], filter_item["operator"], filter_item["value"])
            
        records = query.execute()
        
        if not records.data:
            print("No records found in database")
            return {}
            
        # Convert to DataFrame
        df = pd.DataFrame(records.data)
        
        # --- Data preparation ---
        conversion_factor = 60  # Convert seconds to minutes
        df["repair_time_min"] = pd.to_numeric(df["total_repair_time"], errors="coerce").fillna(0) / conversion_factor
        df["response_time_min"] = pd.to_numeric(df["total_response_time"], errors="coerce").fillna(0) / conversion_factor
        
        # Check if we have timestamp data for trend analysis
        has_timestamp = False
        if "resolved_at" in df.columns:
            try:
                # Using ISO8601 format for flexible timestamp parsing
                df["time_period"] = pd.to_datetime(df["resolved_at"], format='ISO8601').dt.to_period('M')
                has_timestamp = True
                print("ANALYZER: Timestamp data found - will perform trend analysis")
            except Exception as e:
                print(f"ANALYZER: Could not process timestamps for trend analysis: {e}")
        
        # --- 1. Overall mechanic performance analysis (focus on response time) ---
        overall_stats = df.groupby("mechanic_name").agg(
            avg_repair_time_min=("repair_time_min", "mean"),
            avg_response_time_min=("response_time_min", "mean"),
            count=("repair_time_min", "count")
        ).reset_index()

        if overall_stats.empty:
            print("ANALYZER: No data found for overall stats.")
            return {}

        # Add Z-score calculations for response time
        response_times = overall_stats["avg_response_time_min"].values
        resp_z_scores, mean_response_time, std_response_time = calculate_z_scores(response_times)
        overall_stats["response_z_score"] = resp_z_scores
        
        # Also include repair time Z-scores for completeness
        repair_times = overall_stats["avg_repair_time_min"].values
        repair_z_scores, mean_repair_time, std_repair_time = calculate_z_scores(repair_times)
        overall_stats["repair_z_score"] = repair_z_scores

        overall_summary = {
            "mechanic_stats": overall_stats.to_dict(orient="records"),
            "statistical_measures": {
                "mean_response_time": mean_response_time,
                "std_dev_response_time": std_response_time,
                "mean_repair_time": mean_repair_time,
                "std_dev_repair_time": std_repair_time
            }
        }

        # --- 2. Mechanic performance by machine type (focus on repair time) ---
        mechanic_by_machine = mechanic_by_category_summary(df, "machine_type")
        
        # --- 3. Mechanic performance by machine + reason combination (focus on repair time) ---
        machine_reason_summary = mechanic_by_machine_reason(df)
        
        # --- 4. Trend analysis (if timestamp data available) ---
        trend_summary = {}
        if has_timestamp:
            # Group by mechanic and time period
            trend_data = df.groupby(["mechanic_name", "time_period"]).agg(
                avg_repair_time_min=("repair_time_min", "mean"),
                avg_response_time_min=("response_time_min", "mean"),
                count=("repair_time_min", "count")
            ).reset_index()
            
            # Calculate trend for each mechanic's repair time
            repair_trends = {}
            for mechanic in df["mechanic_name"].unique():
                mechanic_data = trend_data[trend_data["mechanic_name"] == mechanic]
                trend_stats = calculate_trend(mechanic_data, value_field="avg_repair_time_min")
                if trend_stats:
                    repair_trends[mechanic] = trend_stats
            
            # Calculate trend for each mechanic's response time
            response_trends = {}
            for mechanic in df["mechanic_name"].unique():
                mechanic_data = trend_data[trend_data["mechanic_name"] == mechanic]
                trend_stats = calculate_trend(mechanic_data, value_field="avg_response_time_min")
                if trend_stats:
                    response_trends[mechanic] = trend_stats
                    
            trend_summary = {
                "repair_time": repair_trends,
                "response_time": response_trends
            }
        
        # --- Combine all results ---
        final_summary = {
            "overall_response": overall_summary,  # Focus on response time
            "machine_repair": mechanic_by_machine,  # Focus on repair time by machine
            "machine_reason_repair": machine_reason_summary,  # Focus on repair time by machine+reason
            "trends": trend_summary if trend_summary else {}  # Include trend data
        }

        print("ANALYZER: Focused analysis complete.")
        result = convert_to_native_types(final_summary)
        
        # Ensure we're returning a dictionary
        if not isinstance(result, dict):
            print("ANALYZER: Warning - result is not a dictionary, returning empty dict")
            return {}
        
        return result
    except Exception as e:
        print(f"Error in run_mechanic_analysis: {e}")
        return {}

# Test function if run directly
if __name__ == '__main__':
    summary = run_mechanic_analysis()
    print("\n--- Generated Summary (sample) ---")
    print("Overall mechanic count:", len(summary.get('overall_response', {}).get('mechanic_stats', [])))
    print("Machine types analyzed:", len(summary.get('machine_repair', {})))
    print("Machine+Reason combinations:", len(summary.get('machine_reason_repair', {})))