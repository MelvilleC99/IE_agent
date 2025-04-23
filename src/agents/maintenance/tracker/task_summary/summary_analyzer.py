#!/usr/bin/env python3
import sys
import os
from datetime import datetime
import numpy as np
from scipy import stats

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(current_dir, "../../../"))
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

class SummaryAnalyzer:
    """
    Analyzes measurement data to generate performance metrics.
    Handles statistical calculations like trend analysis and significance testing.
    """
    def __init__(self):
        self.today = datetime.now().date()
        print(f"ANALYZER: Initializing for {self.today}")
    
    def calculate_percentage_change(self, baseline_value, latest_value, issue_type):
        """
        Calculate percentage change between two values
        
        For response_time and repair_time, negative change means improvement
        For other metrics, positive change means improvement
        
        Args:
            baseline_value: Starting value
            latest_value: Ending value
            issue_type: Type of issue being measured
            
        Returns:
            float: Percentage change (negative can be good for some metrics)
        """
        if baseline_value == 0:
            return 0.0
        
        return ((latest_value - baseline_value) / baseline_value) * 100.0
    
    def analyze_trend(self, measurements, issue_type):
        """
        Analyze trend using linear regression
        
        Args:
            measurements: List of measurement records
            issue_type: Type of issue being measured
            
        Returns:
            dict: Trend analysis results
        """
        if len(measurements) < 3:
            return {
                'slope': None,
                'r_squared': None,
                'p_value': None,
                'is_improving': None,
                'trend_description': "Insufficient data for trend analysis"
            }
        
        try:
            # Extract dates and values as numpy arrays
            dates = np.array([datetime.fromisoformat(m['measurement_date']).timestamp() for m in measurements])
            values = np.array([float(m['value']) for m in measurements])
            
            # Normalize the dates for better numerical stability
            dates = dates - dates[0]  # Start from zero
            
            # Perform linear regression with numpy arrays
            # linregress returns (slope, intercept, r_value, p_value, std_err)
            result = stats.linregress(dates, values)
            
            # Fix for Pylance type errors: explicitly extract values from result tuple
            slope = float(result[0]) if result[0] is not None else 0.0
            intercept = float(result[1]) if result[1] is not None else 0.0
            r_value = float(result[2]) if result[2] is not None else 0.0
            p_value = float(result[3]) if result[3] is not None else 1.0
            std_err = float(result[4]) if result[4] is not None else 0.0
            
            # Determine if trend is improving
            # For time metrics, negative slope is good (times decreasing)
            if issue_type in ['response_time', 'repair_time']:
                is_improving = slope < 0.0
            else:
                is_improving = slope > 0.0
            
            # Calculate r-squared
            r_squared = r_value * r_value
            
            # Create description of trend
            if p_value <= 0.05:
                if r_squared >= 0.7:
                    strength = "strong"
                elif r_squared >= 0.3:
                    strength = "moderate"
                else:
                    strength = "weak"
                    
                direction = "improving" if is_improving else "deteriorating"
                trend_description = f"{strength} {direction} trend (p={p_value:.3f}, r²={r_squared:.2f})"
            else:
                trend_description = f"no statistically significant trend (p={p_value:.3f})"
            
            return {
                'slope': slope,
                'r_squared': r_squared,
                'p_value': p_value,
                'is_improving': bool(is_improving),
                'trend_description': trend_description
            }
        except Exception as e:
            print(f"ANALYZER: Error in trend analysis: {e}")
            return {
                'slope': None,
                'r_squared': None,
                'p_value': None,
                'is_improving': None,
                'trend_description': f"Error in trend analysis: {e}"
            }
    
    def calculate_moving_average(self, measurements, window=3):
        """
        Calculate moving average of last N measurements
        
        Args:
            measurements: List of measurement records
            window: Number of measurements to include in the average
            
        Returns:
            float: Moving average or None if not enough measurements
        """
        if len(measurements) < window:
            return None
        
        recent_measurements = measurements[-window:]
        values = [float(m['value']) for m in recent_measurements]
        return sum(values) / len(values)
    
    def check_significance(self, measurements):
        """
        Check if improvement is statistically significant
        Compares first half of measurements to second half
        
        Args:
            measurements: List of measurement records
            
        Returns:
            dict: Significance test results
        """
        if len(measurements) < 4:  # Need at least 4 for meaningful comparison
            return {
                'is_significant': False,
                'p_value': None,
                'confidence': None,
                'description': "Insufficient data for significance testing"
            }
        
        try:
            # Split measurements into first and second half
            midpoint = len(measurements) // 2
            first_half = np.array([float(m['value']) for m in measurements[:midpoint]])
            second_half = np.array([float(m['value']) for m in measurements[midpoint:]])
            
            # Perform t-test with numpy arrays
            # ttest_ind returns (t-statistic, p-value)
            result = stats.ttest_ind(first_half, second_half)
            
            # Fix for Pylance type errors: explicitly extract values
            t_stat = float(result[0]) if result[0] is not None else 0.0
            p_value = float(result[1]) if result[1] is not None else 1.0
            
            # Calculate confidence level
            confidence = (1.0 - p_value) * 100.0
            
            # Determine if significant based on threshold
            is_significant = p_value <= 0.10
            
            # Generate description
            if is_significant:
                description = f"Statistically significant change ({confidence:.1f}% confidence)"
            else:
                description = f"Not statistically significant ({confidence:.1f}% confidence)"
            
            return {
                'is_significant': bool(is_significant),
                'p_value': float(p_value),
                'confidence': float(confidence),
                'description': description
            }
        except Exception as e:
            print(f"ANALYZER: Error in significance testing: {e}")
            return {
                'is_significant': False,
                'p_value': None,
                'confidence': None,
                'description': f"Error in significance testing: {e}"
            }
    
    def calculate_period_changes(self, measurements, issue_type):
        """
        Calculate changes between consecutive measurements
        
        Args:
            measurements: List of measurement records
            issue_type: Type of issue being measured
            
        Returns:
            list: List of period-to-period changes
        """
        period_changes = []
        for i in range(1, len(measurements)):
            prev = float(measurements[i-1]['value'])
            curr = float(measurements[i]['value'])
            period_change = self.calculate_percentage_change(prev, curr, issue_type)
            period_changes.append({
                'from_date': measurements[i-1]['measurement_date'],
                'to_date': measurements[i]['measurement_date'],
                'from_value': prev,
                'to_value': curr,
                'change_pct': period_change
            })
        return period_changes
    
    def analyze_task_data(self, task_data):
        """
        Analyze task data to generate comprehensive performance summary
        
        Args:
            task_data: Dictionary with task and measurements data
            
        Returns:
            dict: Comprehensive performance summary
        """
        task = task_data['task']
        measurements = task_data['measurements']
        task_id = task.get('id')
        
        print(f"ANALYZER: Analyzing task ID {task_id}: {task.get('title')}")
        
        # Check if we have enough measurements
        if not measurements or len(measurements) < 2:
            print(f"ANALYZER: Insufficient measurements for task ID {task_id}")
            return {
                'task_id': task_id,
                'status': 'insufficient_data',
                'message': f"Only {len(measurements)} measurements available. Need at least 2 for analysis."
            }
        
        # Get issue type for determining improvement direction
        issue_type = task.get('issue_type')
        
        # Calculate overall change
        baseline = measurements[0]
        latest = measurements[-1]
        overall_change_pct = self.calculate_percentage_change(
            float(baseline['value']), float(latest['value']), issue_type
        )
        
        # Calculate recent moving average
        moving_avg = self.calculate_moving_average(measurements)
        recent_change = None
        if moving_avg is not None and len(measurements) >= 3:
            recent_change = self.calculate_percentage_change(
                float(baseline['value']), moving_avg, issue_type
            )
        
        # Perform trend analysis
        trend = self.analyze_trend(measurements, issue_type)
        
        # Check statistical significance
        significance = self.check_significance(measurements)
        
        # Calculate period-to-period changes
        period_changes = self.calculate_period_changes(measurements, issue_type)
        
        # For time metrics, improvement is negative change (decreasing time)
        # For non-time metrics, improvement is positive change (increasing value)
        is_time_metric = issue_type in ['response_time', 'repair_time']
        
        # Normalize improvement percentage for consistent interpretation
        # Positive always means improvement, negative always means deterioration
        improvement_pct = overall_change_pct
        if is_time_metric:
            improvement_pct = -overall_change_pct
        
        # Compile results
        summary = {
            'task_id': task_id,
            'task_title': task.get('title'),
            'issue_type': issue_type,
            'entity_id': task.get('entity_id'),
            'entity_type': task.get('entity_type'),
            'entity_name': task.get('mechanic_name'),
            'measurements_count': len(measurements),
            'monitoring_period': {
                'start': measurements[0]['measurement_date'],
                'end': measurements[-1]['measurement_date'],
                'duration_days': (datetime.fromisoformat(measurements[-1]['measurement_date']) - 
                                datetime.fromisoformat(measurements[0]['measurement_date'])).days
            },
            'overall_metrics': {
                'baseline_value': float(baseline['value']),
                'latest_value': float(latest['value']),
                'raw_change_pct': round(overall_change_pct, 2),
                'improvement_pct': round(improvement_pct, 2),  # Normalized for consistent interpretation
                'improved': improvement_pct > 0.0
            },
            'trend_analysis': trend,
            'significance_test': significance,
            'moving_average': {
                'value': round(moving_avg, 2) if moving_avg is not None else None,
                'raw_change_pct': round(recent_change, 2) if recent_change is not None else None,
                'improvement_pct': round(-recent_change if is_time_metric and recent_change is not None
                                       else recent_change if recent_change is not None else 0.0, 2)
            },
            'period_changes': period_changes,
            'status': 'summarized',
            'analyzed_at': datetime.now().isoformat()
        }
        
        print(f"ANALYZER: Completed analysis for task ID {task_id}")
        print(f"ANALYZER: Overall change: {improvement_pct:.2f}%, Trend: {trend['trend_description']}")
        
        return summary


# For testing this module directly
if __name__ == '__main__':
    import argparse
    import json
    from summary_data import SummaryDataCollector
    
    parser = argparse.ArgumentParser(description='Analyze task performance data')
    parser.add_argument('--task-id', help='ID of task to analyze')
    parser.add_argument('--data-file', help='Path to JSON file with task data')
    parser.add_argument('--output-file', help='Path to save analysis results')
    args = parser.parse_args()
    
    analyzer = SummaryAnalyzer()
    
    # Get task data
    task_data = None
    if args.task_id:
        collector = SummaryDataCollector()
        task_data = collector.collect_data_for_task(args.task_id)
    elif args.data_file and os.path.exists(args.data_file):
        with open(args.data_file) as f:
            task_data = json.load(f)
    
    if task_data:
        # Analyze the data
        analysis = analyzer.analyze_task_data(task_data)
        
        # Save or print results
        if args.output_file:
            with open(args.output_file, 'w') as f:
                json.dump(analysis, f, indent=2)
            print(f"Analysis saved to {args.output_file}")
        else:
            print("\nAnalysis Results:")
            print(json.dumps(analysis, indent=2))
    else:
        print("No task data to analyze. Use --task-id or --data-file.")