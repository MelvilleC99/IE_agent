#!/usr/bin/env python3
import sys
import os
from datetime import datetime
import numpy as np
from scipy import stats
import logging
from typing import List, Dict, Any, Optional, Union, Tuple

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(current_dir, "../../../"))
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

class SummaryAnalyzer:
    """
    Summary Analyzer - Analyzes watchlist performance data and generates summaries.
    
    This class is responsible for:
    1. Analyzing performance measurements
    2. Calculating trends and stability
    3. Generating performance summaries
    """
    
    def __init__(self):
        """Initialize the summary analyzer."""
        self.logger = logging.getLogger(__name__)
        self.today = datetime.now().date()
        print(f"ANALYZER: Initializing for {self.today}")
    
    def calculate_percentage_change(self, baseline_value: float, latest_value: float, issue_type: str) -> float:
        """
        Calculate percentage change between baseline and latest value.
        
        Args:
            baseline_value (float): Baseline value for comparison
            latest_value (float): Latest value to compare against baseline
            issue_type (str): Type of issue being analyzed
            
        Returns:
            float: Percentage change
        """
        if baseline_value == 0:
            return 0.0
            
        if issue_type == 'decrease':
            return ((baseline_value - latest_value) / baseline_value) * 100
        else:
            return ((latest_value - baseline_value) / baseline_value) * 100
    
    def analyze_watchlist_data(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Analyze watchlist performance data and generate a summary.
        
        Args:
            data (dict): Watchlist data including measurements and metadata
            
        Returns:
            dict: Analysis summary with metrics and recommendations
        """
        try:
            # Extract measurements
            measurements = data.get('measurements', [])
            if not measurements:
                self.logger.warning("No measurements found in data")
                return None
            
            # Calculate basic statistics
            values = [float(m.get('value', 0)) for m in measurements]
            if not values:
                self.logger.warning("No valid values found in measurements")
                return None
            
            # Convert to numpy arrays for calculations
            x = np.array(range(len(values)))
            y = np.array(values)
            
            # Calculate trend
            trend = self._calculate_trend(x, y)
            
            # Calculate stability
            stability = self._calculate_stability(y)
            
            # Calculate performance
            performance = self._calculate_performance(y, data.get('baseline'))
            
            # Generate summary
            summary = {
                'watchlist_id': data.get('watchlist', {}).get('id'),
                'analyzed_at': datetime.now().isoformat(),
                'metrics': {
                    'trend': trend,
                    'stability': stability,
                    'performance': performance
                },
                'statistics': {
                    'mean': float(np.mean(y)),
                    'min': float(np.min(y)),
                    'max': float(np.max(y)),
                    'count': len(values)
                },
                'notes': self._generate_analysis_notes(trend, stability, performance)
            }
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error analyzing watchlist data: {str(e)}")
            return None
    
    def _calculate_trend(self, x: np.ndarray, y: np.ndarray) -> str:
        """
        Calculate the trend of values using linear regression.
        
        Args:
            x (np.ndarray): Array of x values (indices)
            y (np.ndarray): Array of y values (measurements)
            
        Returns:
            str: Trend description ('improving', 'stable', 'declining')
        """
        if len(x) < 2:
            return 'unknown'
        
        # Calculate linear regression
        slope, _, _, _, _ = stats.linregress(x, y)
        
        # Determine trend based on slope
        if float(slope) > 0.1:  # Positive slope threshold
            return 'improving'
        elif float(slope) < -0.1:  # Negative slope threshold
            return 'declining'
        else:
            return 'stable'
    
    def _calculate_stability(self, values: np.ndarray) -> str:
        """
        Calculate the stability of values.
        
        Args:
            values (np.ndarray): Array of numeric values
            
        Returns:
            str: Stability description ('stable', 'unstable')
        """
        if len(values) < 2:
            return 'unknown'
        
        # Calculate coefficient of variation
        mean = np.mean(values)
        std_dev = np.std(values)
        cv = std_dev / mean if mean != 0 else float('inf')
        
        # Determine stability based on coefficient of variation
        if cv < 0.2:  # Low variation threshold
            return 'stable'
        else:
            return 'unstable'
    
    def _calculate_performance(self, values: np.ndarray, baseline: Optional[float]) -> str:
        """
        Calculate performance relative to baseline.
        
        Args:
            values (np.ndarray): Array of numeric values
            baseline (float): Baseline value for comparison
            
        Returns:
            str: Performance description ('improving', 'stable', 'declining')
        """
        if not baseline or len(values) == 0:
            return 'unknown'
        
        # Calculate average of recent values
        recent_values = values[-3:] if len(values) >= 3 else values
        avg_recent = float(np.mean(recent_values))
        
        # Compare with baseline
        if avg_recent > baseline * 1.1:  # 10% improvement threshold
            return 'improving'
        elif avg_recent < baseline * 0.9:  # 10% decline threshold
            return 'declining'
        else:
            return 'stable'
    
    def _generate_analysis_notes(self, trend: str, stability: str, performance: str) -> str:
        """
        Generate notes explaining the analysis results.
        
        Args:
            trend (str): Trend analysis result
            stability (str): Stability analysis result
            performance (str): Performance analysis result
            
        Returns:
            str: Analysis notes
        """
        notes = []
        
        # Add trend analysis
        notes.append(f"Trend Analysis: {trend}")
        
        # Add stability analysis
        notes.append(f"Stability Analysis: {stability}")
        
        # Add performance analysis
        notes.append(f"Performance Analysis: {performance}")
        
        # Add overall assessment
        if all(metric == 'improving' for metric in [trend, stability, performance]):
            notes.append("Overall Assessment: Strong positive performance with consistent improvement")
        elif all(metric == 'stable' for metric in [trend, stability, performance]):
            notes.append("Overall Assessment: Stable performance with no significant changes")
        elif all(metric == 'declining' for metric in [trend, stability, performance]):
            notes.append("Overall Assessment: Concerning decline in performance requiring attention")
        else:
            notes.append("Overall Assessment: Mixed performance indicators requiring monitoring")
        
        return "\n".join(notes)


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