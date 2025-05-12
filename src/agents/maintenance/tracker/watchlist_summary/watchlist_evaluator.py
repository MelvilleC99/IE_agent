#!/usr/bin/env python3
import sys
import os
import json
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import logging

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent.parent.parent
import sys
sys.path.append(str(project_root))

# Load environment
load_dotenv(Path(__file__).resolve().parents[3] / ".env.local")

from shared_services.db_client import get_connection
from src.agents.maintenance.tracker.task_summary.summary_writer import SummaryWriter
from src.agents.maintenance.tracker.task_summary.watchlist_data import WatchlistDataCollector

class WatchlistEvaluator:
    """
    Watchlist Evaluator - Evaluates watchlist summaries and makes decisions about next steps.
    
    This class is responsible for:
    1. Finding watchlist summaries that need evaluation
    2. Evaluating the performance data
    3. Making decisions about whether to extend monitoring or close the watchlist
    4. Recording evaluation results
    """
    
    def __init__(self):
        """Initialize the watchlist evaluator."""
        self.data_collector = WatchlistDataCollector()
        self.logger = logging.getLogger(__name__)
    
    def find_and_evaluate_summaries(self):
        """
        Find watchlist summaries that need evaluation and evaluate them.
        
        Returns:
            list: List of evaluation results
        """
        try:
            # Get watchlist items marked for evaluation
            items = self.data_collector.get_watchlist_items_marked_for_evaluation()
            if not items:
                self.logger.info("No watchlist items found for evaluation")
                return []
            
            evaluations = []
            for item in items:
                try:
                    # Get the summary file path
                    summary_path = self._get_summary_path(item['watch_id'])
                    if not os.path.exists(summary_path):
                        self.logger.warning(f"Summary file not found for watchlist {item['watch_id']}")
                        continue
                    
                    # Read and evaluate the summary
                    with open(summary_path, 'r') as f:
                        summary_data = json.load(f)
                    
                    evaluation = self._evaluate_summary(item, summary_data)
                    if evaluation:
                        evaluations.append(evaluation)
                        
                except Exception as e:
                    self.logger.error(f"Error evaluating watchlist {item['watch_id']}: {str(e)}")
                    continue
            
            return evaluations
            
        except Exception as e:
            self.logger.error(f"Error in find_and_evaluate_summaries: {str(e)}")
            return []
    
    def _evaluate_summary(self, item, summary_data):
        """
        Evaluate a watchlist summary and make a decision.
        
        Args:
            item (dict): Watchlist item data
            summary_data (dict): Summary data to evaluate
            
        Returns:
            dict: Evaluation result
        """
        try:
            # Extract key metrics
            metrics = summary_data.get('metrics', {})
            trend = metrics.get('trend', 'unknown')
            stability = metrics.get('stability', 'unknown')
            performance = metrics.get('performance', 'unknown')
            
            # Make decision based on metrics
            decision = self._make_decision(trend, stability, performance)
            
            # Record evaluation
            evaluation = {
                'watch_id': item['watch_id'],
                'evaluated_at': datetime.now().isoformat(),
                'trend': trend,
                'stability': stability,
                'performance': performance,
                'decision': decision,
                'notes': self._generate_evaluation_notes(trend, stability, performance, decision)
            }
            
            # Save evaluation
            self._save_evaluation(evaluation)
            
            return evaluation
            
        except Exception as e:
            self.logger.error(f"Error in _evaluate_summary: {str(e)}")
            return None
    
    def _make_decision(self, trend, stability, performance):
        """
        Make a decision based on the evaluation metrics.
        
        Args:
            trend (str): Trend analysis result
            stability (str): Stability analysis result
            performance (str): Performance analysis result
            
        Returns:
            str: Decision ('extend' or 'close')
        """
        # If any metric indicates improvement, extend monitoring
        if any(metric == 'improving' for metric in [trend, stability, performance]):
            return 'extend'
            
        # If all metrics indicate stable or declining, close the watchlist
        if all(metric in ['stable', 'declining'] for metric in [trend, stability, performance]):
            return 'close'
            
        # Default to extending if uncertain
        return 'extend'
    
    def _generate_evaluation_notes(self, trend, stability, performance, decision):
        """
        Generate notes explaining the evaluation decision.
        
        Args:
            trend (str): Trend analysis result
            stability (str): Stability analysis result
            performance (str): Performance analysis result
            decision (str): Decision made
            
        Returns:
            str: Evaluation notes
        """
        notes = []
        
        # Add trend analysis
        notes.append(f"Trend Analysis: {trend}")
        
        # Add stability analysis
        notes.append(f"Stability Analysis: {stability}")
        
        # Add performance analysis
        notes.append(f"Performance Analysis: {performance}")
        
        # Add decision explanation
        if decision == 'extend':
            notes.append("Decision: Extend monitoring period to gather more data")
        else:
            notes.append("Decision: Close watchlist as metrics indicate stable or declining performance")
        
        return "\n".join(notes)
    
    def _save_evaluation(self, evaluation):
        """
        Save the evaluation result to the database.
        
        Args:
            evaluation (dict): Evaluation data to save
        """
        try:
            result = self.data_collector.supabase.table('watchlist_evaluations').insert(evaluation).execute()
            if hasattr(result, 'error') and result.error:
                raise Exception(f"Error saving evaluation: {result.error}")
                
        except Exception as e:
            self.logger.error(f"Error saving evaluation: {str(e)}")
            raise
    
    def _get_summary_path(self, watch_id):
        """
        Get the path to a watchlist summary file.
        
        Args:
            watch_id (str): Watchlist ID
            
        Returns:
            str: Path to summary file
        """
        return os.path.join(
            os.path.dirname(__file__),
            'summaries',
            f'watchlist_{watch_id}_summary.json'
        )

    def get_evaluation_details(self, evaluation_id):
        """
        Get details for an evaluation.
        
        Args:
            evaluation_id: ID of the evaluation to retrieve
            
        Returns:
            dict: Evaluation details or None if not found
        """
        try:
            result = self.data_collector.supabase.table('watchlist_evaluations').select('*').eq('id', evaluation_id).execute()
            if result.data:
                return result.data[0]
            self.logger.warning(f"No evaluation found with ID {evaluation_id}")
            return None
        except Exception as e:
            self.logger.error(f"Error getting evaluation details: {str(e)}")
            return None

# For testing this module directly
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Watchlist Evaluator")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    
    evaluator = WatchlistEvaluator()
    evaluations = evaluator.find_and_evaluate_summaries()
    print(f"Found {len(evaluations)} evaluations")