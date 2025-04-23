#!/usr/bin/env python3
import sys
import os
import json
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(current_dir, "../../../"))
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

# Load environment
load_dotenv(Path(__file__).resolve().parents[3] / ".env.local")

from shared_services.db_client import get_connection
from summary_writer import SummaryWriter

class TaskEvaluator:
    """
    Task Evaluator component
    Takes performance summaries and makes rule-based decisions on next steps
    """
    def __init__(self):
        self.today = datetime.now().date()
        print(f"EVALUATOR: Initializing for {self.today}")
        
        try:
            # Connect to the database
            self.supabase = get_connection()
            print("EVALUATOR: Connected to Supabase")
            
            # Initialize summary writer for retrieving summaries
            self.summary_writer = SummaryWriter()
        except Exception as e:
            print(f"EVALUATOR: Error initializing: {e}")
            self.supabase = None
            self.summary_writer = None
    
    def make_decision(self, summary):
        """
        Make a rule-based decision based on task summary metrics
        
        Args:
            summary: Performance summary from TaskSummary component
            
        Returns:
            dict: Decision with action, confidence, explanation, and recommendation
        """
        # Extract key metrics
        improvement_pct = summary.get('overall_metrics', {}).get('improvement_pct', 0)
        
        # Get trend information
        is_improving = summary.get('trend_analysis', {}).get('is_improving', False)
        is_significant = summary.get('significance_test', {}).get('is_significant', False)
        
        # Determine action based on rules
        if improvement_pct >= 15 and is_improving and is_significant:
            action = "close"
            confidence = "high"
            explanation = f"Performance has improved by {improvement_pct}% with statistical significance and shows a positive trend."
            recommendation = "Close this task as performance has met the improvement threshold with statistical confidence."
        elif improvement_pct >= 10 and is_improving:
            action = "close"
            confidence = "medium"
            explanation = f"Performance has improved by {improvement_pct}% and shows a positive trend, though not statistically significant."
            recommendation = "Close this task but schedule a follow-up review in 30 days to confirm sustained improvement."
        elif improvement_pct >= 5 and is_improving:
            action = "extend"
            confidence = "medium"
            explanation = f"Some improvement ({improvement_pct}%) but more monitoring needed to confirm trend."
            recommendation = "Extend monitoring for an additional period to confirm sustainable improvement."
        elif improvement_pct > 0:
            action = "review"
            confidence = "medium"
            explanation = f"Minimal improvement ({improvement_pct}%) requires manager review."
            recommendation = "Have a manager review the performance data and decide next steps."
        else:
            action = "intervene"
            confidence = "high"
            explanation = f"Performance is deteriorating by {abs(improvement_pct)}%."
            recommendation = "Immediate intervention required to address performance issues."
        
        return {
            'action': action,
            'confidence': confidence,
            'explanation': explanation,
            'recommendation': recommendation
        }
    
    def evaluate_summary(self, summary):
        """
        Evaluate a performance summary and make a decision
        
        Args:
            summary: Performance summary from TaskSummary component
            
        Returns:
            dict: Evaluation with task_id, summary_id, and decision
        """
        task_id = summary.get('task_id')
        summary_id = summary.get('summary_id')
        print(f"EVALUATOR: Evaluating summary {summary_id} for task {task_id}")
        
        # If not enough data, return a default recommendation
        if summary.get('status') == 'insufficient_data':
            print(f"EVALUATOR: Insufficient data for task ID {task_id}")
            return {
                'task_id': task_id,
                'summary_id': summary_id,
                'decision': {
                    'action': 'review',
                    'confidence': 'low',
                    'explanation': 'Not enough measurements to make a data-driven decision',
                    'recommendation': 'Manual review required due to limited data'
                }
            }
        
        # Make rule-based decision
        decision = self.make_decision(summary)
        
        # Create the evaluation result
        evaluation = {
            'task_id': task_id,
            'summary_id': summary_id,
            'decision': decision
        }
        
        print(f"EVALUATOR: Recommended action for task {task_id}: {decision['action']} with {decision['confidence']} confidence")
        return evaluation
    
    def save_evaluation(self, evaluation):
        """
        Save evaluation to the database
        
        Args:
            evaluation: The evaluation result to save
            
        Returns:
            dict: The saved evaluation record or None if failed
        """
        if not self.supabase:
            print("EVALUATOR: No database connection available")
            return None
            
        try:
            # Prepare the evaluation record
            evaluation_record = {
                'task_id': evaluation['task_id'],
                'summary_id': evaluation['summary_id'],
                'decision': evaluation['decision']['action'],
                'confidence': evaluation['decision']['confidence'],
                'explanation': evaluation['decision']['explanation'],
                'recommendation': evaluation['decision']['recommendation'],
                'evaluation_date': self.today.isoformat()
            }
            
            # Insert the record
            result = self.supabase.table('task_evaluations').insert(evaluation_record).execute()
            
            if result.data:
                evaluation_id = result.data[0]['id']
                print(f"EVALUATOR: Saved evaluation to database with ID {evaluation_id}")
                evaluation['evaluation_id'] = evaluation_id
                return result.data[0]
            else:
                print(f"EVALUATOR: Failed to save evaluation to database")
                return None
                
        except Exception as e:
            print(f"EVALUATOR: Error saving evaluation to database: {e}")
            return None
    
    def evaluate_summary_by_id(self, summary_id):
        """
        Evaluate a summary by its ID
        
        Args:
            summary_id: ID of the summary to evaluate
            
        Returns:
            dict: The evaluation result or None if failed
        """
        if not self.summary_writer:
            print("EVALUATOR: Summary writer not available")
            return None
            
        # Get the summary from the database
        summary = self.summary_writer.get_summary_by_id(summary_id)
        if not summary:
            print(f"EVALUATOR: No summary found with ID {summary_id}")
            return None
            
        # Extract the metrics_json field and parse it
        metrics_json = summary.get('metrics_json')
        if not metrics_json:
            print(f"EVALUATOR: No metrics data in summary {summary_id}")
            return None
            
        try:
            metrics = json.loads(metrics_json)
            metrics['summary_id'] = summary['id']
            
            # Evaluate the summary
            evaluation = self.evaluate_summary(metrics)
            
            # Save the evaluation
            saved_evaluation = self.save_evaluation(evaluation)
            
            if saved_evaluation:
                # Add the ID to the evaluation
                evaluation['evaluation_id'] = saved_evaluation['id']
                return evaluation
            else:
                return evaluation
                
        except json.JSONDecodeError as e:
            print(f"EVALUATOR: Error parsing metrics JSON: {e}")
            return None
        except Exception as e:
            print(f"EVALUATOR: Error evaluating summary: {e}")
            return None
    
    def find_and_evaluate_summaries(self):
        """
        Find summaries that need evaluation and evaluate them
        
        Returns:
            list: List of evaluation results
        """
        if not self.supabase:
            print("EVALUATOR: No database connection available")
            return []
            
        try:
            # Find summaries without evaluations
            # This query finds summaries that don't have corresponding entries in task_evaluations
            query = """
            SELECT s.id, s.task_id, s.metrics_json
            FROM task_summaries s
            LEFT JOIN task_evaluations e ON s.id = e.summary_id
            WHERE e.id IS NULL
            """
            
            result = self.supabase.rpc('find_unevaluated_summaries').execute()
            
            if not result.data:
                print("EVALUATOR: No unevaluated summaries found")
                return []
                
            print(f"EVALUATOR: Found {len(result.data)} unevaluated summaries")
            
            # Evaluate each summary
            evaluations = []
            for summary_record in result.data:
                summary_id = summary_record['id']
                evaluation = self.evaluate_summary_by_id(summary_id)
                if evaluation:
                    evaluations.append(evaluation)
            
            return evaluations
                
        except Exception as e:
            print(f"EVALUATOR: Error finding unevaluated summaries: {e}")
            return []
        

# For testing this module directly
if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Evaluate task performance summaries')
    parser.add_argument('--summary-id', help='ID of summary to evaluate')
    parser.add_argument('--summary-file', help='Path to JSON file with summary')
    parser.add_argument('--find-unevaluated', action='store_true', help='Find and evaluate all unevaluated summaries')
    parser.add_argument('--output-file', help='Output file for evaluation results')
    args = parser.parse_args()
    
    evaluator = TaskEvaluator()
    
    evaluations = []
    
    if args.summary_id:
        evaluation = evaluator.evaluate_summary_by_id(args.summary_id)
        if evaluation:
            evaluations.append(evaluation)
            print(f"Evaluation completed for summary {args.summary_id}")
            print(f"Decision: {evaluation['decision']['action']} ({evaluation['decision']['confidence']})")
            print(f"Explanation: {evaluation['decision']['explanation']}")
            print(f"Recommendation: {evaluation['decision']['recommendation']}")
    
    elif args.summary_file and os.path.exists(args.summary_file):
        with open(args.summary_file) as f:
            summary = json.load(f)
            
        evaluation = evaluator.evaluate_summary(summary)
        if evaluation:
            saved_evaluation = evaluator.save_evaluation(evaluation)
            if saved_evaluation:
                evaluation['evaluation_id'] = saved_evaluation['id']
            evaluations.append(evaluation)
            print(f"Evaluation completed for summary from file")
            print(f"Decision: {evaluation['decision']['action']} ({evaluation['decision']['confidence']})")
    
    elif args.find_unevaluated:
        evaluations = evaluator.find_and_evaluate_summaries()
        print(f"Evaluated {len(evaluations)} summaries")
    
    # Save evaluations to file if requested
    if evaluations and args.output_file:
        with open(args.output_file, 'w') as f:
            json.dump(evaluations, f, indent=2)
        print(f"Saved evaluations to {args.output_file}")
    
    if not (args.summary_id or args.summary_file or args.find_unevaluated):
        print("No action specified. Use --summary-id, --summary-file, or --find-unevaluated.")