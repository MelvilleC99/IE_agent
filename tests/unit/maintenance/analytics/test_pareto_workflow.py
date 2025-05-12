import os
import sys
import logging
import time
import traceback
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union
import unittest
from unittest.mock import Mock, patch

# Ensure src/ directory is on sys.path for absolute imports
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "../../.."))
src_root = os.path.join(project_root, "src")
if src_root not in sys.path:
    sys.path.insert(0, src_root)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Import modules
try:
    # Pareto analysis
    from agents.maintenance.analytics.pareto.pareto_analyser import run_analysis
    
    # Pareto interpreter
    from agents.maintenance.analytics.pareto.pareto_interpreter import interpret_findings
    
    # Pareto summary
    from agents.maintenance.analytics.pareto.pareto_summary import generate_summary
    
    # Pareto writer
    from agents.maintenance.analytics.pareto.pareto_writer import ParetoWriter
    
    # Database client
    from shared_services.supabase_client import SupabaseClient
    
    logger.info("Successfully imported all required modules")
except ImportError as e:
    logger.error(f"Import error: {e}")
    logger.error(traceback.format_exc())
    sys.exit(1)

class ParetoAnalysisWorkflow:
    """
    Pareto Analysis Workflow for maintenance data.
    Identifies the most significant factors contributing to downtime and creates findings.
    Specializes in detecting:
    1. Machines with highest downtime
    2. Most common failure reasons
    3. Production lines with most issues
    4. Product categories with highest impact
    5. Fabric types with highest impact
    6. Supervisors associated with most downtime
    """
    
    def __init__(self):
        """Initialize the Pareto analysis workflow"""
        try:
            self.db_client = SupabaseClient()
            logger.info("ParetoAnalysisWorkflow initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing ParetoAnalysisWorkflow: {e}")
            logger.error(traceback.format_exc())
            raise
    
    def fetch_maintenance_data(self, start_date=None, end_date=None) -> List[Dict]:
        """
        Fetch maintenance data directly from database with optional date filtering.
        
        Args:
            start_date: Start date for filtering (optional)
            end_date: End date for filtering (optional)
            
        Returns:
            List of maintenance record dictionaries
        """
        try:
            logger.info(f"Fetching maintenance data from {start_date} to {end_date}")
            
            # Build filters
            filters = []
            if start_date:
                if isinstance(start_date, str):
                    start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
                filters.append({"column": "created_at", "operator": "gte", "value": start_date.isoformat()})
                
            if end_date:
                if isinstance(end_date, str):
                    end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
                filters.append({"column": "created_at", "operator": "lte", "value": end_date.isoformat()})
            
            # Query the database directly
            records = self.db_client.query_table(
                table_name="downtime_detail",
                columns="*",
                filters=filters if filters else None,
                limit=1000  # Adjust as needed
            )
            
            logger.info(f"Retrieved {len(records)} maintenance records from database")
            return records
            
        except Exception as e:
            logger.error(f"Error fetching maintenance data: {e}")
            logger.error(traceback.format_exc())
            return []
    
    def clean_data(self, records: List[Dict]) -> List[Dict]:
        """
        Clean and prepare maintenance data for analysis
        
        Args:
            records: Raw maintenance records from database
            
        Returns:
            List of cleaned maintenance records
        """
        try:
            # Convert to pandas DataFrame for easier cleaning
            df = pd.DataFrame(records)
            
            if df.empty:
                logger.warning("No records to clean")
                return []
                
            logger.info(f"Cleaning {len(df)} maintenance records")
            
            # Handle missing values
            for column in ['total_downtime', 'total_repair_time', 'total_response_time']:
                if column in df.columns:
                    df[column] = pd.to_numeric(df[column], errors='coerce')
            
            # Drop rows with missing critical values
            critical_columns = ['machine_number', 'total_downtime']
            df = df.dropna(subset=critical_columns)
            
            # Convert timestamps to datetime objects
            if 'created_at' in df.columns:
                df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
                
            if 'resolved_at' in df.columns:
                df['resolved_at'] = pd.to_datetime(df['resolved_at'], errors='coerce')
            
            # Fill missing values with defaults
            if 'reason' in df.columns:
                df['reason'] = df['reason'].fillna('Unknown')
                
            fill_defaults = {
                'product_category': 'Unknown',
                'fabric_type': 'Unknown',
                'machine_type': 'Unknown',
                'production_line_name': 'Unknown',
                'supervisor_name': 'Unknown'
            }
            
            for col, default in fill_defaults.items():
                if col in df.columns:
                    df[col] = df[col].fillna(default)
            
            logger.info(f"Cleaning complete. {len(df)} records remain after cleaning")
            
            # Convert back to records
            return df.to_dict('records')
            
        except Exception as e:
            logger.error(f"Error cleaning data: {e}")
            logger.error(traceback.format_exc())
            return records  # Return original records if cleaning fails
    
    def run(self, start_date=None, end_date=None, dimensions=None, threshold=80.0, 
            skip_database=False, run_by="system") -> Dict[str, Any]:
        """
        Run the complete Pareto analysis workflow with direct database access.
        
        Args:
            start_date: Start date for analysis period (optional)
            end_date: End date for analysis period (optional)
            dimensions: List of dimensions to analyze (optional)
            threshold: Cumulative percentage threshold (default: 80%)
            skip_database: Whether to skip saving results to database
            run_by: Identifier for who/what initiated the analysis
            
        Returns:
            Dictionary with workflow results and summary
        """
        start_time = time.time()
        
        result_summary = {
            'pareto_analysis_success': False,
            'findings_count': 0,
            'errors': []
        }
        
        try:
            # 1. Fetch data directly from database with date filtering
            records = self.fetch_maintenance_data(start_date, end_date)
            
            if not records:
                msg = "No maintenance records found in database for the specified period"
                logger.warning(msg)
                result_summary['errors'].append(msg)
                return result_summary
            
            # 2. Clean and prepare the data
            cleaned_records = self.clean_data(records)
            
            if not cleaned_records:
                msg = "No valid records after data cleaning"
                logger.warning(msg)
                result_summary['errors'].append(msg)
                return result_summary
                
            # 3. Run Pareto analysis on the database records
            logger.info(f"Running Pareto analysis with {threshold}% threshold...")
            analysis_results = run_analysis(cleaned_records, dimensions, metric='total_downtime', threshold=threshold)
            
            # Check for analysis errors
            if "error" in analysis_results:
                msg = f"Error in Pareto analysis: {analysis_results['error']}"
                logger.error(msg)
                result_summary['errors'].append(msg)
                return result_summary
            
            # Log successful analysis
            result_summary['pareto_analysis_success'] = True
            
            # 4. Interpret findings
            logger.info("Interpreting Pareto analysis findings...")
            interpreted_findings = interpret_findings(analysis_results)
            
            # Check for interpretation errors
            if "error" in interpreted_findings:
                msg = f"Error interpreting findings: {interpreted_findings['error']}"
                logger.error(msg)
                result_summary['errors'].append(msg)
                return result_summary
            
            # Count findings
            findings_count = 0
            for dimension, dim_findings in interpreted_findings.get('interpreted_findings', {}).items():
                if 'findings' in dim_findings and dimension != 'cross_dimensional':
                    findings_count += len(dim_findings['findings'])
            
            result_summary['findings_count'] = findings_count
            
            # 5. Generate summary
            logger.info("Generating summary...")
            summary = generate_summary(interpreted_findings, period_start=start_date, period_end=end_date)
            
            # Print summary to terminal
            print("\n" + summary)
            
            # Add to result
            result_summary['summary'] = summary
            
            # 6. Save to database if not skipped
            if not skip_database:
                logger.info("Saving analysis results to database...")
                writer = ParetoWriter(self.db_client)
                
                # Add run duration to analysis results
                run_duration = int((time.time() - start_time) * 1000)  # Convert to milliseconds
                analysis_results['run_duration'] = run_duration
                
                analysis_id = writer.save_analysis(
                    analysis_results=analysis_results,
                    interpreted_findings=interpreted_findings,
                    summary_text=summary,
                    period_start=start_date,
                    period_end=end_date,
                    run_by=run_by
                )
                
                result_summary['analysis_id'] = analysis_id
                logger.info(f"Analysis saved with ID: {analysis_id}")
            
            logger.info("Pareto analysis workflow completed successfully")
            return result_summary
            
        except Exception as e:
            error_msg = f"Error in workflow execution: {str(e)}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            result_summary['errors'].append(error_msg)
            return result_summary

class TestParetoWorkflow(unittest.TestCase):
    """Test cases for the Pareto Analysis Workflow"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.workflow = ParetoAnalysisWorkflow()
        self.mock_db_client = Mock()
        self.workflow.db_client = self.mock_db_client
        
        # Sample test data
        self.sample_records = [
            {
                'machine_number': 'M001',
                'total_downtime': 120,
                'reason': 'Mechanical Failure',
                'product_category': 'Category A',
                'fabric_type': 'Type X',
                'machine_type': 'Type 1',
                'production_line_name': 'Line 1',
                'supervisor_name': 'John Doe',
                'created_at': datetime.now().isoformat(),
                'resolved_at': (datetime.now() + timedelta(hours=2)).isoformat()
            },
            {
                'machine_number': 'M002',
                'total_downtime': 90,
                'reason': 'Electrical Issue',
                'product_category': 'Category B',
                'fabric_type': 'Type Y',
                'machine_type': 'Type 2',
                'production_line_name': 'Line 2',
                'supervisor_name': 'Jane Smith',
                'created_at': datetime.now().isoformat(),
                'resolved_at': (datetime.now() + timedelta(hours=1.5)).isoformat()
            }
        ]
    
    def test_fetch_maintenance_data(self):
        """Test fetching maintenance data from database"""
        # Mock the database query
        self.mock_db_client.query_table.return_value = self.sample_records
        
        # Test with date range
        start_date = datetime.now() - timedelta(days=7)
        end_date = datetime.now()
        records = self.workflow.fetch_maintenance_data(start_date, end_date)
        
        # Verify the query was called with correct parameters
        self.mock_db_client.query_table.assert_called_once()
        call_args = self.mock_db_client.query_table.call_args[1]
        self.assertEqual(call_args['table_name'], 'downtime_detail')
        self.assertEqual(call_args['columns'], '*')
        self.assertEqual(len(call_args['filters']), 2)  # Start and end date filters
        
        # Verify the returned records
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]['machine_number'], 'M001')
    
    def test_clean_data(self):
        """Test data cleaning functionality"""
        # Add some problematic data
        test_records = self.sample_records.copy()
        test_records.append({
            'machine_number': None,  # Missing critical value
            'total_downtime': 'invalid',  # Invalid number
            'reason': None,  # Missing value
            'created_at': 'invalid_date'  # Invalid date
        })
        
        cleaned_records = self.workflow.clean_data(test_records)
        
        # Verify cleaning results
        self.assertEqual(len(cleaned_records), 2)  # Should remove the invalid record
        self.assertIsInstance(cleaned_records[0]['total_downtime'], (int, float))
        self.assertIsInstance(cleaned_records[0]['created_at'], str)
    
    @patch('agents.maintenance.analytics.pareto.pareto_workflow.run_analysis')
    @patch('agents.maintenance.analytics.pareto.pareto_workflow.interpret_findings')
    @patch('agents.maintenance.analytics.pareto.pareto_workflow.generate_summary')
    def test_run_workflow(self, mock_generate_summary, mock_interpret_findings, mock_run_analysis):
        """Test the complete workflow execution"""
        # Mock the database query
        self.mock_db_client.query_table.return_value = self.sample_records
        
        # Mock the analysis results
        mock_run_analysis.return_value = {
            'dimensions': {
                'machine_number': {
                    'data': pd.DataFrame({
                        'category': ['M001', 'M002'],
                        'value': [120, 90],
                        'percentage': [57.14, 42.86],
                        'cumulative_percentage': [57.14, 100.0]
                    })
                }
            }
        }
        
        # Mock the interpreted findings
        mock_interpret_findings.return_value = {
            'interpreted_findings': {
                'machine_number': {
                    'findings': [
                        {
                            'type': 'high_downtime',
                            'description': 'Machine M001 has highest downtime'
                        }
                    ]
                }
            }
        }
        
        # Mock the summary
        mock_generate_summary.return_value = "Test Summary"
        
        # Run the workflow
        result = self.workflow.run(
            start_date=datetime.now() - timedelta(days=7),
            end_date=datetime.now(),
            dimensions=['machine_number'],
            threshold=80.0
        )
        
        # Verify the results
        self.assertTrue(result['pareto_analysis_success'])
        self.assertEqual(result['findings_count'], 1)
        self.assertEqual(result['summary'], "Test Summary")
        self.assertIn('analysis_id', result)
    
    def test_run_workflow_no_data(self):
        """Test workflow behavior when no data is available"""
        # Mock empty database query
        self.mock_db_client.query_table.return_value = []
        
        result = self.workflow.run()
        
        # Verify error handling
        self.assertFalse(result['pareto_analysis_success'])
        self.assertEqual(result['findings_count'], 0)
        self.assertTrue(len(result['errors']) > 0)
    
    def test_run_workflow_invalid_data(self):
        """Test workflow behavior with invalid data"""
        # Mock database query with invalid data
        self.mock_db_client.query_table.return_value = [
            {'invalid': 'data'}  # Missing required fields
        ]
        
        result = self.workflow.run()
        
        # Verify error handling
        self.assertFalse(result['pareto_analysis_success'])
        self.assertEqual(result['findings_count'], 0)
        self.assertTrue(len(result['errors']) > 0)

def main():
    """Main entry point for running the Pareto analysis workflow from command line"""
    logger.info("Starting Pareto Analysis Workflow")
    
    import argparse
    parser = argparse.ArgumentParser(description="Run Pareto Analysis Workflow")
    parser.add_argument("--start_date", type=str, help="Start date for analysis (YYYY-MM-DD)")
    parser.add_argument("--end_date", type=str, help="End date for analysis (YYYY-MM-DD)")
    parser.add_argument("--threshold", type=float, default=80.0, help="Pareto threshold percentage (default: 80.0)")
    parser.add_argument("--dimensions", type=str, help="Comma-separated list of dimensions to analyze")
    parser.add_argument("--no_save", action="store_true", help="Skip saving results to database")
    parser.add_argument("--run_by", type=str, default="cli", help="Identifier for who/what initiated the analysis")
    args = parser.parse_args()
    
    # Process arguments
    start_date = args.start_date
    end_date = args.end_date
    threshold = args.threshold
    skip_database = args.no_save
    run_by = args.run_by
    
    dimensions = None
    if args.dimensions:
        dimensions = [dim.strip() for dim in args.dimensions.split(',')]
    
    try:
        # Print analysis parameters
        print("\n=== Pareto Analysis Parameters ===")
        print(f"Date range: {start_date or 'All time'} to {end_date or 'Present'}")
        print(f"Threshold: {threshold}%")
        print(f"Dimensions: {', '.join(dimensions) if dimensions else 'All available'}")
        print(f"Save to database: {not skip_database}")
        print("===================================\n")
        
        # Run the workflow
        wf = ParetoAnalysisWorkflow()
        result = wf.run(
            start_date=start_date, 
            end_date=end_date, 
            dimensions=dimensions, 
            threshold=threshold,
            skip_database=skip_database,
            run_by=run_by
        )
        
        # Print summary of execution
        print("\n=== Pareto Analysis Execution Summary ===")
        print(f"Analysis status: {'Success' if result.get('pareto_analysis_success') else 'Failed'}")
        print(f"Findings generated: {result.get('findings_count', 0)}")
        
        if not skip_database and 'analysis_id' in result:
            print(f"Analysis saved to database with ID: {result['analysis_id']}")
        
        if result.get('errors'):
            print("\nErrors encountered:")
            for error in result['errors']:
                print(f"- {error}")
        
        print("\nWorkflow execution complete.")
        return result
        
    except Exception as e:
        logger.error(f"Unhandled exception in main workflow: {e}")
        logger.error(traceback.format_exc())
        print(f"\nError running workflow: {e}")
        return {'status': 'failed', 'error': str(e)}

if __name__ == '__main__':
    unittest.main()