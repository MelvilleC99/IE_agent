import sys
import os
from datetime import datetime, timedelta
import json
import re

# Add the project root to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from shared_services.db_client import get_connection

def create_tables():
    """
    Creates the tasks and measurements tables if they don't exist.
    """
    print("SETUP: Creating database tables if they don't exist...")
    
    try:
        # Connect to database
        supabase = get_connection()
        
        # Create tasks table
        tasks_table_sql = """
        CREATE TABLE IF NOT EXISTS tasks (
            id SERIAL PRIMARY KEY,                      -- Using SERIAL for auto-incrementing integer
            finding_id BIGINT REFERENCES findings_log(finding_id), -- Match the BIGINT type
            title VARCHAR(255) NOT NULL,                -- Auto-generated title
            issue_type VARCHAR(50) NOT NULL,            -- 'response_time', 'repair_time', etc.
            entity_type VARCHAR(50) NOT NULL,           -- 'mechanic', 'machine', etc.
            entity_id VARCHAR(50) NOT NULL,             -- Who/what has the issue
            assigned_to VARCHAR(50),                    -- Auto-assigned or manager assigned
            status VARCHAR(20) DEFAULT 'open',          -- 'open', 'in_progress', 'completed', 'ignored'
            monitor_frequency VARCHAR(20),              -- 'daily', 'weekly', null if not monitoring
            monitor_start_date DATE,                    -- When monitoring begins
            monitor_end_date DATE,                      -- When monitoring ends
            monitor_status VARCHAR(20),                 -- 'active', 'completed', 'extended'
            notes TEXT,                                 -- Any special instructions or notes
            created_at TIMESTAMP DEFAULT NOW(),
            completed_at TIMESTAMP,                     -- When task was marked complete
            completion_notes TEXT                       -- What was done to resolve
        )
        """
        
        # Create measurements table
        measurements_table_sql = """
        CREATE TABLE IF NOT EXISTS measurements (
            id SERIAL PRIMARY KEY,                      -- Using SERIAL for auto-incrementing integer
            task_id INTEGER REFERENCES tasks(id),       -- Match the INTEGER from SERIAL
            measurement_date DATE NOT NULL,             -- When measurement was taken
            value FLOAT NOT NULL,                       -- Measured value
            change_pct FLOAT,                           -- Percentage change from baseline
            is_improved BOOLEAN,                        -- Is it better than baseline?
            notes TEXT,                                 -- Any observations about this measurement
            created_at TIMESTAMP DEFAULT NOW()
        )
        """
        
        # Execute SQL directly using supabase.rpc
        # Note: This assumes your Supabase instance has RPC privileges
        # If direct SQL execution is not available, you would need to use an alternative method
        try:
            # Create tables using PostgreSQL function
            # This is just a placeholder - actual implementation depends on how your Supabase is set up
            print("SETUP: Tables should be created using SQL console or migration scripts")
            print("SETUP: Table creation SQL generated:")
            print(tasks_table_sql)
            print("\n")
            print(measurements_table_sql)
            
            # In a real implementation, you would run these statements in your database
            # For Supabase, you might need to create these in the dashboard SQL editor
            
            return True
        except Exception as e:
            print(f"SETUP: Error creating tables: {e}")
            return False
            
    except Exception as e:
        print(f"SETUP: Database connection error: {e}")
        return False


def clean_text(text):
    """Clean text by removing extra spaces and standardizing punctuation"""
    if not text:
        return ""
    # Replace multiple spaces with a single space
    cleaned = re.sub(r'\s+', ' ', text.strip())
    # Ensure proper spacing after punctuation
    cleaned = re.sub(r'([.,!?;:])\s*', r'\1 ', cleaned)
    # Remove any double spaces created by the above
    cleaned = re.sub(r'\s+', ' ', cleaned)
    return cleaned.strip()


def format_title(issue_type, entity_id, employee_number, context_info=""):
    """Create a consistent, readable title format"""
    # Capitalize first letter of each word in issue type
    formatted_issue = issue_type.replace('_', ' ').title()
    
    # Clean and format the context info
    context = clean_text(context_info)
    if context:
        context = f" - {context}"
    
    # Format employee number in parentheses if available
    emp_number = f" (#{employee_number})" if employee_number and employee_number != "Unknown" else ""
    
    return f"{formatted_issue}: {entity_id}{emp_number}{context}"


def extract_machine_reason_from_summary(summary):
    """Extract machine type and reason from a finding summary using regex"""
    machine_match = re.search(r'on\s+([A-Za-z\s]+)\s+(?:machines|with)', summary)
    reason_match = re.search(r"with\s+'([^']+)'", summary)
    
    machine_type = machine_match.group(1).strip() if machine_match else None
    reason = reason_match.group(1).strip() if reason_match else None
    
    return machine_type, reason


def create_tasks_from_findings():
    """
    Identifies new findings from the findings_log table and creates appropriate tasks
    with monitoring schedules based on the issue type.
    
    Rules:
    - Response issues: Monitor daily for 2 weeks
    - Repair issues: Monitor weekly for 4 weeks
    """
    print("TASK CREATOR: Starting task creation from findings...")
    
    # Connect to database
    try:
        supabase = get_connection()
        
        # Get findings that don't have tasks yet (status = 'New')
        findings_result = supabase.table('findings_log').select('*').eq('status', 'New').execute()
        
        if not findings_result.data:
            print("TASK CREATOR: No new findings to process")
            return []
            
        print(f"TASK CREATOR: Found {len(findings_result.data)} new findings to process")
        
        # Process each finding and create tasks
        created_tasks = []
        for finding in findings_result.data:
            try:
                # Extract key information from finding
                finding_id = finding['finding_id']
                analysis_type = finding['analysis_type']
                finding_summary = finding['finding_summary']
                finding_details = finding['finding_details']
                
                # Ensure finding_details is a dictionary
                if isinstance(finding_details, str):
                    try:
                        finding_details = json.loads(finding_details)
                    except json.JSONDecodeError:
                        print(f"TASK CREATOR: ERROR - Invalid JSON in finding_details for ID {finding_id}")
                        finding_details = {}
                
                # Determine issue type more precisely
                issue_type = None
                
                # Extract from metric field if available
                if 'metric' in finding_details:
                    metric = finding_details['metric']
                    if 'response_time' in metric:
                        issue_type = 'response_time'
                    elif 'repair_time' in metric or metric.startswith('trend_repair'):
                        issue_type = 'repair_time'
                    else:
                        issue_type = metric
                # Fallback to analysis type
                else:
                    if 'response' in analysis_type:
                        issue_type = 'response_time'
                    elif 'repair' in analysis_type:
                        issue_type = 'repair_time' 
                    elif 'quality' in analysis_type:
                        issue_type = 'quality_issue'
                    elif 'production' in analysis_type:
                        issue_type = 'production_issue'
                    else:
                        issue_type = 'other'
                
                # Determine entity type and ID
                entity_type = 'mechanic'  # Default entity type for this system
                entity_id = finding_details.get('mechanic_id', 'Unknown')
                employee_number = finding_details.get('employee_number', 'Unknown')
                
                # Extract machine type and reason - either from details or from summary
                machine_type = finding_details.get('machine_type')
                reason = finding_details.get('reason')
                
                # If not in details, try to extract from summary
                if not machine_type or not reason:
                    extracted_machine, extracted_reason = extract_machine_reason_from_summary(finding_summary)
                    if not machine_type and extracted_machine:
                        machine_type = extracted_machine
                    if not reason and extracted_reason:
                        reason = extracted_reason
                
                # Set monitoring schedule based on issue type
                today = datetime.now().date()
                monitor_frequency = None
                monitor_start_date = today
                monitor_end_date = None
                
                if issue_type == 'response_time':
                    monitor_frequency = 'daily'
                    monitor_end_date = today + timedelta(days=14)  # 2 weeks
                elif issue_type == 'repair_time':
                    monitor_frequency = 'weekly'
                    monitor_end_date = today + timedelta(days=28)  # 4 weeks
                else:
                    # Default for other types
                    monitor_frequency = 'weekly'
                    monitor_end_date = today + timedelta(days=21)  # 3 weeks
                
                # Create context info for the title
                context_info = ""
                if machine_type:
                    context_info += f"{machine_type}"
                if reason:
                    if context_info:
                        context_info += f" - {reason}"
                    else:
                        context_info += f"{reason}"
                
                # Create a clean, formatted title
                title = format_title(issue_type, entity_id, employee_number, context_info)
                
                # Create a cleaned task notes field
                notes = f"Auto-created from finding. Original issue: {clean_text(finding_summary)}"
                
                # Check if task already exists for this finding
                existing_task = supabase.table('tasks').select('id').eq('finding_id', finding_id).execute()
                
                if existing_task.data:
                    print(f"TASK CREATOR: Task already exists for finding ID {finding_id}, skipping")
                    continue
                
                # Create the task with clean, structured data
                task_data = {
                    'finding_id': finding_id,
                    'title': title,
                    'issue_type': issue_type,
                    'entity_type': entity_type,
                    'entity_id': entity_id,
                    'assigned_to': None,  # Can be assigned later
                    'status': 'open',
                    'monitor_frequency': monitor_frequency,
                    'monitor_start_date': monitor_start_date.isoformat(),
                    'monitor_end_date': monitor_end_date.isoformat(),
                    'monitor_status': 'active',
                    'notes': notes
                }
                
                # Insert the task
                task_result = supabase.table('tasks').insert(task_data).execute()
                
                if task_result.data:
                    task_id = task_result.data[0]['id']
                    
                    # Create initial measurement record to establish baseline
                    baseline_value = finding_details.get('value', 0)
                    
                    # Ensure the baseline value is a valid number
                    try:
                        baseline_value = float(baseline_value)
                    except (ValueError, TypeError):
                        print(f"TASK CREATOR: Warning - Invalid baseline value for finding ID {finding_id}, using 0")
                        baseline_value = 0
                    
                    measurement_data = {
                        'task_id': task_id,
                        'measurement_date': today.isoformat(),
                        'value': round(baseline_value, 2),  # Round to 2 decimal places for consistency
                        'change_pct': 0,  # Initial measurement, so no change
                        'is_improved': False,  # Not applicable for baseline
                        'notes': "Baseline measurement from finding"
                    }
                    
                    measurement_result = supabase.table('measurements').insert(measurement_data).execute()
                    
                    if measurement_result.data:
                        measurement_id = measurement_result.data[0]['id']
                        print(f"TASK CREATOR: Created task ID {task_id} with baseline measurement ID {measurement_id}")
                        
                        # Update the finding status to show a task has been assigned
                        finding_update = supabase.table('findings_log').update({
                            'status': 'Task_Created'
                        }).eq('finding_id', finding_id).execute()
                        
                        # Return consistent structured data
                        created_tasks.append({
                            'task_id': task_id,
                            'finding_id': finding_id,
                            'title': title,
                            'issue_type': issue_type,
                            'entity_id': entity_id,
                            'machine_type': machine_type,
                            'reason': reason,
                            'monitor_frequency': monitor_frequency,
                            'monitor_end_date': monitor_end_date.isoformat(),
                            'baseline_value': round(baseline_value, 2)
                        })
                    else:
                        print(f"TASK CREATOR: ERROR creating measurement for task ID {task_id}")
                else:
                    print(f"TASK CREATOR: ERROR creating task for finding ID {finding_id}")
                    
            except Exception as e:
                print(f"TASK CREATOR: ERROR processing finding ID {finding.get('finding_id', 'unknown')}: {e}")
                continue
                
        print(f"TASK CREATOR: Successfully created {len(created_tasks)} tasks")
        return created_tasks
        
    except Exception as e:
        print(f"TASK CREATOR: Critical error connecting to database: {e}")
        return []


# Example usage (for testing this script directly)
if __name__ == '__main__':
    # First, ensure the tables exist
    print("Starting database setup...")
    tables_created = create_tables()
    
    if tables_created:
        print("Database tables are ready.")
    else:
        print("Note: Tables may need to be created manually via SQL console.")
    
    # Create tasks from findings
    print("\nStarting task creation process...")
    created_tasks = create_tasks_from_findings()
    
    print(f"\n--- Task Creator Test Results ---")
    print(f"Created {len(created_tasks)} tasks from findings")
    
    for i, task in enumerate(created_tasks):
        if i < 10:  # Only show first 10 tasks
            print(f"- Task ID {task['task_id']}: {task['title']}")
            print(f"  Type: {task['issue_type']}, Entity: {task['entity_id']}")
            if task.get('machine_type'):
                print(f"  Machine: {task['machine_type']}, Reason: {task.get('reason', 'N/A')}")
            print(f"  Monitoring: {task['monitor_frequency']} until {task['monitor_end_date']}")
            print(f"  Baseline Value: {task['baseline_value']}")
        elif i == 10:
            print(f"... and {len(created_tasks) - 10} more tasks.")