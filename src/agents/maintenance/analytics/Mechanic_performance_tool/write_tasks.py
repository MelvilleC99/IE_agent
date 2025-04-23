#!/usr/bin/env python3
import sys
import os
import json
import re
from datetime import datetime, timedelta
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

class TaskWriter:
    """
    Handles creating tasks from findings and writing them to the database.
    """
    
    def __init__(self):
        """Initialize the task writer"""
        try:
            self.supabase = get_connection()
            print("TASK_WRITER: Connected to database successfully")
            self.today = datetime.now().date()
        except Exception as e:
            print(f"TASK_WRITER: Error connecting to database: {e}")
            self.supabase = None
            self.today = datetime.now().date()
    
    def clean_text(self, text):
        """Clean and normalize text"""
        if not text:
            return ""
        cleaned = re.sub(r'\s+', ' ', text.strip())
        cleaned = re.sub(r'([.,!?;:])\s*', r'\1 ', cleaned)
        return cleaned.strip()
    
    def format_title(self, issue_type, display_name, mech_id, context_info=""):
        """Format the task title with consistent structure"""
        formatted_issue = issue_type.replace('_', ' ').title()
        context = self.clean_text(context_info)
        if context:
            context = f" - {context}"
        emp_num = f" (#{mech_id})" if mech_id and mech_id != 'Unknown' else ''
        return f"{formatted_issue}: {display_name}{emp_num}{context}"
    
    def extract_mechanic_from_summary(self, summary):
        """
        Extract mechanic display name and numeric ID from summary:
        '...: Duncan J (#003) ...'
        Returns (name, id)
        """
        match = re.search(r":\s*(.*?)\s*\(#(\d+)\)", summary)
        if match:
            name = match.group(1).strip()
            mech_id = match.group(2)
            return name, mech_id
        return None, None
    
    def extract_machine_reason_from_summary(self, summary):
        """Extract machine type and reason from finding summary"""
        machine_match = re.search(r'on\s+([A-Za-z\s]+?)\s+(?:machines|with)', summary)
        reason_match = re.search(r"with\s+'([^']+)'", summary)
        machine = machine_match.group(1).strip() if machine_match else None
        reason = reason_match.group(1).strip() if reason_match else None
        return machine, reason
    
    def create_task_from_finding(self, finding):
        """
        Create a task record from a finding
        
        Args:
            finding: Dictionary with finding information
            
        Returns:
            dict: The created task record or None if creation failed
        """
        if not self.supabase:
            print("TASK_WRITER: No database connection")
            return None
            
        # Extract finding details
        finding_id = finding.get('finding_id')
        summary = finding.get('finding_summary', '')
        details = finding.get('finding_details', {})
        
        # Check if task already exists for this finding
        existing_check = self.supabase.table('tasks').select('id').eq('finding_id', finding_id).execute()
        if existing_check.data:
            print(f"TASK_WRITER: Task already exists for finding ID {finding_id}")
            return None
        
        # Extract mechanic info from summary
        display_name, mech_id = self.extract_mechanic_from_summary(summary)
        display_name = display_name or details.get('mechanic_id') or 'Unknown'
        mech_id = mech_id or details.get('employee_number') or 'Unknown'
        
        # Determine issue type
        analysis_type = finding.get('analysis_type', '')
        if 'response_time' in analysis_type:
            issue_type = 'response_time'
        elif 'repair_time' in analysis_type or 'machine_repair' in analysis_type:
            issue_type = 'repair_time'
        else:
            issue_type = analysis_type.split('_')[-1] if '_' in analysis_type else 'other'
        
        # Determine monitoring schedule
        if issue_type == 'response_time':
            freq, end = 'daily', self.today + timedelta(days=14)
        elif issue_type == 'repair_time':
            freq, end = 'weekly', self.today + timedelta(days=28)
        else:
            freq, end = 'weekly', self.today + timedelta(days=21)
        
        # Extract machine type and reason
        machine_type = details.get('machine_type')
        reason = details.get('reason')
        
        if not machine_type or not reason:
            m, r = self.extract_machine_reason_from_summary(summary)
            machine_type = machine_type or m
            reason = reason or r
        
        # Format title and notes
        context = ''
        if machine_type:
            context += machine_type
        if reason:
            context += (' - ' if context else '') + reason
            
        title = self.format_title(issue_type, display_name, mech_id, context)
        notes = f"Auto-created from finding. Original issue: {self.clean_text(summary)}"
        
        # Create task record
        task_data = {
            'finding_id': finding_id,
            'title': title,
            'issue_type': issue_type,
            'entity_type': 'mechanic',
            'mechanic_name': display_name,
            'mechanic_id': mech_id,
            'assigned_to': None,
            'status': 'open',
            'monitor_frequency': freq,
            'monitor_start_date': self.today.isoformat(),
            'monitor_end_date': end.isoformat(),
            'monitor_status': 'active',
            'notes': notes,
            'extension_count': 0
        }
        
        try:
            # Insert task
            task_res = self.supabase.table('tasks').insert(task_data).execute()
            
            if not task_res.data:
                print(f"TASK_WRITER: Error creating task for finding {finding_id}")
                return None
                
            task_id = task_res.data[0]['id']
            print(f"TASK_WRITER: Created task ID {task_id} for finding {finding_id}")
            
            # Create baseline measurement
            try:
                value = float(details.get('value', 0))
            except (ValueError, TypeError):
                value = 0
                
            mdata = {
                'task_id': task_id,
                'measurement_date': self.today.isoformat(),
                'value': round(value, 2),
                'change_pct': 0,
                'is_improved': False,
                'notes': 'Baseline measurement from finding'
            }
            
            m_res = self.supabase.table('measurements').insert(mdata).execute()
            
            if m_res.data:
                print(f"TASK_WRITER: Created baseline measurement for task {task_id}")
            else:
                print(f"TASK_WRITER: Failed to create baseline measurement for task {task_id}")
            
            # Update finding status
            self.supabase.table('findings_log').update({'status': 'Task_Created'}).eq('finding_id', finding_id).execute()
            
            # Return the created task with additional info
            created_task = task_res.data[0]
            created_task.update({
                'machine_type': machine_type,
                'reason': reason,
                'baseline_value': round(value, 2)
            })
            
            return created_task
            
        except Exception as e:
            print(f"TASK_WRITER: Error creating task: {e}")
            return None
    
    def create_tasks_from_findings(self):
        """
        Find new findings and create tasks for them
        
        Returns:
            list: Created task records
        """
        if not self.supabase:
            print("TASK_WRITER: No database connection")
            return []
            
        try:
            # Get new findings
            findings = self.supabase.table('findings_log').select('*').eq('status', 'New').execute()
            
            if not findings.data:
                print("TASK_WRITER: No new findings to process")
                return []
                
            print(f"TASK_WRITER: Found {len(findings.data)} new findings")
            
            # Create tasks
            created_tasks = []
            for finding in findings.data:
                task = self.create_task_from_finding(finding)
                if task:
                    created_tasks.append(task)
            
            print(f"TASK_WRITER: Created {len(created_tasks)} tasks")
            return created_tasks
            
        except Exception as e:
            print(f"TASK_WRITER: Error processing findings: {e}")
            return []

# For direct execution
if __name__ == '__main__':
    writer = TaskWriter()
    tasks = writer.create_tasks_from_findings()
    
    print(f"\nTask Writer Summary:")
    print(f"- Created {len(tasks)} tasks from findings")
    
    if tasks:
        print("\nSample Tasks:")
        for i, task in enumerate(tasks[:3]):  # Show first 3 tasks
            print(f"{i+1}. ID {task['id']}: {task['title']}")
            print(f"   Monitoring: {task['monitor_frequency']} until {task['monitor_end_date']}")