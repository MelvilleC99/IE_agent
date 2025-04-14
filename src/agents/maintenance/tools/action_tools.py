# src/agents/maintenance/tools/action_tools.py
from langchain_core.tools import tool
import json # For type hint if needed
# Assume you have a way to get a database connection/cursor
# Maybe from a shared module like src/shared_services/db_client.py
from src.shared_services import db_client # Hypothetical module

def _create_action_in_db(finding_summary: str, finding_details: dict, analysis_type: str, assigned_user: str = "Maintenance Manager") -> int:
    """Internal function to insert into the maintenance_actions table."""
    conn = None
    action_id = -1
    try:
        conn = db_client.get_connection() # Get DB connection
        cursor = conn.cursor()
        sql = """
            INSERT INTO maintenance_actions
                (triggering_finding, finding_details, analysis_type, status, assigned_user, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
            RETURNING action_id;
        """
        # Use json.dumps if your DB function expects a string for JSONB
        cursor.execute(sql, (finding_summary, json.dumps(finding_details), analysis_type, 'New', assigned_user))
        action_id = cursor.fetchone()[0]
        conn.commit()
        print(f"DB: Successfully created action item with ID: {action_id}")
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"DB ERROR: Failed to create action item: {e}")
        action_id = -1 # Indicate error
    finally:
        if conn:
            db_client.release_connection(conn) # Release connection
    return action_id

@tool
def create_maintenance_action_item(finding_summary: str, finding_details: dict, analysis_type: str) -> str:
    """
    Use this tool to log a NEW maintenance action item in the database when an actionable finding is identified by an analysis script.
    Requires:
    - finding_summary: A text description of the problem.
    - finding_details: A dictionary containing specific data about the finding.
    - analysis_type: A string identifying which analysis found the issue (e.g., 'mechanic_repair_time_overall').
    Returns a confirmation message with the new action_id if successful, or an error message.
    DO NOT use this tool for findings that already have an action item.
    """
    print(f"TOOL: Attempting to create action item for: {finding_summary}")
    action_id = _create_action_in_db(finding_summary, finding_details, analysis_type)

    if action_id > 0:
        # --- Optional: Update the findings_log status ---
        # You could add code here to update the status of the original finding
        # in the findings_log table to 'ActionCreated' and store the action_id.
        # update_finding_log_status(original_finding_id, 'ActionCreated', action_id)
        # -------------------------------------------------
        return f"Action item successfully created in the database with ID: {action_id}. Please inform the manager."
    else:
        return "Error: There was a problem creating the action item in the database."