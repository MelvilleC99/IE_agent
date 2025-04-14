import datetime
# Import functions from your other scripts
from .mechanic_repair_analyzer import run_mechanic_analysis
from .mechanic_repair_interpreter import interpret_and_save_findings

# Import the agent entry point function
try:
    from src.agents.maintenance.agent import process_maintenance_finding
except ImportError:
    print("WORKFLOW ERROR: Cannot import agent function 'process_maintenance_finding'. Check path.")
    # Define a dummy function for testing if agent not ready
    def process_maintenance_finding(finding_data: dict):
        print(f"DUMMY AGENT: Would process finding: {finding_data.get('finding_summary')}")
        # In real use, this should not be a dummy

def run_workflow():
    """
    Orchestrates the mechanic repair analysis, interpretation, and agent triggering.
    This is the script to schedule.
    """
    print(f"\n--- Starting Mechanic Repair Workflow: {datetime.datetime.now()} ---")

    # Step 1 & 2: Run Analysis
    # Consider passing data source path if needed, or configure it elsewhere
    analysis_summary = run_mechanic_analysis()

    if not analysis_summary:
        print("WORKFLOW: Analysis did not produce a summary. Exiting.")
        return

    # Step 3: Interpret and Save Findings
    # This function now returns the list of findings it identified/saved
    findings_to_process = interpret_and_save_findings(analysis_summary)

    # Step 4: Trigger Agent for each finding
    if findings_to_process:
        print(f"\nWORKFLOW: Triggering agent for {len(findings_to_process)} findings...")
        for finding in findings_to_process:
            print("-" * 20)
            print(f"WORKFLOW: Calling agent for Finding ID {finding.get('finding_id', 'N/A')}: {finding['finding_summary']}")
            try:
                # Call the actual agent function, passing the specific finding dictionary
                process_maintenance_finding(finding)
            except Exception as e:
                print(f"WORKFLOW ERROR: Failed to process finding with agent: {e}")
                # Log this error properly
    else:
        print("\nWORKFLOW: No actionable findings identified by the interpreter.")

    print(f"\n--- Mechanic Repair Workflow Finished: {datetime.datetime.now()} ---")


if __name__ == "__main__":
    run_workflow()