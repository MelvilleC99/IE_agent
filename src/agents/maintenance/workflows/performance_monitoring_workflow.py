#!/usr/bin/env python3
import sys
import json
import argparse
import logging
from datetime import datetime
from pathlib import Path

# Structured logging setup
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Now that this project is an installable package, imports just work:
from agents.maintenance.tracker.task_monitor import TaskMonitorChecker
from agents.maintenance.tracker.daily_performance import DailyPerformanceMeasurement

def find_firebase_credentials() -> Path:
    """Look for Firebase credentials and exit if none found."""
    project_root = Path(__file__).parent.parent.parent
    possible = [
        project_root / 'config' / 'firebase-credentials.json',
        project_root / 'firebase-credentials.json',
        Path('firebase-credentials.json')
    ]
    for p in possible:
        if p.is_file():
            logger.info(f"Found Firebase credentials at: {p}")
            return p
    logger.error("Firebase credentials not found in any of: %s", ", ".join(str(p) for p in possible))
    sys.exit(1)  # Fail fast

def run_workflow(day_override=None, run_daily=True, run_weekly=False, save_output=True) -> dict:
    logger.info("Starting monitoring workflow")
    creds_path = find_firebase_credentials()

    # STEP 1: Task discovery
    logger.info("STEP 1: Checking for tasks that need attention")
    task_checker = TaskMonitorChecker()
    tasks = task_checker.run()

    results = {
        'timestamp': datetime.now().isoformat(),
        'daily_tasks_found': len(tasks['daily_tasks']),
        'weekly_tasks_found': len(tasks['weekly_tasks']),
        'evaluation_tasks_found': len(tasks['evaluation_tasks']),
        'daily_results': None,
        'weekly_results': None,
        'evaluation_results': None
    }

    # STEP 2: Daily
    if run_daily and tasks['daily_tasks']:
        try:
            logger.info("STEP 2: Processing daily tasks")
            daily = DailyPerformanceMeasurement(creds_path)
            results['daily_results'] = daily.process_tasks(tasks['daily_tasks'])
            if save_output:
                out = Path(__file__).parent / 'output'
                out.mkdir(exist_ok=True)
                today = datetime.now().strftime('%Y-%m-%d')
                (out / f'daily_tasks_{today}.json').write_text(json.dumps(tasks['daily_tasks'], indent=2))
                (out / f'daily_results_{today}.json').write_text(json.dumps(results['daily_results'], indent=2))
        except Exception:
            logger.exception("Error during daily processing")
            sys.exit(1)

    # STEP 3: Weekly (stub)
    if run_weekly and tasks['weekly_tasks']:
        logger.warning("Weekly processing not yet implemented")
        results['weekly_results'] = None

    # STEP 4: Evaluation (stub)
    if tasks['evaluation_tasks']:
        logger.warning("Evaluation processing not yet implemented")
        results['evaluation_results'] = None

    # Summary
    total = results['daily_tasks_found'] + results['weekly_tasks_found'] + results['evaluation_tasks_found']
    logger.info("Workflow complete: %d tasks found (D:%d W:%d E:%d)",
                total, results['daily_tasks_found'], results['weekly_tasks_found'], results['evaluation_tasks_found'])
    if results['daily_results'] is not None:
        measured = sum(1 for r in results['daily_results'] if r.get('status') == 'measured')
        logger.info("Daily tasks successfully measured: %d", measured)

    return results

if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser(description='Run the task monitoring workflow')
        parser.add_argument('--daily', action='store_true', help='Run daily measurements')
        parser.add_argument('--weekly', action='store_true', help='Run weekly measurements')
        parser.add_argument('--all', action='store_true', help='Run all measurements')
        parser.add_argument('--day', help='Override the current day (for testing)')
        parser.add_argument('--no-save', action='store_true', help='Do not save output files')
        args = parser.parse_args()

        run_daily = args.daily or args.all or not (args.daily or args.weekly or args.all)
        run_weekly = args.weekly or args.all
        save_output = not args.no_save

        run_workflow(
            day_override=args.day,
            run_daily=run_daily,
            run_weekly=run_weekly,
            save_output=save_output
        )
        sys.exit(0)

    except Exception:
        logger.exception("Unexpected error in main")
        sys.exit(1)
