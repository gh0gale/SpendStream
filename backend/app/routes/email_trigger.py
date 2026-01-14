from fastapi import APIRouter, BackgroundTasks
import subprocess
import sys
from pathlib import Path

router = APIRouter(prefix="/email", tags=["Email"])

# Store the status of the last run
last_run_status = {"status": "idle", "message": "", "run_id": None}

def run_ingestion_sync():
    """Synchronous function to run the ingestion script"""
    global last_run_status
    last_run_status = {"status": "running", "message": "Email ingestion in progress...", "run_id": last_run_status.get("run_id")}
    
    try:
        # Get the project root directory (SpendStream folder)
        project_root = Path(__file__).parent.parent.parent.parent
        
        # Run the script using the same Python interpreter
        result = subprocess.run(
            [sys.executable, "-m", "data_pipeline.scripts.run_email_ingestion"],
            cwd=str(project_root),
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            last_run_status = {
                "status": "error",
                "message": "Email ingestion completed with errors",
                "error": result.stderr,
                "output": result.stdout,
                "run_id": last_run_status.get("run_id")
            }
        else:
            last_run_status = {
                "status": "success",
                "message": "Email ingestion completed successfully",
                "output": result.stdout,
                "run_id": last_run_status.get("run_id")
            }
    except Exception as e:
        last_run_status = {
            "status": "error",
            "message": "Email ingestion failed",
            "error": str(e),
            "run_id": last_run_status.get("run_id")
        }

@router.post("/trigger-ingestion")
def trigger_email_ingestion(background_tasks: BackgroundTasks):
    """
    Trigger the email ingestion script to run in the background.
    This will fetch emails from Gmail and store them as JSON files.
    """
    global last_run_status
    import time
    run_id = int(time.time())  # Simple run ID based on timestamp
    last_run_status["run_id"] = run_id
    
    background_tasks.add_task(run_ingestion_sync)
    
    return {
        "message": "Email ingestion started in background",
        "status": "started",
        "run_id": run_id
    }

@router.get("/ingestion-status")
async def get_ingestion_status():
    """
    Get the status of the last email ingestion run.
    """
    return last_run_status
