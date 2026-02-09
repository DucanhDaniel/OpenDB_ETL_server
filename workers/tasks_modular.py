"""
Celery Tasks - Modularized Version
Hỗ trợ cả TikTok GMV và Facebook Ads reports thông qua Worker Factory
"""

import logging
import redis
from celery import Celery
from celery.signals import task_prerun, task_postrun
from typing import Dict, Any
import os
from datetime import datetime, timezone

from .worker_factory import WorkerFactory
from services.exceptions import TaskCancelledException 
from services.sheet_writer.gg_sheet_writer import GoogleSheetWriter
from services.database.mongo_client import MongoDbClient
from utils.utils import write_data_to_sheet

# ==================== CONFIG ====================

REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')
REDIS_HOST = os.getenv('REDIS_HOST')
CREDENTIALS_PATH = os.getenv('GOOGLE_CREDENTIALS_PATH', 'credentials.json')

# ==================== CELERY APP ====================

celery_app = Celery(
    'tasks', 
    broker=f'redis://:{REDIS_PASSWORD}@{REDIS_HOST}:6379/0', 
    backend=f'redis://:{REDIS_PASSWORD}@{REDIS_HOST}:6379/0'
)

celery_app.conf.update(
    broker_transport_options={'health_check_interval': 30.0},
    broker_connection_retry_on_startup=True
)

# ==================== LOGGING ====================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==================== CLIENTS ====================

redis_client = redis.Redis(
    host=REDIS_HOST, 
    port=6379, 
    db=0, 
    password=REDIS_PASSWORD, 
    decode_responses=True
)

db_client = MongoDbClient()


# ==================== CELERY SIGNALS ====================

@task_prerun.connect
def on_task_prerun(sender=None, task_id=None, args=None, **kwargs):
    """Log task start to database"""
    if sender and 'run_report_job' in sender.name and db_client: 
        context = args[0]
        job_id = context.get("job_id")
        
        try:
            # Build log document với fields phù hợp cho từng task type
            log_doc = {
                "job_id": job_id,
                "celery_task_id": task_id,
                "user_email": context.get("user_email"),
                "task_type": context.get("task_type"),
                "date_start": context.get("start_date"),
                "date_stop": context.get("end_date"),
                "status": "STARTED",
                "start_time": datetime.now(timezone.utc),
                "end_time": None,
                "duration_seconds": None,
                "error_message": None
            }
            
            # TikTok specific fields
            if context.get("advertiser_id"):
                log_doc["advertiser_id"] = context.get("advertiser_id")
                log_doc["store_id"] = context.get("store_id")
            
            # Facebook specific fields
            if context.get("accounts"):
                log_doc["accounts"] = context.get("accounts")
                log_doc["template_name"] = context.get("template_name")
            
            db_client.db.task_logs.insert_one(log_doc)
            
        except Exception as e:
            logger.error(f"Error logging task start for job {job_id}: {e}")


@task_postrun.connect
def on_task_postrun(sender=None, task_id=None, state=None, retval=None, args=None, **kwargs):
    """Log task completion to database"""
    if sender and 'run_report_job' in sender.name:
        context = args[0]
        job_id = context.get("job_id")
        
        logger.info(f"Task postrun: Updating log for job {job_id} with state {state}")
        
        # Determine final status and message
        final_status = state
        message = None
        api_total_counts = {}

        if isinstance(retval, dict):
            # Worker returned a dict (likely SUCCESS or handled FAILED)
            final_status = retval.get("status", state)
            message = retval.get("message")
            api_total_counts = retval.get("api_usage", {})
        elif isinstance(retval, Exception):
            # Celery exception (TaskCancelled or other crash)
            message = str(retval)
            final_status = 'FAILED'
        elif isinstance(retval, str):
            message = retval

        # Special casing for cancellation
        if isinstance(retval, TaskCancelledException) or (message and 'TaskCancelledException' in message):
            final_status = 'CANCELLED'
            message = "Task was cancelled by user."
        
        # Update database
        try:
            if db_client:
                end_time = datetime.now(timezone.utc)
                start_log = db_client.db.task_logs.find_one({"celery_task_id": task_id})
                
                duration = -1
                if start_log:
                    start_time = start_log['start_time'].replace(tzinfo=timezone.utc)
                    duration = (end_time - start_time).total_seconds()
                
                db_client.db.task_logs.update_one(
                    {"celery_task_id": task_id},
                    {"$set": {
                        "status": final_status,
                        "end_time": end_time,
                        "duration_seconds": round(duration, 2),
                        "message": message,
                        "api_total_counts": api_total_counts
                    }}
                )
        except Exception as e:
            logger.error(f"Error logging task completion for task {task_id}: {e}")


# ==================== CELERY TASK ====================

@celery_app.task(soft_time_limit=1500, time_limit=1800)
def run_report_job(context: Dict[str, Any]):
    """
    Universal Celery task cho tất cả report types.
    Sử dụng WorkerFactory để delegate cho worker phù hợp.
    
    Args:
        context: Job context chứa:
            - job_id: Unique job identifier
            - task_id: Task ID cho progress tracking
            - task_type: Loại report ("creative", "product", "facebook_daily", etc.)
            - spreadsheet_id: Google Sheet ID
            - start_date, end_date: Date range
            - Các fields khác tùy theo task_type
            
    Returns:
        Dict containing:
            - status: "SUCCESS" or "FAILED"
            - message: Human-readable message
            - api_usage: API usage statistics
    """
    job_id = context["job_id"]
    task_id = context["task_id"]
    task_type = context["task_type"]
    spreadsheet_id = context["spreadsheet_id"]
    
    logger.info(f"[Job {job_id}] Starting Celery task for type: {task_type}")
    
    # Khởi tạo sheet writer
    writer = GoogleSheetWriter(CREDENTIALS_PATH, spreadsheet_id)
    
    def send_progress_update(status: str, message: str, progress: int = 0):
        """Callback function để ghi progress vào sheet"""
        if status == "STOPPED":
            return
        try:
            writer.log_progress(task_id, status, message, progress)
        except Exception as e:
            logger.warning(f"[Job {job_id}] Could not log progress: {e}")
    
    try:
        # ========== BƯỚC 1: Tạo worker từ factory ==========
        send_progress_update("RUNNING", "Khởi tạo worker...", 0)
        
        worker = WorkerFactory.create_worker(
            task_type=task_type,
            context=context,
            db_client=db_client,
            redis_client=redis_client,
            progress_callback=send_progress_update
        )
        
        logger.info(f"[Job {job_id}] Created worker: {worker.__class__.__name__}")
        
        # ========== BƯỚC 2: Chạy worker ==========
        result = worker.run()
        
        # result = {
        #     "status": "SUCCESS",
        #     "message": "...",
        #     "data": [...],
        #     "api_usage": {...}
        # }
        
        # ========== BƯỚC 3: Xử lý kết quả ==========
        final_message = result.get("message", "No message")
        final_status = result.get("status", "SUCCESS")
        api_usage = result.get("api_usage", {})
        
        # Nếu trạng thái là FAILED thì gửi update FAILED và raise lỗi
        if final_status == "FAILED":
            logger.error(f"[Job {job_id}] Worker reported failure: {final_message}")
            send_progress_update("FAILED", final_message)
            return {
                "status": "FAILED",
                "message": final_message,
                "api_usage": api_usage
            }

        # ========== BƯỚC 4: Gửi final callback SUCCESS ==========
        logger.info(f"[Job {job_id}] Completed successfully")
        send_progress_update("COMPLETED", final_message, 100)
        
        return {
            "status": "SUCCESS",
            "message": final_message,
            "api_usage": api_usage
        }
        
    except TaskCancelledException:
        logger.warning(f"[Job {job_id}] Task was cancelled by user")
        send_progress_update("STOPPED", "Task was cancelled by user")
        raise
    
    except Exception as e:
        logger.error(f"[Job {job_id}] Error during processing: {e}", exc_info=True)
        send_progress_update("FAILED", str(e))
        raise


# ==================== HELPER FUNCTIONS ====================

def get_supported_task_types():
    """Get list of supported task types"""
    return WorkerFactory.get_supported_types()


if __name__ == "__main__":
    # For testing
    print("Supported task types:")
    for task_type in get_supported_task_types():
        print(f"  - {task_type}")