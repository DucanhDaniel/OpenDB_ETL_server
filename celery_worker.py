# celery_worker.py
import logging
import requests
import redis
from celery import Celery
from typing import Dict, Any

# Import các lớp Reporter và hàm flatten từ project của bạn
from services.gmv.campaign_creative_detail import GMVCampaignCreativeDetailReporter, _flatten_creative_report
from services.gmv.campaign_product_detail import GMVCampaignProductDetailReporter, _flatten_product_report
from services.exceptions import TaskCancelledException # Sẽ tạo exception này ở Bước 2

# Cấu hình Celery để sử dụng Redis
celery_app = Celery('tasks', broker='redis://localhost:6379/0', backend='redis://localhost:6379/0')

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kết nối tới Redis để quản lý trạng thái
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

@celery_app.task
def run_report_job(context: Dict[str, Any]):
    """
    Đây là tác vụ Celery, chứa logic từ hàm process_report_and_callback cũ.
    """
    job_id = context["job_id"]
    task_id = context["task_id"]
    task_type = context["task_type"]
    callback_url = context["callback_url"]
    
    logger.info(f"[Job ID: {job_id}] Celery task started for type: {task_type}.")

    def send_progress_update(status: str, message: str, progress: int):
        # ... (giữ nguyên hàm send_progress_update của bạn)
        try:
            progress_payload = {
                "job_id": job_id, "task_id": task_id, "status": status,
                "message": message, "progress": progress
            }
            logger.info(f"[Job ID: {job_id}] Sending progress: {message}")
            requests.post(callback_url, json=progress_payload, timeout=15)
        except requests.exceptions.RequestException as e:
            logger.warning(f"[Job ID: {job_id}] Could not send progress update: {e}")

    try:
        send_progress_update(status="RUNNING", message="Server đã nhận request, bắt đầu khởi tạo...", progress=0)
        
        common_reporter_args = {
            "access_token": context["access_token"],
            "advertiser_id": context["advertiser_id"],
            "store_id": context["store_id"],
            "progress_callback": send_progress_update,
            "job_id": job_id, # Truyền job_id để kiểm tra cờ dừng
            "redis_client": redis_client
        }

        if task_type == "creative":
            reporter = GMVCampaignCreativeDetailReporter(**common_reporter_args)
            flatten_function = _flatten_creative_report
        elif task_type == "product":
            reporter = GMVCampaignProductDetailReporter(**common_reporter_args)
            flatten_function = _flatten_product_report
        else:
            raise ValueError("Invalid task type specified.")

        raw_data = reporter.get_data(context["start_date"], context["end_date"])
        flattened_data = flatten_function(raw_data, context)

        callback_payload = {
            "job_id": job_id, "task_id": task_id,
            "status": "SUCCESS", "data": flattened_data
        }

    except TaskCancelledException:
        logger.warning(f"[Job ID: {job_id}] Task was cancelled by user.")
        callback_payload = {
            "job_id": job_id, "task_id": task_id,
            "status": "CANCELLED", "error_message": "Task was cancelled by user.", "data": []
        }
    except Exception as e:
        logger.error(f"[Job ID: {job_id}] Error during data processing: {e}", exc_info=True)
        callback_payload = { "job_id": job_id, "task_id": task_id, "status": "FAILED", "error_message": str(e), "data": [] }

    try:
        logger.info(f"[Job ID: {job_id}] Sending final data to callback URL: {callback_url}")
        requests.post(callback_url, json=callback_payload, timeout=60).raise_for_status()
        logger.info(f"[Job ID: {job_id}] Final callback sent successfully.")
    except requests.exceptions.RequestException as e:
        logger.error(f"[Job ID: {job_id}] Failed to send final callback: {e}", exc_info=True)