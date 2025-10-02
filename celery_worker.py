import logging
import requests
import redis
from celery import Celery
from typing import Dict, Any
import os

from services.gmv.campaign_creative_detail import GMVCampaignCreativeDetailReporter, _flatten_creative_report
from services.gmv.campaign_product_detail import GMVCampaignProductDetailReporter, _flatten_product_report
from services.exceptions import TaskCancelledException 
from services.sheet_writer.gg_sheet_writer import GoogleSheetWriter

REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')
CREDENTIALS_PATH = os.getenv('GOOGLE_CREDENTIALS_PATH', 'credentials.json')
# SPREADSHEET_ID = os.getenv('GOOGLE_SPREADSHEET_ID')

celery_app = Celery(
    'tasks', 
    broker=f'redis://:{REDIS_PASSWORD}@redis:6379/0', 
    backend=f'redis://:{REDIS_PASSWORD}@redis:6379/0'
)

celery_app.conf.update(
    broker_transport_options={
        'health_check_interval': 30.0,
    },
    
    broker_connection_retry_on_startup=True
)

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kết nối tới Redis để quản lý trạng thái
# redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
redis_client = redis.Redis(
    host='redis', 
    port=6379, 
    db=0, 
    password=REDIS_PASSWORD, # <-- THÊM THAM SỐ NÀY
    decode_responses=True
)

@celery_app.task
def run_report_job(context: Dict[str, Any]):
    """
    Đây là tác vụ Celery, chứa logic từ hàm process_report_and_callback cũ.
    """
    job_id = context["job_id"]
    task_id = context["task_id"]
    task_type = context["task_type"]
    # callback_url = context["callback_url"]
    spreadsheet_id = context["spreadsheet_id"]
    
    # Khởi tạo writer
    writer = GoogleSheetWriter(CREDENTIALS_PATH, spreadsheet_id)
    
    logger.info(f"[Job ID: {job_id}] Celery task started for type: {task_type}.")

    def send_progress_update(status: str, message: str, progress: int = 0):
        """
        Hàm con mới: Ghi tiến trình trực tiếp vào Sheet thay vì gọi POST.
        """
        if (status == "STOPPED"): return
        try:
            writer.log_progress(task_id, status, message, progress)
        except Exception as e:
            logger.warning(f"[Job ID: {job_id}] Could not log progress to sheet: {e}")

    try:
        send_progress_update(status="RUNNING", message="Server đã nhận request, bắt đầu khởi tạo...", progress=0)
        
        common_reporter_args = {
            "access_token": context["access_token"],
            "advertiser_id": context["advertiser_id"],
            "store_id": context["store_id"],
            "progress_callback": send_progress_update,
            "job_id": job_id, 
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
        
        
        final_message = "Hoàn tất! Không có dữ liệu mới để ghi."
        if flattened_data:
            send_progress_update(status="RUNNING", message="Đã lấy xong dữ liệu, bắt đầu ghi...", progress=95)
            
            if not spreadsheet_id:
                raise ValueError("Chưa có spreadsheet_id.")

            

            # Lấy các tùy chọn ghi từ context được gửi từ Apps Script
            sheet_options = {
                "sheetName": context.get("sheet_name"),
                "isOverwrite": context.get("is_overwrite", False),
                "isFirstChunk": context.get("is_first_chunk", False)
            }
            
            # Lấy selected_fields từ context
            selected_fields = context.get("selected_fields")

            # Ưu tiên dùng selected_fields làm headers, nếu không có thì dùng như cũ để dự phòng
            if selected_fields:
                headers = selected_fields
                logger.info(f"[Job ID: {job_id}] Sử dụng {len(headers)} trường đã chọn làm tiêu đề.")
            else:
                headers = list(flattened_data[0].keys())
                logger.warning(f"[Job ID: {job_id}] Không có selected_fields. Sử dụng tất cả {len(headers)} trường có sẵn làm tiêu đề.")

            # Ghi dữ liệu
            rows_written = writer.write_data(flattened_data, headers, sheet_options)
            final_message = f"Hoàn tất! Đã ghi {rows_written} dòng vào sheet '{sheet_options['sheetName']}'."
            
        
        callback_payload = {
            "job_id": job_id, 
            "task_id": task_id,
            "status": "COMPLETED", 
            "message": final_message 
        }

    except TaskCancelledException:
        logger.warning(f"[Job ID: {job_id}] Task was cancelled by user.")
        callback_payload = {
            "job_id": job_id, "task_id": task_id,
            "status": "STOPPED", "message": "Task was cancelled by user.", "data": []
        }
    except Exception as e:
        logger.error(f"[Job ID: {job_id}] Error during data processing: {e}", exc_info=True)
        callback_payload = { "job_id": job_id, "task_id": task_id, "status": "FAILED", "message": str(e), "data": [] }

    try:
        logger.info(f"[Job ID: {job_id}] Sending final data sheet_ID: {spreadsheet_id}")
        # requests.post(callback_url, json=callback_payload, timeout=60).raise_for_status()
        send_progress_update(callback_payload["status"], callback_payload["message"])
        logger.info(f"[Job ID: {job_id}] Final callback sent successfully.")
    except requests.exceptions.RequestException as e:
        logger.error(f"[Job ID: {job_id}] Failed to send final callback: {e}", exc_info=True)