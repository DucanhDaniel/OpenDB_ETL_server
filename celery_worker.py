import logging
import requests
import redis
from celery import Celery
from celery.signals import task_prerun, task_postrun
from pprint import pprint
from typing import Dict, Any
import os
from utils.utils import write_data_to_sheet, is_full_month
from services.gmv.campaign_creative_detail import GMVCampaignCreativeDetailReporter, _flatten_creative_report
from services.gmv.campaign_product_detail import GMVCampaignProductDetailReporter, _flatten_product_report
from services.gmv.gmv_reporter import GMVReporter
from services.exceptions import TaskCancelledException 
from services.sheet_writer.gg_sheet_writer import GoogleSheetWriter
from services.database.mongo_client import MongoDbClient
from datetime import date, datetime, timedelta, timezone

REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')
REDIS_HOST = os.getenv('REDIS_HOST')
CREDENTIALS_PATH = os.getenv('GOOGLE_CREDENTIALS_PATH', 'credentials.json')

celery_app = Celery(
    'tasks', 
    broker=f'redis://:{REDIS_PASSWORD}@{REDIS_HOST}:6379/0', 
    backend=f'redis://:{REDIS_PASSWORD}@{REDIS_HOST}:6379/0'
)

celery_app.conf.update(
    broker_transport_options={
        'health_check_interval': 30.0,
    },
    
    broker_connection_retry_on_startup=True
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
redis_client = redis.Redis(
    host=REDIS_HOST, 
    port=6379, 
    db=0, 
    password=REDIS_PASSWORD, 
    decode_responses=True
)


db_client = MongoDbClient()
# db_client = None


@task_prerun.connect
def on_task_prerun(sender=None, task_id=None, args=None, **kwargs):
    if sender and 'run_report_job' in sender.name and db_client: 
        context = args[0]
        job_id = context.get("job_id")
        try:
            db_client.db.task_logs.insert_one({
                "job_id": job_id,
                "celery_task_id": task_id,
                "user_email": context.get("user_email"),
                "task_type": context.get("task_type"),
                "advertiser_id": context.get("advertiser_id"),
                "store_id": context.get("store_id"),
                "date_start" : context.get("start_date"),
                "date_stop" : context.get("end_date"),
                "status": "STARTED",
                "start_time": datetime.now(timezone.utc),
                "end_time": None,
                "duration_seconds": None,
                "error_message": None
            })
        except Exception as e:
            logger.error(f"LỖI GHI LOG (PRERUN) cho job {job_id}: {e}")

@task_postrun.connect
def on_task_postrun(sender=None, task_id=None, state=None, retval=None, args=None, **kwargs):
    if sender and 'run_report_job' in sender.name:
        context = args[0]
        job_id = context.get("job_id")
        print(f"SIGNAL POSTRUN: Cập nhật log cho job {job_id} với trạng thái {state}")

        error_msg = str(retval) if state == 'FAILURE' else None
        
        # --- [NEW] Biến lưu api usage ---
        api_total_counts = {} 
        # --------------------------------

        if isinstance(retval, TaskCancelledException) or (error_msg and 'TaskCancelledException' in error_msg):
            final_status = 'CANCELLED'
            error_msg = "Task was cancelled by user."
        elif state == 'SUCCESS':
            final_status = 'SUCCESS'
            # --- [NEW] Lấy api_usage từ giá trị return của hàm run_report_job ---
            if isinstance(retval, dict):
                api_total_counts = retval.get("api_usage", {})
            # ---------------------------------------------------------------------
        else:
            final_status = 'FAILED'

        try:
            end_time = datetime.now(timezone.utc)
            
            if (db_client):
                start_log = db_client.db.task_logs.find_one({"celery_task_id": task_id})
                duration = (end_time - start_log['start_time'].replace(tzinfo=timezone.utc)).total_seconds() if start_log else -1

                db_client.db.task_logs.update_one(
                    {"celery_task_id": task_id},
                    {"$set": {
                        "status": final_status,
                        "end_time": end_time,
                        "duration_seconds": round(duration, 2),
                        "error_message": error_msg,
                        "api_total_counts": api_total_counts 
                    }}
                )
        except Exception as e:
            logger.error(f"LỖI GHI LOG (POSTRUN) cho task {task_id}: {e}")
    

@celery_app.task(soft_time_limit=900, time_limit=1200)
def run_report_job(context: Dict[str, Any]):
    """
    Đây là tác vụ Celery, chứa logic từ hàm process_report_and_callback cũ.
    """
    job_id = context["job_id"]
    task_id = context["task_id"]
    task_type = context["task_type"]
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

        # ---------------------------------- Lấy dữ liệu -------------------------------
        all_date_chunks = GMVReporter._generate_monthly_date_chunks(context["start_date"], context["end_date"])
        chunks_to_fetch_from_api = []
        cached_flattened_data = []
        accurate_data_date = date.today() - timedelta(days = 2)
        collection_name = f"{task_type}_reports"
        
        for chunk in all_date_chunks:
            chunk_start = datetime.strptime(chunk['start'], '%Y-%m-%d').date()
            chunk_end = datetime.strptime(chunk['end'], '%Y-%m-%d').date()

            # Luôn fetch API nếu chunk chứa khoảng không ổn định dữ liệu
            if chunk_start <= accurate_data_date <= chunk_end:
                logger.info(f"Chunk [{chunk['start']} - {chunk['end']}] chứa ngày hiện tại, sẽ được lấy từ API.")
                chunks_to_fetch_from_api.append(chunk)
                continue

            # Xây dựng query để tìm trong DB
            query = {
                # "user_email": context.get("user_email"),
                "advertiser_id": context.get("advertiser_id"),
                "store_id": context.get("store_id"),
                "start_date": chunk['start'],
                "end_date": chunk['end']
            }
            
            if (db_client): 
                existing_records = db_client.find(collection_name, query)
            
            if existing_records:
                logger.info(f"CACHE HIT: Tìm thấy {len(existing_records)} bản ghi cho chunk [{chunk['start']} - {chunk['end']}].")
                cached_flattened_data.extend(existing_records)
            else:
                logger.info(f"CACHE MISS: Không tìm thấy dữ liệu cho chunk [{chunk['start']} - {chunk['end']}], sẽ được lấy từ API.")
                chunks_to_fetch_from_api.append(chunk)
        
        api_raw_data = []
        if chunks_to_fetch_from_api:
            send_progress_update(status="RUNNING", message=f"Đang lấy dữ liệu cho {len(chunks_to_fetch_from_api)} chunk từ API...", progress=20)
            
            api_raw_data = reporter.get_data(chunks_to_fetch_from_api)
        else:
            logger.info("Tất cả dữ liệu đã có trong cache. Không cần gọi API.")
        # ----------------------------------- kết thúc phần lấy dữ liệu ----------------
        
        flattened_data_from_api = flatten_function(api_raw_data, context)
        
        
        if flattened_data_from_api:
            send_progress_update(status="RUNNING", message="Đang lọc và lưu dữ liệu vào DB...", progress=85)

            # Lọc ra những bản ghi thuộc về các tháng trọn vẹn
            data_to_save_in_db = [
                row for row in flattened_data_from_api 
                if is_full_month(row.get("start_date"), row.get("end_date"))
            ]

            if data_to_save_in_db:
                logger.info(f"Tìm thấy {len(data_to_save_in_db)} bản ghi thuộc các tháng trọn vẹn để lưu vào DB.")
                collection_name = f"{task_type}_reports"
                user_email = context.get("user_email")
                
                db_client.save_flattened_reports(
                    collection_name=collection_name,
                    data=data_to_save_in_db,
                    user_email=user_email,
                    api_usage = reporter.api_usage
                )
            else:
                logger.info("Không có dữ liệu mới nào thuộc tháng trọn vẹn để lưu vào DB.")
        
            
        cancel_key = f"job:{job_id}:cancel_requested"
        if redis_client.exists(cancel_key):
            redis_client.delete(cancel_key)
            raise TaskCancelledException()
        
        # -------------------------- GHI DỮ LIỆU RA SHEET --------------------------
        final_flattened_data = cached_flattened_data + flattened_data_from_api
        final_message = "Hoàn tất! Không có dữ liệu mới để ghi."
        if final_flattened_data:
            send_progress_update(status="RUNNING", message="Đã lấy xong dữ liệu, bắt đầu ghi...", progress=95)
            final_message = write_data_to_sheet(job_id, spreadsheet_id, context, final_flattened_data, writer)
        
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
        send_progress_update(callback_payload["status"], callback_payload["message"])
        raise
    
    except Exception as e:
        logger.error(f"[Job ID: {job_id}] Error during data processing: {e}", exc_info=True)
        callback_payload = { "job_id": job_id, "task_id": task_id, "status": "FAILED", "message": str(e), "data": [] }
        send_progress_update(callback_payload["status"], callback_payload["message"])
        raise
    
    try:
        logger.info(f"[Job ID: {job_id}] Sending final data sheet_ID: {spreadsheet_id}")
        send_progress_update(callback_payload["status"], callback_payload["message"])
        logger.info(f"[Job ID: {job_id}] Final callback sent successfully.")
        
        current_api_usage = reporter.api_usage if reporter else {}
        
        return {
            "status": "COMPLETED",
            "message": final_message,
            "api_usage": current_api_usage  
        }
    except requests.exceptions.RequestException as e:
        logger.error(f"[Job ID: {job_id}] Failed to send final callback: {e}", exc_info=True)
        raise