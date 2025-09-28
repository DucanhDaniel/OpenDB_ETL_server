from fastapi import FastAPI, HTTPException, Query
import logging
import redis
from typing import Dict, Any, Optional

# Import tác vụ Celery
from celery_worker import run_report_job

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="TikTok Reporting API", version="2.0.0")

# --- API Endpoints ---

@app.get("/reports/create-job", tags=["Async Jobs"])
def create_report_job(
    # ... (giữ nguyên tất cả các tham số Query của bạn) ...
    task_type: str, callback_url: str, job_id: str, task_id: str,
    access_token: str, advertiser_id: str, store_id: str,
    start_date: str, end_date: str,
    advertiser_name: Optional[str] = None, store_name: Optional[str] = None
):
    """
    Tạo một công việc nền bằng Celery.
    Phản hồi ngay lập tức và gửi dữ liệu sau qua callback_url.
    """
    logger.info(f"Received Celery job request. Job ID: {job_id}, Type: {task_type}")
    if task_type not in ["creative", "product"]:
        raise HTTPException(status_code=400, detail="Invalid 'task_type'.")

    context = {
        "task_type": task_type, "callback_url": callback_url, "job_id": job_id, "task_id": task_id,
        "access_token": access_token, "advertiser_id": advertiser_id, "store_id": store_id,
        "start_date": start_date, "end_date": end_date,
        "advertiser_name": advertiser_name, "store_name": store_name
    }
    
    # SỬA ĐỔI: Gọi tác vụ Celery thay vì BackgroundTasks
    run_report_job.delay(context)

    return {
        "status": "queued",
        "job_id": job_id,
        "message": "Job accepted and queued for processing. Data will be sent to the callback URL."
    }

@app.post("/reports/{job_id}/cancel", tags=["Async Jobs"])
def cancel_report_job(job_id: str):
    """
    Gửi yêu cầu dừng một công việc đang chạy.
    """
    try:
        redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        cancel_key = f"job:{job_id}:cancel_requested"
        # Đặt cờ yêu cầu dừng, tự hết hạn sau 1 giờ để tránh rác
        redis_client.set(cancel_key, "true", ex=3600)
        logger.info(f"Cancel request sent for Job ID: {job_id}")
        return {"status": "cancel_requested", "job_id": job_id}
    except Exception as e:
        logger.error(f"Could not send cancel request for {job_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to connect to state manager (Redis).")
    
import uvicorn
from pyngrok import ngrok
if __name__ == "__main__":
    # uvicorn.run("test:app", host = "0.0.0.0", port = 8001)
    port = 8001
    
    try:
        # Mở một tunnel HTTP tới cổng 8001

        public_url = ngrok.connect(port, "http")
        print("="*50)
        print(f" * Ngrok tunnel đang chạy tại: {public_url}")
        print(f" * Uvicorn đang chạy trên http://127.0.0.1:{port}")
        print("="*50)
        
        # Chạy uvicorn. Lưu ý: truyền đối tượng 'app' trực tiếp
        uvicorn.run(app, host="0.0.0.0", port=port)
        
    except Exception as e:
        print(f"Lỗi: {e}")
    finally:
        # Ngắt kết nối ngrok khi ứng dụng dừng
        ngrok.disconnect(public_url.public_url)
        print("Đã đóng kết nối ngrok.")
        
# celery -A celery_worker.celery_app worker -P gevent --loglevel=info