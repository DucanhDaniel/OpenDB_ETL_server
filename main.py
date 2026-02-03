from fastapi import FastAPI, HTTPException
import logging
from typing import Optional, List, Dict
import redis
from workers.tasks_modular import run_report_job

from models.schemas import CreateJobRequest
from fastapi.middleware.cors import CORSMiddleware
from services.database.mongo_client import MongoDbClient
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
import os 
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')
REDIS_HOST = os.getenv('REDIS_HOST')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
app = FastAPI(title="TikTok Reporting API", version="2.0.0")
db_client = MongoDbClient()
redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=0, password=REDIS_PASSWORD, decode_responses=True)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Cho phép tất cả các nguồn gốc
    allow_credentials=True,
    allow_methods=["*"],  # Cho phép tất cả các phương thức (GET, POST, etc.)
    allow_headers=["*"],  # Cho phép tất cả các header
)
app.mount("/static", StaticFiles(directory="static"), name="static")
@app.get("/dashboard", response_class=HTMLResponse, tags=["Dashboard"])
async def read_root():
    return FileResponse('static/index.html')

# --- API Endpoints ---

from services.dashboard.dashboard_service import get_dashboard_data

@app.get("/api/dashboard", tags=["Dashboard"])
def dashboard_endpoint():
    """
    Tổng hợp và trả về tất cả dữ liệu cần thiết cho dashboard.
    """
    if not db_client or not redis_client:
        raise HTTPException(status_code=503, detail="Database or Redis connection is unavailable.")
    
    try:
        return get_dashboard_data(db_client, redis_client)
        
    except Exception as e:
        logger.error(f"Lỗi khi truy vấn dashboard data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Lỗi server khi lấy dữ liệu: {e}")



@app.post("/reports/create-job", tags=["Async Jobs"])
def create_report_job(job_request: CreateJobRequest):
    """
    Tạo một công việc nền bằng Celery từ một request body dạng JSON.
    """
    logger.info(f"Received Celery job request. Job ID: {job_request.job_id}, Type: {job_request.task_type}")
    
    context = job_request.model_dump()
    
    run_report_job.delay(context)

    return {
        "status": "queued",
        "job_id": job_request.job_id,
        "message": "Job accepted and queued for processing. Data will be sent to the callback URL."
    }
    

@app.post("/reports/{job_id}/cancel", tags=["Async Jobs"])
def cancel_report_job(job_id: str):
    """
    Gửi yêu cầu dừng một công việc đang chạy.
    """
    try:
        cancel_key = f"job:{job_id}:cancel_requested"
        redis_client.set(cancel_key, "true", ex=3600)
        logger.info(f"Cancel request sent for Job ID: {job_id}")
        return {"status": "cancel_requested", "job_id": job_id}
    except Exception as e:
        logger.error(f"Could not send cancel request for {job_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to connect to state manager (Redis).")




