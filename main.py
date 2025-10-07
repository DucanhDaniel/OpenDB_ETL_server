from fastapi import FastAPI, HTTPException
import logging
from typing import Optional, List, Dict
import redis
from celery_worker import run_report_job
from models.schemas import CreateJobRequest
from fastapi.middleware.cors import CORSMiddleware
from services.database.mongo_client import MongoDbClient
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
import os 
from datetime import datetime, timezone, timedelta

REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
app = FastAPI(title="TikTok Reporting API", version="2.0.0")
db_client = MongoDbClient()
redis_client = redis.Redis(host='redis', port=6379, db=0, password=REDIS_PASSWORD, decode_responses=True)

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
def get_task_logs_from_db() -> List[Dict]:
    """Lấy log tác vụ từ MongoDB."""
    if not db_client: return []
    tasks_cursor = db_client.db.task_logs.find({}).sort("start_time", -1).limit(100)
    tasks = list(tasks_cursor)
    for task in tasks:
        task['_id'] = str(task['_id'])
        if task.get('start_time'): task['start_time'] = task['start_time'].isoformat() + 'Z'
        if task.get('end_time'): task['end_time'] = task['end_time'].isoformat() + 'Z'
    return tasks

def get_api_total_counts() -> Dict[str, int]:
    """Lấy tổng số lần gọi API từ Redis."""
    if not redis_client: return {}
    counts = {}
    keys = list(redis_client.scan_iter("api_calls_total:*"))
    if not keys: return {}
    
    values = redis_client.mget(keys)
    for i, key in enumerate(keys):
        endpoint = key.replace('api_calls_total:', '')
        counts[endpoint] = int(values[i]) if values[i] else 0
    return counts

def get_api_timeseries_counts(endpoints: List[str], hours: int = 24) -> Dict[str, List]:
    """Lấy dữ liệu time-series cho các endpoint được chỉ định."""
    if not redis_client or not endpoints: return {}
    
    timeseries_data = {}
    now = datetime.now(timezone.utc)

    for endpoint in endpoints:
        keys_to_fetch = []
        timestamps = []
        for i in range(hours):
            target_time = now - timedelta(hours=i)
            hour_str = target_time.strftime('%Y-%m-%d-%H')
            keys_to_fetch.append(f"api_calls:{endpoint}:{hour_str}")
            timestamps.append(target_time.isoformat())
        
        keys_to_fetch.reverse()
        timestamps.reverse()
        
        values = redis_client.mget(keys_to_fetch)
        
        endpoint_data = [{"timestamp": ts, "count": int(val) if val else 0} for ts, val in zip(timestamps, values)]
        timeseries_data[endpoint] = endpoint_data
        
    return timeseries_data


@app.get("/api/dashboard", tags=["Dashboard"])
def get_dashboard_data():
    """
    Tổng hợp và trả về tất cả dữ liệu cần thiết cho dashboard.
    """
    if not db_client or not redis_client:
        raise HTTPException(status_code=503, detail="Database or Redis connection is unavailable.")
    
    try:
        task_logs = get_task_logs_from_db()
        api_total_counts = get_api_total_counts()
        
        endpoints_with_data = list(api_total_counts.keys())
        api_timeseries = get_api_timeseries_counts(endpoints_with_data)

        return {
            "task_logs": task_logs,
            "api_total_counts": api_total_counts,
            "api_timeseries": api_timeseries
        }
        
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




