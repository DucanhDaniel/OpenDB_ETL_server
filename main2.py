from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
import logging
import requests
from typing import List, Dict, Any, Optional

from services.gmv.campaign_creative_detail import GMVCampaignCreativeDetailReporter
from services.gmv.campaign_product_detail import GMVCampaignProductDetailReporter

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="TikTok Reporting API",
    description="API to fetch and process performance data from TikTok GMV Max.",
    version="1.2.0"
)

# --- NEW: Data Flattening Logic ---

def _flatten_product_report(
    campaign_data_list: List[Dict[str, Any]],
    context: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Flattens the raw product report data into a list of rows, ready for a spreadsheet.
    This logic mirrors the Google Apps Script _flattenTiktokProductReport function.
    """
    flattened_data = []
    for campaign in campaign_data_list:
        if not campaign.get("performance_data"):
            continue

        for perf_data in campaign["performance_data"]:
            row = {
                # General info from context
                "start_date": context.get("start_date"),
                "end_date": context.get("end_date"),
                "advertiser_id": context.get("advertiser_id"),
                "advertiser_name": context.get("advertiser_name"),
                "store_id": context.get("store_id"),
                "store_name": context.get("store_name"),

                # Campaign info
                "campaign_id": campaign.get("campaign_id"),
                "campaign_name": campaign.get("campaign_name"),
                "operation_status": campaign.get("operation_status"),
                "bid_type": campaign.get("bid_type"),

                # Product info and dimensions
                "item_group_id": perf_data.get("dimensions", {}).get("item_group_id"),
                "stat_time_day": perf_data.get("dimensions", {}).get("stat_time_day"),
                "product_name": perf_data.get("product_info", {}).get("title"),
                "product_image_url": perf_data.get("product_info", {}).get("product_image_url"),
                "product_status": perf_data.get("product_info", {}).get("status"),
                "product_img": perf_data.get("product_info", {}).get("product_image_url"),
            }
            # Add all metrics dynamically
            row.update(perf_data.get("metrics", {}))
            flattened_data.append(row)
            
    return flattened_data

def _flatten_creative_report(
    campaign_data_list: List[Dict[str, Any]],
    context: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Flattens the raw creative report data into a list of rows.
    This logic mirrors the Google Apps Script _flattenTiktokCreativeReport function.
    """
    flattened_data = []
    for campaign in campaign_data_list:
        if not campaign.get("performance_data"):
            continue
        
        for perf_group in campaign["performance_data"]:
            if not perf_group.get("creative_details"):
                continue

            for creative in perf_group["creative_details"]:
                row = {
                    # General info from context
                    "start_date": context.get("start_date"),
                    "end_date": context.get("end_date"),
                    "advertiser_id": context.get("advertiser_id"),
                    "advertiser_name": context.get("advertiser_name"),
                    "store_id": context.get("store_id"),
                    "store_name": context.get("store_name"),

                    # Campaign info
                    "campaign_id": campaign.get("campaign_id"),
                    "campaign_name": campaign.get("campaign_name"),
                    "operation_status": campaign.get("operation_status"),

                    # Product Group & Details info
                    "item_group_id": perf_group.get("dimensions", {}).get("item_group_id"),
                    "product_name": perf_group.get("product_details", {}).get("product_title"),
                    "product_status": perf_group.get("product_details", {}).get("product_status"),
                    "product_image_url": perf_group.get("product_details", {}).get("product_image_url"),
                    
                    # Creative Info
                    "item_id": creative.get("item_id"),
                    "title": creative.get("metadata", {}).get("title"),
                    "tt_account_name": creative.get("metadata", {}).get("tt_account_name"),
                    "tt_account_profile_image_url": creative.get("metadata", {}).get("tt_account_profile_image_url"),
                    "product_img": creative.get("metadata", {}).get("product_img") or perf_group.get("product_details", {}).get("product_image_url"),
                }
                # Add all metrics dynamically
                row.update(creative.get("metrics", {}))
                flattened_data.append(row)
                
    return flattened_data


# --- Helper function for background task ---

def process_report_and_callback(context: Dict[str, Any]):
    """
    This function runs in the background. It fetches, flattens, and then
    sends the data back to the Google Apps Script Web App.
    """
    job_id = context["job_id"]
    task_type = context["task_type"]
    callback_url = context["callback_url"]
    
    logger.info(f"[Job ID: {job_id}] Background task started for type: {task_type}.")
    
    try:
        # 1. Select the correct reporter and flattening function
        if task_type == "creative":
            reporter = GMVCampaignCreativeDetailReporter(
                access_token=context["access_token"],
                advertiser_id=context["advertiser_id"],
                store_id=context["store_id"]
            )
            flatten_function = _flatten_creative_report
        elif task_type == "product":
            reporter = GMVCampaignProductDetailReporter(
                access_token=context["access_token"],
                advertiser_id=context["advertiser_id"],
                store_id=context["store_id"]
            )
            flatten_function = _flatten_product_report
        else:
            raise ValueError("Invalid task type specified.")

        # 2. Fetch the raw data
        raw_data = reporter.get_data(context["start_date"], context["end_date"])
        logger.info(f"[Job ID: {job_id}] Successfully fetched raw data.")

        # 3. Flatten the data
        flattened_data = flatten_function(raw_data, context)
        logger.info(f"[Job ID: {job_id}] Successfully flattened into {len(flattened_data)} rows.")

        # 4. Prepare payload for the callback
        callback_payload = {
            "job_id": job_id,
            "status": "SUCCESS",
            "data": flattened_data
        }

    except Exception as e:
        logger.error(f"[Job ID: {job_id}] Error during data processing: {e}", exc_info=True)
        callback_payload = { "job_id": job_id, "status": "FAILED", "error_message": str(e), "data": [] }

    # 5. Send the flattened data back to the Apps Script Web App
    try:
        logger.info(f"[Job ID: {job_id}] Sending flattened data to callback URL: {callback_url}")
        response = requests.post(callback_url, json=callback_payload, timeout=60)
        response.raise_for_status()
        logger.info(f"[Job ID: {job_id}] Callback sent successfully.")
    except requests.exceptions.RequestException as e:
        logger.error(f"[Job ID: {job_id}] Failed to send callback to Apps Script: {e}", exc_info=True)


# --- API Endpoint ---

@app.get("/reports/create-job", tags=["Async Jobs"])
def create_report_job(
    background_tasks: BackgroundTasks,
    # Task definition
    task_type: str = Query(..., description="Type of the report. Must be 'creative' or 'product'."),
    callback_url: str = Query(..., description="The Google Apps Script Web App URL."),
    job_id: str = Query(..., description="A unique ID for this job, generated by the client."),
    # Auth and IDs
    access_token: str = Query(..., description="Tiktok access token"),
    advertiser_id: str = Query(..., description="Ad account ID"),
    store_id: str = Query(..., description="Shop ID"),
    # Date Range
    start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD format"),
    # Optional context for enriching data
    advertiser_name: Optional[str] = Query(None, description="Advertiser name for data enrichment."),
    store_name: Optional[str] = Query(None, description="Store name for data enrichment.")
):
    """
    Creates a background job to fetch and flatten report data.
    Responds immediately and sends the data later to the callback_url.
    """
    logger.info(f"Received async job request. Job ID: {job_id}, Type: {task_type}")
    if task_type not in ["creative", "product"]:
        raise HTTPException(status_code=400, detail="Invalid 'task_type'.")

    # Group all params into a context dictionary to pass to the background task
    context = {
        "task_type": task_type, "callback_url": callback_url, "job_id": job_id,
        "access_token": access_token, "advertiser_id": advertiser_id, "store_id": store_id,
        "start_date": start_date, "end_date": end_date,
        "advertiser_name": advertiser_name, "store_name": store_name
    }
    
    background_tasks.add_task(process_report_and_callback, context)

    return {
        "status": "processing",
        "job_id": job_id,
        "message": "Job accepted. Data will be fetched, flattened, and sent to the callback URL."
    }
    
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