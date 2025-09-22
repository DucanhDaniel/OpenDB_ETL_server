from fastapi import FastAPI, HTTPException, Query
import logging

from services.gmv.campaign_creative_detail import GMVCampaignCreativeDetailReporter
from services.gmv.campaign_product_detail import GMVCampaignProductDetailReporter

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Khởi tạo ứng dụng FastAPI
app = FastAPI(
    title="TikTok Reporting API",
    description="API để lấy dữ liệu hiệu suất từ TikTok GMV Max.",
    version="1.0.0"
)

@app.get("/")
def read_root():
    """Endpoint gốc để kiểm tra API có hoạt động không."""
    return {"status": "ok", "message": "Welcome to TikTok Reporting API!"}


@app.get("/reports/performance-creative", tags=["Reports GMV Details"])
def get_performance_creative_report(
    access_token: str = Query(..., description="Access token của Tiktok"),
    advertiser_id: str = Query(..., description="ID của tài khoản quảng cáo"),
    store_id: str = Query(..., description="ID của shop"),
    start_date: str = Query(..., description="Ngày bắt đầu theo định dạng YYYY-MM-DD"),
    end_date: str = Query(..., description="Ngày kết thúc theo định dạng YYYY-MM-DD")
):
    """
    Endpoint để lấy báo cáo hiệu suất chi tiết đến từng creative.
    """
    logger.info(f"Yêu cầu báo cáo performance-creative từ {start_date} đến {end_date}")
    try:
        reporter = GMVCampaignCreativeDetailReporter(
            access_token=access_token,
            advertiser_id=advertiser_id,
            store_id=store_id
        )
        
        data = reporter.get_data(start_date, end_date)
        
        if not data:
            return {"message": "Không tìm thấy dữ liệu cho khoảng thời gian đã chọn.", "data": []}
            
        return {"data": data}

    except Exception as e:  
        logger.error(f"Lỗi khi xử lý báo cáo performance-creative: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Lỗi máy chủ nội bộ: {str(e)}")


@app.get("/reports/campaign-product", tags=["Reports GMV Details"])
def get_campaign_product_report(
    access_token: str = Query(..., description="Access token của Tiktok"),
    advertiser_id: str = Query(..., description="ID của tài khoản quảng cáo"),
    store_id: str = Query(..., description="ID của shop"),
    start_date: str = Query(..., description="Ngày bắt đầu theo định dạng YYYY-MM-DD"),
    end_date: str = Query(..., description="Ngày kết thúc theo định dạng YYYY-MM-DD")
):
    """
    Endpoint để lấy báo cáo hiệu suất chiến dịch gộp với thông tin sản phẩm.
    """
    logger.info(f"Yêu cầu báo cáo campaign-product từ {start_date} đến {end_date}")
    try:
        reporter = GMVCampaignProductDetailReporter(
            access_token=access_token,
            advertiser_id=advertiser_id,
            store_id=store_id
        )

        data = reporter.get_data(start_date, end_date)
        
        if not data:
            return {"message": "Không tìm thấy dữ liệu cho khoảng thời gian đã chọn.", "data": []}

        return {"data": data}
        
    except Exception as e:
        logger.error(f"Lỗi khi xử lý báo cáo campaign-product: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Lỗi máy chủ nội bộ: {str(e)}")