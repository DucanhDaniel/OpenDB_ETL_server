# Hướng dẫn Thêm Module Mới (New Platform Integration)

Tài liệu này hướng dẫn quy trình thêm một module mới (ví dụ: `Google Ads`, `TikTok Ads`) vào hệ thống report server, tương đương với module `facebook` hiện tại.

Quy trình gồm 3 bước chính:

1.  **Service Layer**: Viết logic lấy dữ liệu từ API.
2.  **Worker Layer**: Viết Worker để điều phối việc lấy data, xử lý cache và ghi sheet.
3.  **Registry**: Đăng ký Worker mới vào Factory.

---

## 1. Service Layer (Logic lấy dữ liệu)

Tạo thư mục mới trong `services/`, ví dụ `services/google_ads/`.
Tạo file `google_processor.py` chứa class xử lý việc gọi API.

**Cấu trúc khuyến nghị:**

```python
# services/google_ads/google_processor.py

class GoogleAdsReporter:
    def __init__(self, api_key, progress_callback=None):
        self.api_key = api_key
        # Callback để báo cáo tiến độ về Worker (quan trọng)
        # Signature: (status, message, percentage, api_usage)
        self.progress_callback = progress_callback
        
        # Tracking API Usage
        self.api_usage = {
            "request_count": 0,
            "total_rows": 0
        }

    def get_data(self, date_chunks):
        """
        Hàm chính để lấy dữ liệu theo các khoảng thời gian.
        
        Args:
            date_chunks: List [{"start": "2024-01-01", "end": "2024-01-31"}, ...]
        
        Returns:
            List[Dict]: Danh sách các bản ghi dữ liệu (Raw Data)
        """
        all_data = []
        
        for i, chunk in enumerate(date_chunks):
            # 1. Gọi API lấy dữ liệu
            data = self._fetch_from_api(chunk['start'], chunk['end'])
            all_data.extend(data)
            
            # 2. Báo cáo tiến độ
            percent = int((i + 1) / len(date_chunks) * 100)
            if self.progress_callback:
                self.progress_callback(
                    status="RUNNING", 
                    message=f"Đã lấy dữ liệu {chunk['start']} - {chunk['end']}", 
                    progress=percent,
                    api_usage=self.api_usage
                )
                
        return all_data

    def _fetch_from_api(self, start_date, end_date):
        # Logic gọi API thực tế
        # return [{"date": "2024-01-01", "clicks": 100, ...}]
        pass
```

> [!IMPORTANT]
> **Lưu ý về API Usage Tracking:**
>
> 1.  **Tracking theo thời gian thực (Real-time):** Bạn cần truyền tham số `api_usage` vào hàm `progress_callback` như ví dụ trên để hệ thống cập nhật DB liên tục khi task đang chạy.
> 2.  **Tổng hợp cuối cùng (Final Aggregation):** Ngay cả khi bạn *không* truyền vào callback, Worker vẫn sẽ tự động lấy giá trị từ thuộc tính `self.api_usage` của Reporter khi task kết thúc để lưu lần cuối.
>
> **Khuyên dùng:** Hay khai báo `self.api_usage` trong `__init__` và cập nhật nó đầy đủ. Việc truyền vào callback là tùy chọn nhưng được khuyến khích để theo dõi tiến độ tốt hơn.

---

## 2. Worker Layer (Celery Worker)

Tạo file worker mới trong `workers/`, ví dụ `workers/google_ads_worker.py`.
Class này **BẮT BUỘC** phải kế thừa từ `BaseReportWorker`.

```python
# workers/google_ads_worker.py

from typing import Dict, List, Any
from .base_report_worker import BaseReportWorker
from services.google_ads.google_processor import GoogleAdsReporter

class GoogleAdsWorker(BaseReportWorker):
    
    def _create_reporter(self):
        """Khởi tạo Service Reporter đã viết ở Bước 1"""
        return GoogleAdsReporter(
            api_key=self.context.get("api_key"),
            progress_callback=self._send_progress # Hàm callback có sẵn từ Base Class
        )
    
    def _get_collection_name(self) -> str:
        """Tên collection MongoDB để lưu cache"""
        return "google_ads_daily_reports"
    
    def _get_cache_query(self, chunk: Dict[str, str]) -> Dict:
        """Định nghĩa query để tìm cache trong MongoDB"""
        return {
            "account_id": self.context.get("account_id"),
            "start_date": chunk['start'],
            "end_date": chunk['end']
        }
    
    def _flatten_data(self, raw_data: List[Dict], context: Dict) -> List[Dict]:
        """
        Chuyển đổi dữ liệu Raw từ API thành dạng phẳng (Flat Dict) để ghi vào Google Sheet.
        Mỗi dict trong list trả về sẽ là một dòng trong Sheet.
        """
        flattened = []
        for item in raw_data:
            row = {
                "Date": item.get("date"),
                "Campaign Name": item.get("campaign", {}).get("name"),
                "Impressions": item.get("metrics", {}).get("impressions"),
                "Spend": item.get("metrics", {}).get("cost"),
                # Thêm các trường cần thiết để khớp với _get_cache_query và Cache logic
                # (Quan trọng để Cache Hit hoạt động đúng)
                "account_id": context.get("account_id"),
                "start_date": item.get("date"),
                "end_date": item.get("date"),
            }
            flattened.append(row)
        return flattened
```

---

## 3. Registry (Đăng ký Worker)

Mở file `workers/worker_factory.py` và đăng ký worker mới.

```python
# workers/worker_factory.py

# 1. Import Worker mới
from .google_ads_worker import GoogleAdsWorker

class WorkerFactory:
    
    WORKER_REGISTRY = {
        "facebook_daily": FacebookDailyWorker,
        # ... các worker cũ
        
        # 2. Thêm Key vào Registry
        "google_ads": GoogleAdsWorker, 
    }
```

## 4. Sử dụng

Khi gọi task Celery `run_report_job`, chỉ cần truyền `task_type="google_ads"` trong `context`.

```python
context = {
    "job_id": "job_123",
    "task_id": "task_abc",
    "task_type": "google_ads",  # Khớp với key trong Registry
    "spreadsheet_id": "...",
    "start_date": "2024-01-01",
    "end_date": "2024-01-31",
    "api_key": "...",
    # ...
}
run_report_job.delay(context)
```

## Tóm tắt luồng chạy (BaseReportWorker)

Hệ thống sẽ tự động thực hiện các bước sau khi bạn kế thừa `BaseReportWorker`:

1.  **Initialize**: Gọi `_create_reporter`.
2.  **Check Cache**: Dùng `_get_cache_query` để tìm dữ liệu có sẵn trong DB.
3.  **Fetch API**: Với những khoảng thời gian chưa có cache, gọi `reporter.get_data()`.
4.  **Flatten**: Gọi `_flatten_data` để chuẩn hóa dữ liệu API.
5.  **Save Cache**: Lưu dữ liệu mới fetch được vào DB (`_get_collection_name`).
6.  **Write Sheet**: Ghi tất cả dữ liệu (Cache + New) vào Google Sheet.
7.  **Logging**: Tự động update progress và API usage (nếu reporter có gửi `api_usage`).

---

## 5. Database Schema (`task_logs`)

Khi một task được chạy, hệ thống sẽ tự động ghi log vào collection `task_logs` trong MongoDB. Dưới đây là các fields quan trọng:

| Field | Type | Description |
| :--- | :--- | :--- |
| `_id` | ObjectId | MongoDB ID tự sinh. |
| `job_id` | String | ID duy nhất của job (do client tạo). |
| `celery_task_id` | String | ID của Celery Task. |
| `task_type` | String | Loại task (e.g., `facebook_daily`, `google_ads`). |
| `user_email` | String | Email người yêu cầu. |
| `status` | String | Trạng thái hiện tại: `STARTED`, `RUNNING`, `SUCCESS`, `FAILED`, `CANCELLED`. |
| `date_start` | String | Ngày bắt đầu lấy dữ liệu (YYYY-MM-DD). |
| `date_stop` | String | Ngày kết thúc lấy dữ liệu (YYYY-MM-DD). |
| `start_time` | Date | Thời gian task bắt đầu chạy. |
| `end_time` | Date | Thời gian task kết thúc. |
| `duration_seconds` | Float | Tổng thời gian chạy (giây). |
| `message` | String | Thông báo trạng thái hoặc lỗi cuối cùng. |
| `api_total_counts` | Object | Thông tin sử dụng API (Request count, rows written...). Được cập nhật realtime nếu Reporter gửi lên. |
| `full_logs` | String | Toàn bộ log text (stdout/stderr) của task. |
| `accounts` | Array | (Optional) Danh sách accounts được xử lý. |
| `template_name` | String | (Optional) Tên template báo cáo. |

