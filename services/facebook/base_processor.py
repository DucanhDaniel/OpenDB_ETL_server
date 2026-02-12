"""
Facebook Batch Reporter - Base Class
Xử lý batch requests đến Facebook Graph API với rate limit backoff thông minh
"""

import requests
import json
import time
from typing import List, Dict, Any, Optional, Callable
from .constant import FACEBOOK_REPORT_TEMPLATES_STRUCTURE, CONVERSION_METRICS_MAP
from datetime import datetime, timedelta
from collections import defaultdict
import logging
from services.facebook.err_handler.rate_limit import EnhancedBackoffHandler

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FacebookBatchReporter")


class FacebookAdsBaseReporter:
    """
    Base class cho việc lấy dữ liệu từ Facebook Graph API sử dụng batch requests.
    Hỗ trợ rate limit backoff, retry logic, và pagination.
    """
    
    # Constants
    API_VERSION = "v24.0"
    BATCH_API_URL = "http://facebook_batch_server:8010/batch"  # FastAPI endpoint
    # BATCH_API_URL = "http://localhost:8010/batch"
    MAX_BACKOFF_SECONDS = 360  # 6 phút
    DEFAULT_BATCH_SIZE = 20
    DEFAULT_SLEEP_TIME = 10  # seconds
    MAX_RETRIES = 3
    MAX_PAGES_PER_RETRY = 10
    PLUS_BACKOFF_SEC = 3 # Thời gian đệm thêm khi backoff
    
    def __init__(
        self, 
        access_token: str, 
        api_version: str = API_VERSION,
        batch_api_url: str = BATCH_API_URL,
        email: Optional[str] = None,
        progress_callback: Optional[Callable] = None,
        job_id: Optional[str] = None
    ):
        """
        Khởi tạo Facebook Batch Reporter.
        
        Args:
            access_token: Facebook access token
            api_version: Facebook API version (default: v24.0)
            batch_api_url: URL của FastAPI batch endpoint
            email: Email để tracking (optional)
            progress_callback: Callback function để report progress (optional)
            job_id: Job ID để tracking log (optional)
        """
        self.access_token = access_token
        self.api_version = api_version
        self.batch_api_url = batch_api_url
        self.email = email or "unknown@example.com"
        self.progress_callback = progress_callback
        self.job_id = job_id
        
        self.summaries = []
        self.batch_count = 0
        self.total_backoff_sec = 0        
        # Stats
        self.total_rows_written = 0
        self.request_count = 0

        self.backoff_handler = EnhancedBackoffHandler(reporter=self)

        
    def _report_progress(self, message: str, percentage: int = None):
        """Report progress nếu có callback"""
        api_usage = {
            "summaries" : self.summaries,
            "batch_count": self.batch_count,
            "total_backoff_sec": self.total_backoff_sec,
            "total_rows_written": self.total_rows_written,
            "request_count": self.request_count
        }

        if self.progress_callback:
            self.progress_callback(status = "RUNNING", message = message, progress = percentage, api_usage = api_usage)
        
        # Log with job_id prefix if available
        prefix = f"[Job {self.job_id}] " if self.job_id else ""
        logger.info(f"{prefix}{message}")
    
    # ==================== RATE LIMIT BACKOFF ====================
    
    def _calculate_backoff_time(self, rate_limit_summary: Dict[str, Any]) -> Dict[str, Any]:
        """
        Tính toán thời gian backoff dựa trên rate limit info.
        
        Returns:
            {
                "should_backoff": bool,
                "backoff_seconds": int,
                "reason": str
            }
        """
        if not rate_limit_summary or "rate_limits" not in rate_limit_summary:
            return {"should_backoff": False, "backoff_seconds": 0, "reason": None}
        
        rate_limits = rate_limit_summary["rate_limits"]
        max_backoff_seconds = 0
        backoff_reason = None
        
        # 1. Kiểm tra app-level usage
        app_usage = rate_limits.get("app_usage_pct", 0)
        if app_usage >= 95:
            max_backoff_seconds = max(max_backoff_seconds, 300)  # 5 phút
            backoff_reason = f"App usage cao: {app_usage}%"
        elif app_usage >= 75:
            max_backoff_seconds = max(max_backoff_seconds, 60)  # 1 phút
            backoff_reason = f"App usage vừa phải: {app_usage}%"
        
        # 2. Kiểm tra account-level limits
        account_details = rate_limits.get("account_details", [])
        for account in account_details:
            # Insights usage
            insights_usage = account.get("insights_usage_pct", 0)
            if insights_usage >= 95:
                max_backoff_seconds = max(max_backoff_seconds, 300)
                backoff_reason = f"Account {account['account_id']} insights usage cao: {insights_usage}%"
            elif insights_usage >= 75:
                max_backoff_seconds = max(max_backoff_seconds, 60)
                backoff_reason = f"Account {account['account_id']} insights usage vừa: {insights_usage}%"
            
            # ETA từ business use cases
            eta = account.get("eta_seconds", 0)
            if eta > max_backoff_seconds:
                max_backoff_seconds = eta
                backoff_reason = f"Account {account['account_id']} yêu cầu chờ {eta}s"
        
        return {
            "should_backoff": max_backoff_seconds > 0,
            "backoff_seconds": max_backoff_seconds,
            "reason": backoff_reason
        }
    
    def _perform_backoff_if_needed(self, summary: Dict[str, Any]):
        """Thực hiện backoff nếu cần, throw error nếu quá lâu"""
        backoff_info = self._calculate_backoff_time(summary)
        
        if not backoff_info["should_backoff"]:
            return
        
        if backoff_info["backoff_seconds"] > self.MAX_BACKOFF_SECONDS:
            error_msg = (
                f"Rate limit backoff quá lâu ({backoff_info['backoff_seconds']}s > {self.MAX_BACKOFF_SECONDS}s). "
                f"Lý do: {backoff_info['reason']}"
            )
            logger.error(error_msg)
            self._report_progress(message = error_msg)
            raise Exception(error_msg)
        
        logger.warning(f"⚠ Rate limit detected. Chờ {backoff_info['backoff_seconds']}s. Lý do: {backoff_info['reason']}")
        self._report_progress(message = f"⚠ Rate limit detected. Chờ {backoff_info['backoff_seconds']}s. Lý do: {backoff_info['reason']}")
        time.sleep(backoff_info['backoff_seconds'] + self.PLUS_BACKOFF_SEC) 
        logger.info("✓ Backoff hoàn tất, tiếp tục xử lý.")
        self._report_progress(message = "✓ Backoff hoàn tất, tiếp tục xử lý.")
    
    # ==================== BATCH API CALLS ====================
    
    def _send_batch_request(
        self, 
        relative_urls: List[str],
        request_id: str = None
    ) -> Dict[str, Any]:
        """
        Gửi batch request đến FastAPI endpoint.
        
        Returns:
            {
                "status": "success",
                "results": [...],
                "summary": {
                    "success_count": int,
                    "error_count": int,
                    "rate_limits": {...}
                }
            }
        """
        logger.info(relative_urls)

        if not request_id:
            request_id = f"req_{int(time.time())}"
        
        payload = {
            "access_token": self.access_token,
            "relative_urls": relative_urls,
            "email": self.email
        }
        
        try:
            response = requests.post(
                self.batch_api_url,
                json=payload,
                timeout=180 # Timeout khi gọi batch API 3 phút
            )
            response.raise_for_status()
            
            data = response.json()
            self.batch_count += 1
            self.request_count += len(relative_urls)
            
            # print(data)
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Batch request failed: {e}")
            raise
    
    def _execute_single_batch(
        self, 
        urls_for_batch: List[str],
        batch_metadata: List[Dict],
        batch_number: int,
        wave_number: int
    ) -> List[Dict[str, Any]]:
        """
        Gửi một batch với retry logic.
        
        Returns:
            List of responses với metadata attached
        """
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                self._report_progress(f"  → Gửi batch {batch_number} ({len(urls_for_batch)} requests)...")
                
                response_json = self._send_batch_request(urls_for_batch)
                
                if not response_json or "results" not in response_json:
                    raise Exception("Invalid response from batch server.")
                
                # Attach metadata vào từng response
                responses_with_metadata = []
                for res in response_json["results"]:
                    idx = res["request_index"]
                    res["metadata"] = batch_metadata[idx]["metadata"]
                    res["original_url"] = batch_metadata[idx]["url"]
                    responses_with_metadata.append(res)
                
                logger.info(f"  ✓ Batch {batch_number} thành công.")
                
                if "summary" in response_json:
                    summary_with_time = response_json.get("summary")
                    summary_with_time["timestamp"] = datetime.now().isoformat()
                    self.summaries.append(summary_with_time)
                
                if hasattr(self, 'backoff_handler'):
                    # print("Tồn tại summary: ", response_json["summary"])
                    self.total_backoff_sec += self.backoff_handler.analyze_and_backoff(
                        responses=responses_with_metadata,
                        summary=response_json.get("summary")
                    )
                else:
                    # Fallback to old logic if backoff_handler not initialized
                    if "summary" in response_json:
                        # print("Tồn tại summary: ", response_json["summary"])
                        self._perform_backoff_if_needed(response_json["summary"])
                
                return responses_with_metadata
                
            except Exception as e:
                logger.warning(f"  ✗ Batch {batch_number} lỗi (lần {attempt}/{self.MAX_RETRIES}): {e}")
                
                if attempt >= self.MAX_RETRIES:
                    raise Exception(f"Batch {batch_number} thất bại sau {self.MAX_RETRIES} lần thử: {e}")
                
                # Exponential backoff
                sleep_time = (2 ** attempt) * 2
                logger.info(f"  ⏳ Chờ {sleep_time}s trước khi retry...")
                time.sleep(sleep_time)
    
    def _execute_wave(
        self,
        requests_for_wave: List[Dict],
        batch_size: int,
        sleep_time: float,
        wave_number: int
    ) -> List[Dict[str, Any]]:
        """
        Xử lý một wave (nhiều batches).
        
        Returns:
            List of all responses from wave
        """
        all_responses = []
        batch_count = (len(requests_for_wave) + batch_size - 1) // batch_size
        
        logger.info(f"\n===== SÓNG {wave_number}: {len(requests_for_wave)} requests, {batch_count} batches =====")
        
        for i in range(0, len(requests_for_wave), batch_size):
            batch_slice = requests_for_wave[i:i + batch_size]
            urls_for_batch = [req["url"] for req in batch_slice]
            batch_number = (i // batch_size) + 1
            
            batch_responses = self._execute_single_batch(
                urls_for_batch,
                batch_slice,
                batch_number,
                wave_number
            )
            
            all_responses.extend(batch_responses)
            
            # Sleep giữa các batches (trừ batch cuối)
            if i + batch_size < len(requests_for_wave):
                time.sleep(sleep_time)
        
        return all_responses
    
    # ==================== HELPER FUNCTIONS ====================
    
    @staticmethod
    def _get_relative_url(absolute_url: str) -> str:
        """Chuyển đổi absolute URL thành relative URL"""
        if not absolute_url:
            return ""
        
        try:
            from urllib.parse import urlparse, parse_qs, urlencode
            
            parsed = urlparse(absolute_url)
            # Remove version prefix
            path = parsed.path
            for version in ["v24.0", "v23.0", "v25.0"]:
                path = path.replace(f"/{version}/", "")
            
            # Remove access_token from query
            query_params = parse_qs(parsed.query)
            query_params.pop('access_token', None)
            
            query_string = urlencode(query_params, doseq=True)
            relative_url = path.lstrip('/')
            
            if query_string:
                relative_url += '?' + query_string
            
            return relative_url
            
        except Exception as e:
            logger.warning(f"Cannot parse URL: {absolute_url}. Error: {e}")
            return absolute_url
    
    @staticmethod
    def _chunk_list(lst: List, chunk_size: int) :
        """Chia list thành các chunks nhỏ hơn"""
        for i in range(0, len(lst), chunk_size):
            yield lst[i:i + chunk_size]
    
    @staticmethod
    def _generate_monthly_date_chunks(start_date: str, end_date: str, factor: int = 1) -> List[Dict[str, str]]:
        """
        Chia khoảng thời gian thành các chunks theo tháng.
        Có thể chia nhỏ tháng thành nhiều phần nếu factor > 1.
        
        Args:
            start_date: YYYY-MM-DD
            end_date: YYYY-MM-DD
            factor: Hệ số chia nhỏ (mặc định 1 = không chia nhỏ)
            
        Returns:
            List of {"start": "YYYY-MM-DD", "end": "YYYY-MM-DD"}
        """
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        chunks = []
        current_start = start
        
        while current_start <= end:
            # Chunk end là cuối tháng hoặc end_date (Logic cũ)
            current_month = current_start.month
            current_year = current_start.year
            
            # Tính ngày cuối tháng
            if current_month == 12:
                next_month = datetime(current_year + 1, 1, 1)
            else:
                next_month = datetime(current_year, current_month + 1, 1)
            
            # Ngày cuối cùng của đoạn tháng này (chặn bởi end_date)
            month_chunk_end = next_month - timedelta(days=1)
            if month_chunk_end > end:
                month_chunk_end = end
                
            # --- Logic mới: Chia nhỏ month_chunk dựa trên factor ---
            # Khoảng thời gian thực tế của chunk tháng này
            days_in_chunk = (month_chunk_end - current_start).days + 1
            
            if factor > 1 and days_in_chunk > 1:
                # Tính kích thước mỗi sub-chunk (làm tròn lên)
                sub_chunk_days = (days_in_chunk + factor - 1) // factor
                
                temp_start = current_start
                while temp_start <= month_chunk_end:
                    temp_end = temp_start + timedelta(days=sub_chunk_days - 1)
                    if temp_end > month_chunk_end:
                        temp_end = month_chunk_end
                        
                    chunks.append({
                        "start": temp_start.strftime("%Y-%m-%d"),
                        "end": temp_end.strftime("%Y-%m-%d")
                    })
                    
                    temp_start = temp_end + timedelta(days=1)
            else:
                # Giữ nguyên nếu không cần chia
                chunks.append({
                    "start": current_start.strftime("%Y-%m-%d"),
                    "end": month_chunk_end.strftime("%Y-%m-%d")
                })
            
            # Move to next month logic
            current_start = next_month
        
        return chunks
    
    @staticmethod
    def get_facebook_template_config_by_name(name):
        """
        Tìm kiếm config của template dựa trên tên trong cấu trúc dữ liệu Facebook report.
        """
        # Kiểm tra biến toàn cục có tồn tại và phải là một danh sách (list)
        if not FACEBOOK_REPORT_TEMPLATES_STRUCTURE or not isinstance(FACEBOOK_REPORT_TEMPLATES_STRUCTURE, list):
            return None
        
        for group in FACEBOOK_REPORT_TEMPLATES_STRUCTURE:
            # Lấy danh sách templates, dùng .get() để tránh lỗi nếu key không tồn tại
            templates = group.get('templates')
            
            # Kiểm tra nếu templates tồn tại và là một danh sách
            if templates and isinstance(templates, list):
                # Duyệt qua từng template để tìm tên trùng khớp
                for t in templates:
                    # print(t.get('name') == name)
                    if t.get('name') == name:
                        return t.get('config')
        
        return None
    
    def get_accessible_page_map(self):
        """
        Lấy danh sách các Page mà user có quyền truy cập và trả về một dictionary
        map từ Page ID -> Page Name.
        """
        url = "https://graph.facebook.com/v24.0/me/accounts"
        
        # Các tham số query string
        params = {
            "fields": "id,name",
            "limit": 50,
            "access_token": self.access_token
        }

        try:
            response = requests.get(url, params=params)
            
            # Kiểm tra nếu request bị lỗi HTTP (4xx, 5xx) thì ném ra exception
            response.raise_for_status()
            
            data_json = response.json()
            page_map = {}

            # Duyệt qua mảng data (Tương đương response.data.forEach)
            if "data" in data_json:
                for page in data_json["data"]:
                    page_id = page.get("id")
                    page_name = page.get("name")
                    
                    # Đảm bảo cả ID và Name đều tồn tại trước khi map
                    if page_id and page_name:
                        page_map[page_id] = page_name

            return page_map

        except Exception as e:
            print(f"Không thể lấy danh sách Page. Báo cáo có thể thiếu Tên Page. Lỗi: {e}")
            return {}
        
    def _extract_value_from_list(self, data_list: List[Dict], action_type: str) -> float:
        """Helper để lấy value từ list các actions dựa trên action_type."""
        if not isinstance(data_list, list):
            return 0.0
        
        item = next(
            (x for x in data_list if x.get("action_type") == action_type), 
            None
        )
        return float(item.get("value", 0)) if item else 0.0

    def _flatten_action_metrics(
        self,
        row: Dict[str, Any],
        selected_fields: List[str]
    ) -> Dict[str, Any]:
        """
        Flatten các action metrics phức tạp (Video, Actions, Cost, etc.)
        Logic dựa trên CONVERSION_METRICS_MAP.
        """
        new_row = {**row}
        
        for friendly_name, metric_info in CONVERSION_METRICS_MAP.items():
            # 1. Chỉ xử lý nếu người dùng chọn
            if friendly_name not in selected_fields:
                continue
            
            api_field = metric_info.get("api_field")
            parent_field = metric_info.get("parent_field")
            target_action_type = metric_info.get("action_type")
            
            value = 0.0

            # TRƯỜNG HỢP 1: Cấu hình rõ ràng action_type trong MAP (Ưu tiên cao nhất)
            # Ví dụ: "Video Views (75%)" -> parent: video_p75_watched_actions, type: video_view
            if parent_field and target_action_type:
                data_list = row.get(parent_field, [])
                value = self._extract_value_from_list(data_list, target_action_type)

            # TRƯỜNG HỢP 2: Parse từ api_field có dấu ":" (Logic cũ)
            # Ví dụ: "actions:comment" -> parent: actions, type: comment
            elif api_field and ":" in api_field:
                parts = api_field.split(":")
                if len(parts) == 2:
                    p_field = parts[0] # actions, cost_per_action_type, ...
                    a_type = parts[1]  # comment, like, ...
                    
                    data_list = row.get(p_field, [])
                    value = self._extract_value_from_list(data_list, a_type)

            # TRƯỜNG HỢP 3: Special Case cho purchase_roas (nếu không được định nghĩa rõ trong Map)
            elif api_field == "purchase_roas":
                data_list = row.get("purchase_roas", [])
                value = self._extract_value_from_list(data_list, "omni_purchase")

            # TRƯỜNG HỢP 4: Giá trị trực tiếp (Scalar value)
            # Ví dụ: "Inline link clicks", "impressions", "spend"
            elif api_field:
                raw_val = row.get(api_field, 0)
                try:
                    value = float(raw_val)
                except (ValueError, TypeError):
                    value = 0.0
            
            # Gán giá trị vào row mới
            new_row[friendly_name] = value

        # Cleanup: Xóa các trường list gốc để file output gọn gàng
        # Danh sách các trường kỹ thuật cần dọn dẹp
        technical_fields_to_remove = {
            "actions", "action_values", "cost_per_action_type", "purchase_roas",
            "video_p25_watched_actions", "video_p50_watched_actions", 
            "video_p75_watched_actions", "video_p95_watched_actions", 
            "video_p100_watched_actions", "video_30_sec_watched_actions",
            "video_avg_time_watched_actions", "video_play_actions",
            "video_thruplay_watched_actions", "cost_per_thruplay",
            "outbound_clicks", "outbound_clicks_ctr", "unique_outbound_clicks"
        }

        for field in technical_fields_to_remove:
            new_row.pop(field, None)
            
        return new_row
        
    @staticmethod
    def _reduce_time_range_in_url(url: str, reduction_factor: int = 2) -> Dict[str, Any]:
        """
        Giảm time range trong URL khi gặp lỗi "reduce the amount of data".
        Hỗ trợ cả URL encoded (%27, %22) và non-encoded.
        """
        try:
            import re
            from datetime import datetime, timedelta
            
            # Q = Quote Pattern: Bắt dấu nháy đơn ('), kép ("), hoặc encoded (%27, %22) hoặc không có gì
            Q = r"(?:['\"]|%27|%22)?"
            
            # Regex chi tiết:
            # 1. time_range theo sau là ( hoặc =( hoặc %28
            # 2. Bắt đầu json bằng { hoặc %7B
            # 3. Tìm key 'since' (được bao bởi Q)
            # 4. Tìm dấu : hoặc %3A
            # 5. Capture Group 1: Ngày bắt đầu (YYYY-MM-DD)
            # 6. Tìm dấu , hoặc %2C
            # 7. Tìm key 'until' (được bao bởi Q)
            # 8. Capture Group 2: Ngày kết thúc (YYYY-MM-DD)
            regex = (
                r"time_range(?:[=\(]|%28)(?:%7B|\{).*?"
                # Sửa dòng dưới: Thêm 'r' trước 'f' để thành rf"..."
                rf"{Q}since{Q}(?:%3A|:)\s*{Q}(\d{{4}}-\d{{2}}-\d{{2}}){Q}"
                r".*?(?:%2C|,).*?"
                # Sửa dòng dưới: Thêm 'r' trước 'f' để thành rf"..."
                rf"{Q}until{Q}(?:%3A|:)\s*{Q}(\d{{4}}-\d{{2}}-\d{{2}}){Q}"
                r".*?(?:%7D|\})(?:\)|%29)?"
            )
            
            match = re.search(regex, url, re.IGNORECASE)
            
            if not match:
                logger.warning(f"Cannot parse time_range from URL: {url[:100]}...")
                return {"urls": [url], "date_chunks": []}
            
            original_match_string = match.group(0)
            start_date_str = match.group(1)
            end_date_str = match.group(2)
            
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
            
            # Tính số ngày
            total_days = (end_date - start_date).days + 1
            
            # Nếu chỉ có 1 ngày thì không chia được nữa
            if total_days <= 1:
                logger.warning("Time range is already 1 day, cannot reduce further.")
                return {"urls": [url], "date_chunks": [{"start": start_date_str, "end": end_date_str}]}

            chunk_days = max(1, total_days // reduction_factor)
            
            logger.info(f" -> Chia time range: {total_days} ngày thành {reduction_factor} chunks (~{chunk_days} ngày/chunk)")
            
            # Tạo các chunks
            chunks = []
            current_start = start_date
            
            while current_start <= end_date:
                current_end = current_start + timedelta(days=chunk_days - 1)
                # Đảm bảo chunk cuối cùng không vượt quá end_date gốc
                if current_end > end_date or (current_end + timedelta(days=1) > end_date):
                    current_end = end_date
                
                chunks.append({
                    "start": current_start.strftime("%Y-%m-%d"),
                    "end": current_end.strftime("%Y-%m-%d")
                })
                
                current_start = current_end + timedelta(days=1)
            
            # Tạo URLs mới cho từng chunk
            urls = []
            for chunk in chunks:
                # Thay thế dates trong original match string
                # Lưu ý: Vì Regex chỉ capture số ngày (YYYY-MM-DD) mà không capture dấu nháy encoded,
                # nên việc replace chuỗi ngày thuần túy vẫn hoạt động đúng bên trong chuỗi encoded.
                new_segment = original_match_string
                new_segment = new_segment.replace(start_date_str, chunk["start"], 1)
                new_segment = new_segment.replace(end_date_str, chunk["end"], 1)
                
                new_url = url.replace(original_match_string, new_segment, 1)
                urls.append(new_url)
            
            return {"urls": urls, "date_chunks": chunks}
            
        except Exception as e:
            logger.error(f"Error reducing time range: {e}")
            return {"urls": [url], "date_chunks": []}