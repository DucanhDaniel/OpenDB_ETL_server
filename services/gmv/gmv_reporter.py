import requests
import time
import random
from datetime import datetime, date
from calendar import monthrange
from ..exceptions import TaskCancelledException

class GMVReporter:
    
    """
    Lớp cơ sở cho tất cả các loại TikTok GMV Reporter.
    Chịu trách nhiệm cho việc xác thực, gọi API chung, báo cáo tiến trình và hủy tác vụ.
    """
    PERFORMANCE_API_URL = "https://business-api.tiktok.com/open_api/v1.3/gmv_max/report/get/"
    PRODUCT_API_URL = "https://business-api.tiktok.com/open_api/v1.3/store/product/get/"
    BC_API_URL = "https://business-api.tiktok.com/open_api/v1.3/bc/get/"
    def __init__(self, access_token: str, advertiser_id: str, store_id: str,
                 progress_callback=None, job_id: str = None, redis_client=None):

        if not all([access_token, advertiser_id, store_id]):
            raise ValueError("access_token, advertiser_id, và store_id không được để trống.")
            
        # Thuộc tính chung
        self.access_token = access_token
        self.advertiser_id = advertiser_id
        self.store_id = store_id
        
        # Session dùng chung cho các request
        self.session = requests.Session()
        self.session.headers.update({
            "Access-Token": self.access_token,
            "Content-Type": "application/json",
        })

        # Thuộc tính cho cơ chế throttling và backoff
        self.throttling_delay = 0.0
        self.recovery_factor = 0.8

        # Thuộc tính cho việc kiểm soát tác vụ nền
        self.progress_callback = progress_callback
        self.job_id = job_id
        self.redis_client = redis_client
        self.cancel_key = f"job:{self.job_id}:cancel_requested" if self.job_id else None
        
        print("------------------------- cancel_key \n", self.cancel_key)
        print("------------------------- redis_client \n", redis_client)

    # --- Các phương thức điều khiển tác vụ ---
    def _check_for_cancellation(self):
        """Kiểm tra Redis xem có yêu cầu dừng không. Nếu có, raise Exception."""
        if self.redis_client and self.cancel_key and self.redis_client.exists(self.cancel_key):
            self.redis_client.delete(self.cancel_key)
            raise TaskCancelledException()
            
    def _report_progress(self, message: str, progress: int = 0):
        """Hàm tiện ích để gọi callback nếu nó tồn tại."""
        if self.progress_callback:
            self.progress_callback(status="RUNNING", message=message, progress=progress)
            
    @staticmethod
    def _generate_monthly_date_chunks(start_date_str, end_date_str):
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
        chunks = []
        cursor = date(start_date.year, start_date.month, 1)
        while cursor <= end_date:
            _, last_day = monthrange(cursor.year, cursor.month)
            month_end = date(cursor.year, cursor.month, last_day)
            chunks.append({
                'start': max(cursor, start_date).strftime('%Y-%m-%d'),
                'end': min(month_end, end_date).strftime('%Y-%m-%d')
            })
            next_month = cursor.month + 1
            next_year = cursor.year
            if next_month > 12: next_month, next_year = 1, next_year + 1
            cursor = date(next_year, next_month, 1)
        return chunks
        
    @staticmethod
    def _chunk_list(data, size):
        for i in range(0, len(data), size):
            yield data[i:i + size]
            
    def _make_api_request_with_backoff(self, url: str, params: dict, max_retries: int = 6, base_delay: int = 3) -> dict | None:
        """Thực hiện gọi API với cơ chế thử lại (exponential backoff) và throttling."""
        self._check_for_cancellation()
        if self.throttling_delay > 0:
            print(f"  [THROTTLING] Áp dụng delay hãm tốc {self.throttling_delay:.2f} giây.")
            time.sleep(self.throttling_delay)
        
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, params=params, timeout=60)
                response.raise_for_status()
                data = response.json()
                
                if data.get("code") == 0: 
                    # Giảm dần delay nếu yêu cầu thành công
                    self.throttling_delay *= self.recovery_factor
                    if self.throttling_delay < 0.1: self.throttling_delay = 0
                    return data
                
                # Xử lý các lỗi cụ thể từ API
                error_message = data.get("message", "")
                if "Too many requests" in error_message or "Request too frequent" in error_message:
                    print(f"  [RATE LIMIT] Gặp lỗi (lần {attempt + 1}/{max_retries})...")
                elif "Internal time out" in error_message:
                    print(f"  [TIME OUT] Gặp lỗi (lần {attempt + 1}/{max_retries})...")
                else:
                    print(f"  [LỖI API] {error_message}")
                    # Không thử lại với các lỗi không thể phục hồi
                    if ("permission" not in error_message):
                        raise Exception(f"[LỖI API KHÔNG THỂ PHỤC HỒI] {error_message}")
                    return None # Trả về None cho lỗi quyền truy cập
            
            except requests.exceptions.RequestException as e:
                print(f"  [LỖI MẠNG] (lần {attempt + 1}/{max_retries}): {e}")
            
            if attempt < max_retries - 1:
                delay = (base_delay ** (attempt + 1)) + random.uniform(0, 1)
                self.throttling_delay = delay  # Kích hoạt throttling
                print(f"  Thử lại sau {delay:.2f} giây.")
                time.sleep(delay)

        print("  [THẤT BẠI] Đã thử lại tối đa.")
        raise Exception("Hết số lần thử, vui lòng kiểm tra kết nối hoặc trạng thái API và thử lại sau.")

    def _fetch_all_pages(self, url: str, params: dict) -> list:
        """Lấy dữ liệu từ tất cả các trang của một endpoint API."""
        all_results, current_page = [], 1
        while True:
            self._check_for_cancellation()
            params['page'] = current_page
            data = self._make_api_request_with_backoff(url, params)
            if not data or data.get("code") != 0: break
            
            page_data = data.get("data", {})
            # Linh hoạt lấy list kết quả từ các key khác nhau
            result_list = page_data.get("list", []) or page_data.get("store_products", [])
            all_results.extend(result_list)
            
            total_pages = page_data.get("page_info", {}).get("total_page", 1)
            print(f"  [PHÂN TRANG] Đã lấy trang {current_page}/{total_pages}...")
            
            if current_page >= total_pages: break
            current_page += 1
            time.sleep(1.2) # Delay nhỏ giữa các trang để tránh bị block
        return all_results

    # def _get_bc_ids(self) -> list[str]:
    #     """Lấy danh sách Business Center ID."""
    #     print("Đang lấy danh sách BC ID...")
    #     try:
    #         response = requests.get(self.BC_API_URL, headers={'Access-Token': self.access_token})
    #         response.raise_for_status()
    #         data = response.json()
    #         if data.get("code") == 0:
    #             bc_list = data.get("data", {}).get("list", [])
    #             bc_ids = [bc.get("bc_info", {}).get("bc_id") for bc in bc_list if bc.get("bc_info", {}).get("bc_id")]
    #             print(f"Đã lấy thành công {len(bc_ids)} BC ID.")
    #             self._report_progress(f"Đã lấy thành công {len(bc_ids)} BC ID.", 80)
    #             return bc_ids
    #     except requests.exceptions.RequestException as e:
    #         print(f"Lỗi kết nối khi lấy BC ID: {e}")
    #         raise Exception(f"Lỗi kết nối khi lấy BC ID: {e}")
    #     print("Không thể lấy danh sách BC ID.")
    #     raise Exception("Không thể lấy danh sách BC ID.")
    
    def _get_bc_ids(self) -> list[str]:
        """Lấy danh sách Business Center ID."""
        print("Đang lấy danh sách BC ID...")
        
        # SỬA LỖI: Luôn dùng phương thức đã được chuẩn hóa.
        # Phương thức này đã có sẵn backoff, throttling, VÀ KIỂM TRA HỦY.
        data = self._make_api_request_with_backoff(self.BC_API_URL, params={})
        
        if data and data.get("code") == 0:
            bc_list = data.get("data", {}).get("list", [])
            bc_ids = [bc.get("bc_info", {}).get("bc_id") for bc in bc_list if bc.get("bc_info", {}).get("bc_id")]
            print(f"Đã lấy thành công {len(bc_ids)} BC ID.")
            # Dòng _report_progress này có thể không cần nữa nếu bạn đã báo cáo trong get_data
            # self._report_progress(f"Đã lấy thành công {len(bc_ids)} BC ID.", 80)
            return bc_ids
            
        print("Không thể lấy danh sách BC ID.")
        # Bạn có thể raise Exception hoặc trả về list rỗng tùy logic mong muốn
        raise Exception("Không thể lấy danh sách BC ID.")