import requests
import time
import random
from datetime import datetime, date, timedelta, timezone
from calendar import monthrange
from ..exceptions import TaskCancelledException
from concurrent.futures import ThreadPoolExecutor
from ..rate_limiter.rate_limiter import RedisRateLimiter
from collections import defaultdict

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
        
        self.api_usage = defaultdict(int)
        
        if self.redis_client:
            gmv_rules = [
                (2, 1), # 2 request mỗi giây
                (45, 60) # 45 request mỗi phút
            ]
            self.gmv_limiter = RedisRateLimiter(redis_client, rules = gmv_rules)
            
            basic_rules = [
                (8, 1), # 8 request mỗi giây
                (550, 60) # 550 requests mỗi phút
            ]
            self.basic_limiter = RedisRateLimiter(redis_client, rules = basic_rules)

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
    
    def _generate_weekly_date_chunks(start_date_str: str, end_date_str: str) -> list:
        """
        Chia một khoảng thời gian thành các chunk nhỏ, mỗi chunk dài 7 ngày.
        Chunk cuối cùng có thể ngắn hơn 7 ngày.
        """
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
        
        chunks = []
        cursor = start_date
        
        while cursor <= end_date:
            # Ngày bắt đầu của chunk là con trỏ hiện tại
            chunk_start = cursor
            
            # Ngày kết thúc của chunk là 6 ngày sau đó, nhưng không được vượt quá end_date tổng
            chunk_end = min(cursor + timedelta(days=6), end_date)
            
            chunks.append({
                'start': chunk_start.strftime('%Y-%m-%d'),
                'end': chunk_end.strftime('%Y-%m-%d')
            })
            
            # Di chuyển con trỏ đến ngày bắt đầu của chunk tiếp theo (7 ngày sau)
            cursor += timedelta(days=7)
            
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
                if self.redis_client:
                    self.check_rate_limit(url)
                response = self.session.get(url, params=params, timeout=60)
                response.raise_for_status()
                data = response.json()
                
                if data.get("code") == 0: 
                    # Giảm dần delay nếu yêu cầu thành công
                    self.throttling_delay *= self.recovery_factor
                    if (self.throttling_delay > 190):
                        self.throttling_delay = 0 # recover sau 5 phút đợi
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
            finally:
                self.log_api_counter(url)
                
            if attempt < max_retries - 1:
                delay = (base_delay ** (attempt + 1)) + random.uniform(0, 1)
                self.throttling_delay = delay  # Kích hoạt throttling
                print(f"  Thử lại sau {delay:.2f} giây.")
                time.sleep(delay)

        print("  [THẤT BẠI] Đã thử lại tối đa.")
        raise Exception("Hết số lần thử, vui lòng kiểm tra kết nối hoặc trạng thái API và thử lại sau.")

    def log_api_counter(self, url):
        
        self.api_usage[url] += 1
            
        if self.redis_client:
                try:
                    api_call_key = f"api_calls_total:{url}"
                    self.redis_client.incr(api_call_key)
                    
                    # Tạo key theo giờ, ví dụ: 'api_calls:gmv_report:2025-10-06-17'
                    current_hour = datetime.now(timezone.utc).strftime('%Y-%m-%d-%H')
                    api_call_key = f"api_calls:{url}:{current_hour}"
                    
                    # Dùng pipeline để đảm bảo 2 lệnh được thực hiện nguyên tử
                    pipe = self.redis_client.pipeline()
                    pipe.incr(api_call_key)
                    pipe.expire(api_call_key, 3600 * 24) # Giữ dữ liệu trong 24 giờ
                    pipe.execute()
                except Exception as e:
                    print(f"WARNING: Không thể ghi log API call: {e}")
                    
    def _fetch_all_tiktok_products(self, bc_id: str) -> list:
        """Lấy tất cả sản phẩm từ một Business Center ID cụ thể."""
        print(f"--- Bắt đầu lấy dữ liệu sản phẩm cho BC ID: {bc_id} ---")
        params = {'bc_id': bc_id, 'store_id': self.store_id, 'page_size': 100, 'advertiser_id': self.advertiser_id, 'filtering': '{"ad_creation_eligible":"GMV_MAX"}'}
        try:
            all_products = self._fetch_all_pages(self.PRODUCT_API_URL, params, max_threads=5)
        except Exception as e:
            print(e)
            all_products = []
        print(f"--- Hoàn tất lấy sản phẩm cho BC ID: {bc_id}. Tổng cộng: {len(all_products)} sản phẩm. ---")
        self._report_progress(f"Đã lấy tổng cộng: {len(all_products)} sản phẩm.")
        return all_products
   
    
    def _fetch_all_pages(self, url: str, params: dict, max_threads = 1, throttling_delay = None) -> list:
        """
        Lấy dữ liệu từ tất cả các trang của một endpoint API.
        Sử dụng đa luồng để tăng tốc nếu max_threads > 1.
        """
        all_results = []
        
        # --- BƯỚC 1: LUÔN LẤY TRANG ĐẦU TIÊN ĐỂ LẤY total_pages ---
        first_page_params = params.copy()
        first_page_params['page'] = 1
        
        if (throttling_delay):
            self.throttling_delay = throttling_delay
        first_page_data = self._make_api_request_with_backoff(url, first_page_params)

        if not first_page_data or first_page_data.get("code") != 0:
            return [] # Trả về rỗng nếu có lỗi ngay trang đầu
        
        page_data = first_page_data.get("data", {})
        result_list = page_data.get("list", []) or page_data.get("store_products", [])
        all_results.extend(result_list)
        
        total_pages = page_data.get("page_info", {}).get("total_page", 1)
        print(f"   [PHÂN TRANG] Lấy trang 1/{total_pages}. Tổng số trang: {total_pages}.")

        if total_pages <= 1:
            return all_results

        # --- BƯỚC 2: LẤY CÁC TRANG CÒN LẠI ĐỒNG THỜI (NẾU max_threads > 1) ---
        
        pages_to_fetch = list(range(2, total_pages + 1))

        def fetch_page(page_num):
            """Hàm con để lấy dữ liệu của một trang cụ thể."""
            self._check_for_cancellation()
            page_params = params.copy()
            page_params['page'] = page_num
            
            if throttling_delay:
                self.throttling_delay = throttling_delay
            data = self._make_api_request_with_backoff(url, page_params)
            
            if data and data.get("code") == 0:
                page_data = data.get("data", {})
                results = page_data.get("list", []) or page_data.get("store_products", [])
                print(f"   [PHÂN TRANG] Đã lấy xong trang {page_num}/{total_pages}.")
                return results
            print(f"   [PHÂN TRANG] Lỗi khi lấy trang {page_num}/{total_pages}.")
            return []

        # Sử dụng ThreadPoolExecutor để chạy các request đồng thời
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            # executor.map sẽ chạy hàm fetch_page cho mỗi phần tử trong pages_to_fetch
            results_from_threads = executor.map(fetch_page, pages_to_fetch)
            
            # Gom kết quả từ các luồng
            for result in results_from_threads:
                all_results.extend(result)
        
        return all_results

    
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
    
    def check_rate_limit(self, url):
        # Rate limiter cho GMV MAX
        
        rate_limit_key = f"ratelimit:{self.advertiser_id}:{url}"
        if (url == self.PERFORMANCE_API_URL) :
            self.check_limiter(self.gmv_limiter, rate_limit_key)
        self.check_limiter(self.basic_limiter, rate_limit_key)
            
    def check_limiter(self, limiter : RedisRateLimiter, key : str):
        while not limiter.acquire(key):
            print(f"  [RATE LIMITER] Đã đạt giới hạn, đang chờ 1 giây...")
            time.sleep(1)