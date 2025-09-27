import requests
import json
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date
from calendar import monthrange
from dotenv import load_dotenv
import os

# Tải các biến môi trường một lần khi module được import
load_dotenv()

class GMVCampaignProductDetailReporter:
    """
    Lấy và kết hợp dữ liệu hiệu suất chiến dịch với thông tin chi tiết sản phẩm
    từ TikTok Marketing API.
    
    Đã được nâng cấp với cơ chế backoff và throttling để tăng độ ổn định.
    """
    # --- CÁC HẰNG SỐ API ---
    BC_API_URL = "https://business-api.tiktok.com/open_api/v1.3/bc/get/"
    PRODUCT_API_URL = "https://business-api.tiktok.com/open_api/v1.3/store/product/get/"
    PERFORMANCE_API_URL = "https://business-api.tiktok.com/open_api/v1.3/gmv_max/report/get/"

    def __init__(self, access_token: str, advertiser_id: str, store_id: str):
        """
        Khởi tạo reporter.

        Args:
            access_token (str): Access token để xác thực với API.
            advertiser_id (str): ID của tài khoản quảng cáo.
            store_id (str): ID của cửa hàng TikTok Shop.
        """
        if not all([access_token, advertiser_id, store_id]):
            raise ValueError("access_token, advertiser_id, và store_id không được để trống.")
            
        self.access_token = access_token
        self.advertiser_id = advertiser_id
        self.store_id = store_id
        
        self.session = requests.Session()
        self.session.headers.update({
            "Access-Token": self.access_token,
            "Content-Type": "application/json",
        })

        # Thuộc tính cho cơ chế throttling và backoff
        self.throttling_delay = 0.0
        self.recovery_factor = 0.8 # Giảm delay đi 20% sau mỗi lần thành công

    # --- PHẦN 1: CÁC PHƯƠNG THỨC TIỆN ÍCH VÀ GỌI API CỐT LÕI ---

    def _make_api_request_with_backoff(self, url: str, params: dict, max_retries: int = 6, base_delay: int = 3) -> dict | None:
        """Thực hiện gọi API với cơ chế thử lại (exponential backoff) và throttling."""
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

    @staticmethod
    def _chunk_list(data, size):
        for i in range(0, len(data), size):
            yield data[i:i + size]

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
        
    # --- PHẦN 2: CÁC PHƯƠNG THỨC LẤY DỮ LIỆU CỤ THỂ ---

    def _get_bc_ids(self) -> list[str]:
        """Lấy danh sách BC ID."""
        print(">> Bước 1A: Đang lấy danh sách BC ID...")
        data = self._make_api_request_with_backoff(self.BC_API_URL, params={})
        if data and data.get("code") == 0:
            bc_list = data.get("data", {}).get("list", [])
            bc_ids = [bc["bc_info"]["bc_id"] for bc in bc_list if bc.get("bc_info")]
            print(f"   -> Đã lấy thành công {len(bc_ids)} BC ID.")
            return bc_ids
        print("   -> Lỗi hoặc không lấy được BC ID.")
        return []

    def _fetch_products_from_bc_id(self, bc_id: str) -> list | None:
        """Lấy tất cả sản phẩm cho một bc_id cụ thể bằng cách sử dụng _fetch_all_pages."""
        print(f">> Bước 1B: Thử lấy sản phẩm với BC ID: {bc_id}...")
        params = {'bc_id': bc_id, 'store_id': self.store_id, 'page_size': 100}
        
        # Thử gọi trang đầu tiên để kiểm tra quyền
        first_page_data = self._make_api_request_with_backoff(self.PRODUCT_API_URL, {**params, 'page': 1})
        
        # SỬA LỖI TẠI ĐÂY: Xử lý trường hợp `first_page_data` có thể là `None`
        if not first_page_data or first_page_data.get("code") != 0:
            error_msg = "Không có quyền hoặc không nhận được phản hồi hợp lệ"
            if first_page_data:
                error_msg = first_page_data.get('message', error_msg)
            
            print(f"   -> Lỗi: {error_msg}. BC ID này không hợp lệ.")
            return None
        
        # Nếu trang đầu tiên OK, tiếp tục lấy tất cả các trang
        print("   -> Quyền hợp lệ. Bắt đầu lấy tất cả sản phẩm...")
        return self._fetch_all_pages(self.PRODUCT_API_URL, params)
    
    def _get_product_map(self) -> dict | None:
        """Lấy toàn bộ sản phẩm và chuyển thành một dictionary để tra cứu nhanh."""
        print("\n--- BƯỚC 1: LẤY VÀ CHUẨN BỊ DỮ LIỆU SẢN PHẨM ---")
        bc_ids = self._get_bc_ids()
        if not bc_ids:
            return None

        all_products = []
        for bc_id in bc_ids:
            products_list = self._fetch_products_from_bc_id(bc_id)
            if products_list is not None:
                print(f"   => THÀNH CÔNG! Tìm thấy BC ID hợp lệ: {bc_id}. Đã lấy {len(products_list)} sản phẩm.")
                all_products = products_list
                break 
        
        if not all_products:
            print("   -> Không tìm thấy BC ID nào có thể truy cập sản phẩm của store này.")
            return None

        print("\n>> Bước 1C: Tạo bản đồ sản phẩm để tra cứu nhanh...")
        product_map = {p['item_group_id']: p for p in all_products}
        print(f"   -> Đã tạo bản đồ cho {len(product_map)} sản phẩm độc nhất.")
        return product_map

    def _get_all_campaigns(self, start_date, end_date):
        """Lấy tất cả campaign trong một khoảng thời gian."""
        params = {
            "advertiser_id": self.advertiser_id, "store_ids": json.dumps([self.store_id]),
            "start_date": start_date, "end_date": end_date,
            "dimensions": json.dumps(["campaign_id"]),
            "metrics": json.dumps(["campaign_name", "operation_status", "bid_type"]),
            "filtering": json.dumps({"gmv_max_promotion_types": ["PRODUCT"]}), "page_size": 1000,
        }
        items = self._fetch_all_pages(self.PERFORMANCE_API_URL, params)
        return {
            item["dimensions"]["campaign_id"]: item["metrics"]
            for item in items
        }

    def _fetch_data_for_batch(self, campaign_batch, start_date, end_date):
        """Lấy dữ liệu hiệu suất chi tiết cho một lô campaign."""
        batch_ids = list(campaign_batch.keys())
        params = {
            "advertiser_id": self.advertiser_id, "store_ids": json.dumps([self.store_id]),
            "start_date": start_date, "end_date": end_date,
            "dimensions": json.dumps(["campaign_id", "item_group_id", "stat_time_day"]),
            "metrics": json.dumps(["orders", "gross_revenue", "cost", "cost_per_order", "roi"]),
            "filtering": json.dumps({"campaign_ids": batch_ids}), "page_size": 1000,
        }
        perf_list = self._fetch_all_pages(self.PERFORMANCE_API_URL, params)
        
        results = {}
        for cid, info in campaign_batch.items():
            results[cid] = {
                "campaign_id": cid, "campaign_name": info.get("campaign_name"),
                "operation_status": info.get("operation_status"), "bid_type": info.get("bid_type"),
                "performance_data": []
            }
        
        for record in perf_list:
            cid = record["dimensions"]["campaign_id"]
            if cid in results:
                results[cid]["performance_data"].append(record)
        return list(results.values())

    # --- PHẦN 3: PHƯƠNG THỨC CHÍNH VÀ GỘP DỮ LIỆU ---

    def _enrich_campaign_data(self, campaign_results, product_map):
        print("\n--- BƯỚC 3: GỘP DỮ LIỆU SẢN PHẨM VÀO CAMPAIGN ---")
        if not product_map:
            print("   -> Cảnh báo: Không có bản đồ sản phẩm. Dữ liệu sẽ không được làm giàu.")
            return campaign_results
            
        enriched_results = []
        unique_campaigns = {}

        for campaign in campaign_results:
            campaign_id = campaign.get("campaign_id")
            if not campaign_id: continue

            # Gộp các record của cùng một campaign lại
            if campaign_id not in unique_campaigns:
                unique_campaigns[campaign_id] = campaign
            else:
                unique_campaigns[campaign_id]["performance_data"].extend(campaign.get("performance_data", []))

        for campaign in unique_campaigns.values():
            if not campaign.get("performance_data"):
                continue
            
            for perf_record in campaign["performance_data"]:
                item_id = perf_record.get("dimensions", {}).get("item_group_id")
                if item_id:
                    perf_record["product_info"] = product_map.get(item_id, {"title": f"Không tìm thấy thông tin cho ID {item_id}"})
            enriched_results.append(campaign)
            
        print("   -> Đã gộp dữ liệu thành công.")
        return enriched_results

    def get_data(self, start_date: str, end_date: str) -> list:
        """
        Hàm chính để chạy toàn bộ quy trình: lấy sản phẩm, lấy hiệu suất
        chiến dịch, và gộp chúng lại.
        """
        # BƯỚC 1: Lấy dữ liệu sản phẩm
        product_map = self._get_product_map()
        if not product_map:
            print("Không thể lấy dữ liệu sản phẩm. Dừng thực thi.")
            return []

        # BƯỚC 2: Lấy dữ liệu campaign
        print("\n--- BƯỚC 2: LẤY DỮ LIỆU CAMPAIGN ---")
        date_chunks = self._generate_monthly_date_chunks(start_date, end_date)
        all_campaign_results = []

        for chunk in date_chunks:
            print(f"\n>> Xử lý chunk: {chunk['start']} to {chunk['end']}")
            campaigns = self._get_all_campaigns(chunk['start'], chunk['end'])
            if not campaigns:
                print("   -> Không có campaign nào trong khoảng thời gian này.")
                continue
            
            print(f"   -> Tìm thấy {len(campaigns)} campaigns. Chia thành lô để xử lý...")
            batches = list(self._chunk_list(list(campaigns.items()), 20))
            
            with ThreadPoolExecutor(max_workers=1) as executor:
                future_to_batch = {
                    executor.submit(self._fetch_data_for_batch, dict(batch), chunk['start'], chunk['end']): batch
                    for batch in batches
                }
                for future in as_completed(future_to_batch):
                    try:
                        all_campaign_results.extend(future.result())
                    except Exception as e:
                        print(f"Lỗi khi xử lý một lô: {e}")
                        raise

        # BƯỚC 3: Gộp dữ liệu
        final_data = self._enrich_campaign_data(all_campaign_results, product_map)
        return final_data

# --- HÀM CHÍNH ĐỂ CHẠY ---
if __name__ == "__main__":
    # --- CẤU HÌNH ---
    ACCESS_TOKEN = os.getenv("TIKTOK_ACCESS_TOKEN")
    ADVERTISER_ID = "7137968211592495105"
    STORE_ID = "7494588040522401840"
    START_DATE = "2025-06-01"
    END_DATE = "2025-09-18"

    start_time = time.perf_counter()
    if not ACCESS_TOKEN:
        print("LỖI: Vui lòng thiết lập biến môi trường TIKTOK_ACCESS_TOKEN trong file .env")
    else:
        try:
            reporter = GMVCampaignProductDetailReporter(
                access_token=ACCESS_TOKEN,
                advertiser_id=ADVERTISER_ID,
                store_id=STORE_ID
            )
            enriched_results = reporter.get_data(start_date=START_DATE, end_date=END_DATE)

            if enriched_results:
                print("\n--- BƯỚC 4: LƯU KẾT QUẢ ---")
                output_filename = "GMV_Campaign_product_detail_v2.json"
                with open(output_filename, "w", encoding="utf-8") as f:
                    json.dump(enriched_results, f, ensure_ascii=False, indent=4)
                print(f"   -> Đã lưu kết quả vào file '{output_filename}'")
                
                total_cost = sum(
                    float(perf.get("metrics", {}).get("cost", 0))
                    for campaign in enriched_results
                    for perf in campaign.get("performance_data", [])
                )
                print(f"\n💰 Tổng chi phí của tất cả campaign: {total_cost:,.0f} VND")
            else:
                print("\nKhông có dữ liệu nào để xử lý.")

        except ValueError as ve:
            print(f"Lỗi cấu hình: {ve}")
        except Exception as e:
            print(f"Đã xảy ra lỗi không mong muốn: {e}")

    end_time = time.perf_counter()
    print(f"\n--- HOÀN TẤT ---")
    print(f"Tổng thời gian thực thi: {end_time - start_time:.2f} giây.")