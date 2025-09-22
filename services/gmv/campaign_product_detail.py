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

    # --- PHẦN 1: CÁC PHƯƠNG THỨC LẤY DỮ LIỆU SẢN PHẨM ---

    def _get_bc_ids(self) -> list[str]:
        """Lấy danh sách BC ID."""
        print(">> Bước 1A: Đang lấy danh sách BC ID...")
        headers = {'Access-Token': self.access_token}
        try:
            response = requests.get(self.BC_API_URL, headers=headers)
            response.raise_for_status()
            data = response.json()
            if data.get("code") == 0:
                bc_list = data.get("data", {}).get("list", [])
                bc_ids = [bc["bc_info"]["bc_id"] for bc in bc_list if bc.get("bc_info")]
                print(f"   -> Đã lấy thành công {len(bc_ids)} BC ID.")
                return bc_ids
            else:
                print(f"   -> Lỗi API khi lấy BC ID: {data.get('message')}")
        except requests.exceptions.RequestException as e:
            print(f"   -> Lỗi kết nối khi lấy BC ID: {e}")
        return []

    def _fetch_products_from_bc_id(self, bc_id: str) -> list | None:
        """Lấy tất cả sản phẩm cho một bc_id cụ thể."""
        all_products = []
        current_page = 1
        total_pages = 1
        print(f">> Bước 1B: Thử lấy sản phẩm với BC ID: {bc_id}...")
        
        while current_page <= total_pages:
            params = {'bc_id': bc_id, 'store_id': self.store_id, 'page': current_page, 'page_size': 100}
            try:
                response = self.session.get(self.PRODUCT_API_URL, params=params)
                response.raise_for_status()
                data = response.json()
                if data.get("code") != 0:
                    print(f"   -> Lỗi: {data.get('message')}. BC ID này không có quyền.")
                    return None  # BC ID không hợp lệ
                
                api_data = data.get("data", {})
                products = api_data.get("store_products", [])
                all_products.extend(products)

                if current_page == 1:
                    total_pages = api_data.get("page_info", {}).get("total_page", 1)
                
                print(f"   -> Đã lấy trang {current_page}/{total_pages}. Tổng sản phẩm: {len(all_products)}")
                current_page += 1
                time.sleep(1.5) # Thêm độ trễ nhỏ
            except requests.exceptions.RequestException as e:
                print(f"   -> Đã xảy ra lỗi khi gọi API: {e}")
                raise Exception(f"Lỗi API: {e} Vui lòng thử lại sau.")
        return all_products

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

    # --- PHẦN 2: CÁC PHƯƠNG THỨC LẤY DỮ LIỆU CAMPAIGN ---

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
    
    def _make_api_request_with_backoff(self, params, max_retries=5, base_delay=3, base_url = PERFORMANCE_API_URL):
        for attempt in range(max_retries):
            try:
                response = self.session.get(base_url, params=params, timeout=45)
                response.raise_for_status()
                data = response.json()
                if data.get("code") == 0: return data
                print(f"   [LỖI API] {data.get('message')}")
                if ("Too many requests" not in data.get("message", "")) and ("time out" not in data.get("message", "")) and ("You don't have permission to the asset" not in data.get("message", "")): 
                    raise Exception(f"Lỗi { data.get("message", "")}")
            except requests.exceptions.RequestException as e:
                print(f"   [LỖI MẠNG] (lần {attempt + 1}): {e}")
            delay = (base_delay ** attempt) + random.uniform(0, 1)
            time.sleep(delay)
        return None

    def _fetch_all_pages(self, params):
        all_results, page = [], 1
        while True:
            params['page'] = page
            data = self._make_api_request_with_backoff(params)
            if not data: break
            page_data = data.get("data", {})
            all_results.extend(page_data.get("list", []))
            total_pages = page_data.get("page_info", {}).get("total_page", 1)
            if page >= total_pages: break
            page += 1
        return all_results
    
    def _get_all_campaigns(self, start_date, end_date):
        params = {
            "advertiser_id": self.advertiser_id, "store_ids": json.dumps([self.store_id]),
            "start_date": start_date, "end_date": end_date,
            "dimensions": json.dumps(["campaign_id"]),
            "metrics": json.dumps(["campaign_name", "operation_status", "bid_type"]),
            "filtering": json.dumps({"gmv_max_promotion_types": ["PRODUCT"]}), "page_size": 1000,
        }
        items = self._fetch_all_pages(params)
        return {
            item["dimensions"]["campaign_id"]: item["metrics"]
            for item in items
        }

    def _fetch_data_for_batch(self, campaign_batch, start_date, end_date):
        batch_ids = list(campaign_batch.keys())
        params = {
            "advertiser_id": self.advertiser_id, "store_ids": json.dumps([self.store_id]),
            "start_date": start_date, "end_date": end_date,
            "dimensions": json.dumps(["campaign_id", "item_group_id", "stat_time_day"]),
            "metrics": json.dumps(["orders", "gross_revenue", "cost", "cost_per_order", "roi"]),
            "filtering": json.dumps({"campaign_ids": batch_ids}), "page_size": 1000,
        }
        perf_list = self._fetch_all_pages(params)
        
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
        enriched_results = []
        for campaign in campaign_results:
            if not campaign.get("performance_data"):
                continue
            
            for perf_record in campaign["performance_data"]:
                item_id = perf_record.get("dimensions", {}).get("item_group_id")
                if item_id:
                    perf_record["product_info"] = product_map.get(item_id, {"title": "Không tìm thấy thông tin"})
            enriched_results.append(campaign)
        print("   -> Đã gộp dữ liệu thành công.")
        return enriched_results

    def get_data(self, start_date: str, end_date: str) -> list:
        """
        Hàm chính để chạy toàn bộ quy trình: lấy sản phẩm, lấy hiệu suất
        chiến dịch, và gộp chúng lại.

        Args:
            start_date (str): Ngày bắt đầu (YYYY-MM-DD).
            end_date (str): Ngày kết thúc (YYYY-MM-DD).

        Returns:
            list: Danh sách dữ liệu chiến dịch đã được làm giàu thông tin sản phẩm.
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
            
            with ThreadPoolExecutor(max_workers=4) as executor:
                future_to_batch = {
                    executor.submit(self._fetch_data_for_batch, dict(batch), chunk['start'], chunk['end']): batch
                    for batch in batches
                }
                for future in as_completed(future_to_batch):
                    try:
                        all_campaign_results.extend(future.result())
                    except Exception as e:
                        print(f"Lỗi khi xử lý một lô: {e}")
                        raise Exception(f"Lỗi: {e}, Vui lòng thử lại sau.")

        # BƯỚC 3: Gộp dữ liệu
        final_data = self._enrich_campaign_data(all_campaign_results, product_map)
        return final_data

if __name__ == "__main__":
    # --- CẤU HÌNH ---
    ACCESS_TOKEN = os.getenv("TIKTOK_ACCESS_TOKEN")
    ADVERTISER_ID = "6967547145545105410"
    STORE_ID = "7494600253418473607"
    START_DATE = "2025-06-01"
    END_DATE = "2025-09-18"

    start_time = time.perf_counter()
    if not ACCESS_TOKEN:
        print("LỖI: Vui lòng thiết lập biến môi trường TIKTOK_ACCESS_TOKEN trong file .env")
    else:
        try:
            # 1. Khởi tạo reporter
            reporter = GMVCampaignProductDetailReporter(
                access_token=ACCESS_TOKEN,
                advertiser_id=ADVERTISER_ID,
                store_id=STORE_ID
            )

            # 2. Gọi hàm get_data để lấy kết quả
            enriched_results = reporter.get_data(start_date=START_DATE, end_date=END_DATE)

            # 3. Xử lý kết quả trả về
            if enriched_results:
                print("\n--- BƯỚC 4: LƯU KẾT QUẢ ---")
                output_filename = "GMV_Campaign_product_detail.json"
                with open(output_filename, "w", encoding="utf-8") as f:
                    json.dump(enriched_results, f, ensure_ascii=False, indent=4)
                print(f"   -> Đã lưu kết quả vào file '{output_filename}'")
                
                # Tính tổng chi phí
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