import requests
import json
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date
from calendar import monthrange
from dotenv import load_dotenv
import os

# Tải các biến môi trường từ file .env
load_dotenv()

# --- CẤU HÌNH ---
ACCESS_TOKEN = os.getenv("TIKTOK_ACCESS_TOKEN")
ADVERTISER_ID = "6967547145545105410"
STORE_ID = "7494600253418473607"
START_DATE = "2025-06-01"
END_DATE = "2025-09-18"
CAMPAIGN_API_URL = "https://business-api.tiktok.com/open_api/v1.3/gmv_max/report/get/"

HEADERS = {
    "Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json",
}


# ==============================================================================
# PHẦN 1: CÁC HÀM LẤY DỮ LIỆU SẢN PHẨM (TỪ SCRIPT PRODUCT)
# ==============================================================================

def get_bc_ids(access_token, max_retries=3, backoff_factor=3):
    """Lấy danh sách BC ID với cơ chế thử lại."""
    url = "https://business-api.tiktok.com/open_api/v1.3/bc/get/"
    headers = {'Access-Token': access_token}
    print(">> Bước 1A: Đang lấy danh sách BC ID...")
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers)
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
        if attempt < max_retries - 1:
            time.sleep(backoff_factor)
    print("   -> Không thể lấy danh sách BC ID sau nhiều lần thử.")
    return []

def fetch_all_tiktok_products(bc_id, store_id, access_token):
    """Lấy tất cả sản phẩm cho một bc_id và store_id cụ thể."""
    base_url = "https://business-api.tiktok.com/open_api/v1.3/store/product/get/"
    headers = {'Access-Token': access_token, 'Content-Type': 'application/json'}
    all_products = []
    current_page = 1
    total_pages = 1
    print(f">> Bước 1B: Thử lấy sản phẩm với BC ID: {bc_id}...")
    while current_page <= total_pages:
        params = {'bc_id': bc_id, 'store_id': store_id, 'page': current_page, 'page_size': 100}
        try:
            response = requests.get(base_url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            if data.get("code") != 0:
                print(f"   -> Lỗi: {data.get('message')}. BC ID này không có quyền.")
                return None # Trả về None để báo hiệu BC ID không hợp lệ
            
            api_data = data.get("data", {})
            products = api_data.get("store_products", [])
            page_info = api_data.get("page_info", {})
            if not products and current_page == 1:
                print(f"   -> Không tìm thấy sản phẩm nào.")
                break
            all_products.extend(products)
            if current_page == 1:
                total_pages = page_info.get("total_page", 1)
            print(f"   -> Đã lấy trang {current_page}/{total_pages}. Tổng sản phẩm hiện tại: {len(all_products)}")
            current_page += 1
        except requests.exceptions.RequestException as e:
            print(f"   -> Đã xảy ra lỗi khi gọi API: {e}")
            return None
    return all_products

def get_product_map(access_token, store_id):
    """
    Lấy toàn bộ sản phẩm và chuyển thành một dictionary để tra cứu nhanh.
    """
    print("\n--- BƯỚC 1: LẤY VÀ CHUẨN BỊ DỮ LIỆU SẢN PHẨM ---")
    bc_ids_list = get_bc_ids(access_token)
    if not bc_ids_list:
        return None

    all_products = []
    for bc_id in bc_ids_list:
        products_list = fetch_all_tiktok_products(bc_id, store_id, access_token)
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

# ==============================================================================
# PHẦN 2: CÁC HÀM LẤY DỮ LIỆU CAMPAIGN (TỪ SCRIPT CAMPAIGN)
# ==============================================================================

def chunk_list(data, size):
    for i in range(0, len(data), size):
        yield data[i:i + size]

def generate_monthly_date_chunks(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    chunks = []
    cursor_date = date(start_date.year, start_date.month, 1)
    while cursor_date <= end_date:
        _, last_day = monthrange(cursor_date.year, cursor_date.month)
        month_end = date(cursor_date.year, cursor_date.month, last_day)
        chunks.append({
            'start': max(cursor_date, start_date).strftime('%Y-%m-%d'),
            'end': min(month_end, end_date).strftime('%Y-%m-%d')
        })
        next_month = cursor_date.month + 1
        next_year = cursor_date.year
        if next_month > 12: next_month, next_year = 1, next_year + 1
        cursor_date = date(next_year, next_month, 1)
    return chunks

def make_api_request_with_backoff(session, params, max_retries=5, base_delay=3):
    for attempt in range(max_retries):
        try:
            response = session.get(CAMPAIGN_API_URL, params=params, timeout=45)
            response.raise_for_status()
            data = response.json()
            if data.get("code") == 0: return data
            print(f"   [LỖI API] {data.get('message')}")
            if "Too many requests" not in data.get("message", ""): return None
        except requests.exceptions.RequestException as e:
            print(f"   [LỖI MẠNG] (lần {attempt + 1}): {e}")
        delay = (base_delay ** attempt) + random.uniform(0, 1)
        time.sleep(delay)
    return None

def fetch_all_pages(session, params):
    all_results, current_page = [], 1
    while True:
        params['page'] = current_page
        data = make_api_request_with_backoff(session, params)
        if not data: break
        page_data = data.get("data", {})
        all_results.extend(page_data.get("list", []))
        total_pages = page_data.get("page_info", {}).get("total_page", 1)
        if current_page >= total_pages: break
        current_page += 1
    return all_results

def get_all_campaigns(session, start_date, end_date):
    params = {
        "advertiser_id": ADVERTISER_ID, "store_ids": json.dumps([STORE_ID]),
        "start_date": start_date, "end_date": end_date,
        "dimensions": json.dumps(["campaign_id"]),
        "metrics": json.dumps(["campaign_name", "operation_status", "bid_type"]),
        "filtering": json.dumps({"gmv_max_promotion_types": ["PRODUCT"]}), "page_size": 1000,
    }
    all_items = fetch_all_pages(session, params)
    campaigns_map = {}
    if all_items:
        for item in all_items:
            cid = item["dimensions"]["campaign_id"]
            metrics = item["metrics"]
            campaigns_map[cid] = {
                "campaign_name": metrics.get("campaign_name"),
                "operation_status": metrics.get("operation_status"),
                "bid_type": metrics.get("bid_type"),
            }
    return campaigns_map

def fetch_data_for_batch(campaign_batch, campaigns_map, start_date, end_date):
    batch_ids = list(campaign_batch.keys())
    params_perf = {
        "advertiser_id": ADVERTISER_ID, "store_ids": json.dumps([STORE_ID]),
        "start_date": start_date, "end_date": end_date,
        "dimensions": json.dumps(["campaign_id", "item_group_id", "stat_time_day"]),
        "metrics": json.dumps(["orders", "gross_revenue", "cost", "cost_per_order", "roi"]),
        "filtering": json.dumps({"campaign_ids": batch_ids}), "page_size": 1000,
    }
    with requests.Session() as session:
        session.headers.update(HEADERS)
        perf_list = fetch_all_pages(session, params_perf)
    
    results = {}
    for cid in batch_ids:
        info = campaigns_map.get(cid, {})
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


# ==============================================================================
# PHẦN 3: HÀM THỰC THI CHÍNH
# ==============================================================================

if __name__ == "__main__":
    start_time = time.perf_counter()
    if not ACCESS_TOKEN:
        print("LỖI: Vui lòng thiết lập biến môi trường TIKTOK_ACCESS_TOKEN trong file .env")
    else:
        # BƯỚC 1: Lấy dữ liệu sản phẩm
        product_map = get_product_map(ACCESS_TOKEN, STORE_ID)

        if product_map:
            # BƯỚC 2: Lấy dữ liệu campaign
            print("\n--- BƯỚC 2: LẤY DỮ LIỆU CAMPAIGN ---")
            date_chunks = generate_monthly_date_chunks(START_DATE, END_DATE)
            all_campaign_results = []

            with requests.Session() as session:
                session.headers.update(HEADERS)
                for chunk in date_chunks:
                    print(f"\n>> Xử lý chunk: {chunk['start']} to {chunk['end']}")
                    campaigns = get_all_campaigns(session, chunk['start'], chunk['end'])
                    if not campaigns:
                        print("   -> Không có campaign nào trong khoảng thời gian này.")
                        continue
                    
                    print(f"   -> Tìm thấy {len(campaigns)} campaigns. Chia thành các lô để xử lý song song...")
                    batches = list(chunk_list(list(campaigns.items()), 20))
                    
                    with ThreadPoolExecutor(max_workers=4) as executor:
                        future_to_batch = {
                            executor.submit(fetch_data_for_batch, dict(batch), campaigns, chunk['start'], chunk['end']): batch
                            for batch in batches
                        }
                        for future in as_completed(future_to_batch):
                            all_campaign_results.extend(future.result())

            # BƯỚC 3: Gộp dữ liệu sản phẩm vào campaign
            print("\n--- BƯỚC 3: GỘP DỮ LIỆU SẢN PHẨM VÀO CAMPAIGN ---")
            enriched_results = []
            for campaign in all_campaign_results:
                if not campaign.get("performance_data"):
                    print(f"   -> Bỏ qua campaign '{campaign['campaign_name']}' vì không có dữ liệu hiệu suất.")
                    continue
                
                for perf_record in campaign["performance_data"]:
                    item_id = perf_record.get("dimensions", {}).get("item_group_id")
                    if item_id:
                        # Gắn thông tin sản phẩm tương ứng vào mỗi bản ghi
                        perf_record["product_info"] = product_map.get(item_id, {"title": "Không tìm thấy thông tin"})
                enriched_results.append(campaign)
            print("   -> Đã gộp dữ liệu thành công.")
            
            # BƯỚC 4: Xuất file
            print("\n--- BƯỚC 4: LƯU KẾT QUẢ ---")
            output_filename = "GMV_Campaign_product_detail.json"
            with open(output_filename, "w", encoding="utf-8") as f:
                json.dump(enriched_results, f, ensure_ascii=False, indent=4)
            print(f"   -> Đã lưu kết quả vào file '{output_filename}'")
            
            total_cost = 0
            for campaign_result in enriched_results:
                for perf_record in campaign_result.get("performance_data", []):
                    try:
                        cost_value = float(perf_record.get("metrics", {}).get("cost", 0))
                        total_cost += cost_value
                    except (ValueError, TypeError):
                        continue
            print(f"\n💰 Tổng chi phí (cost) của tất cả các campaign đã xử lý: {total_cost:,.0f} VND")


    end_time = time.perf_counter()
    print(f"\n--- HOÀN TẤT ---")
    print(f"Tổng thời gian thực thi: {end_time - start_time:.2f} giây.")
    # return enriched_results
