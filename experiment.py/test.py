import requests
import json
import time
import random
from datetime import datetime, date
from calendar import monthrange
from dotenv import load_dotenv
import os

# Tải biến môi trường
load_dotenv()

# --- CẤU HÌNH ---
ACCESS_TOKEN = os.getenv("TIKTOK_ACCESS_TOKEN") 
ADVERTISER_ID = "6967547145545105410"
STORE_ID = "7494600253418473607"
START_DATE = "2025-09-01"
END_DATE = "2025-09-18"
API_URL = "https://business-api.tiktok.com/open_api/v1.3/gmv_max/report/get/"

HEADERS = {
    "Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json",
}

def chunk_list(data, size):
    """Chia một danh sách thành các danh sách con có kích thước `size`."""
    for i in range(0, len(data), size):
        yield data[i:i + size]

def generate_monthly_date_chunks(start_date_str, end_date_str):
    """Chia một khoảng thời gian thành các chunk theo tháng."""
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    chunks = []
    cursor_date = date(start_date.year, start_date.month, 1)
    while cursor_date <= end_date:
        _, last_day_of_month = monthrange(cursor_date.year, cursor_date.month)
        month_end_date = date(cursor_date.year, cursor_date.month, last_day_of_month)
        chunk_start = max(cursor_date, start_date)
        chunk_end = min(month_end_date, end_date)
        chunks.append({
            'start': chunk_start.strftime('%Y-%m-%d'),
            'end': chunk_end.strftime('%Y-%m-%d')
        })
        next_month = cursor_date.month + 1
        next_year = cursor_date.year
        if next_month > 12:
            next_month = 1
            next_year += 1
        cursor_date = date(next_year, next_month, 1)
    return chunks

def make_api_request_with_backoff(session, params, max_retries=5, base_delay=3):
    """Thực hiện gọi API với cơ chế thử lại (exponential backoff)."""
    for attempt in range(max_retries):
        try:
            response = session.get(API_URL, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
            if data.get("code") == 0: return data
            if "Too many requests" in data.get("message", ""):
                print(f"   [RATE LIMIT] Gặp lỗi (lần {attempt + 1}/{max_retries})...")
            else:
                print(f"   [LỖI API] {data.get('message')}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"   [LỖI MẠNG] (lần {attempt + 1}/{max_retries}): {e}")
        if attempt < max_retries - 1:
            delay = (base_delay ** attempt) + random.uniform(0, 1)
            print(f"   Thử lại sau {delay:.2f} giây.")
            time.sleep(delay)
    print("   [THẤT BẠI] Đã thử lại tối đa.")
    return None

def fetch_all_pages(session, params):
    """Lấy tất cả các trang kết quả từ một API endpoint."""
    all_results = []
    current_page = 1
    while True:
        params['page'] = current_page
        data = make_api_request_with_backoff(session, params)
        if not data: break
        page_data = data.get("data", {})
        result_list = page_data.get("list", [])
        all_results.extend(result_list)
        page_info = page_data.get("page_info", {})
        total_pages = page_info.get("total_page", 1)
        if current_page >= total_pages: break
        current_page += 1
        time.sleep(0.5)
    return all_results

def get_all_campaigns(session, start_date, end_date):
    """Lấy danh sách tất cả campaign ID và tên trong một khoảng thời gian."""
    print(f"Bước 1: Đang lấy danh sách Campaigns từ {start_date} đến {end_date}...")
    params = {
        "advertiser_id": ADVERTISER_ID, "store_ids": json.dumps([STORE_ID]),
        "start_date": start_date, "end_date": end_date,
        "dimensions": json.dumps(["campaign_id"]), "metrics": json.dumps(["campaign_name"]),
        "filtering": json.dumps({"gmv_max_promotion_types": ["PRODUCT"]}), "page_size": 1000,
    }
    all_campaign_items = fetch_all_pages(session, params)
    if all_campaign_items:
        campaigns = {item["dimensions"]["campaign_id"]: item["metrics"]["campaign_name"] for item in all_campaign_items}
        print(f"==> Tìm thấy tổng cộng {len(campaigns)} campaigns.")
        return campaigns
    return {}

def fetch_data_for_batch(session, campaign_batch, campaign_name_map, start_date, end_date):
    """Lấy dữ liệu hiệu suất cho một lô các campaign_id."""
    batch_ids = [cid for cid in campaign_batch]
    
    params_perf = {
        "advertiser_id": ADVERTISER_ID, "store_ids": json.dumps([STORE_ID]),
        "start_date": start_date, "end_date": end_date,
        "dimensions": json.dumps(["campaign_id", "item_group_id"]),
        "metrics": json.dumps(["cost"]),
        "filtering": json.dumps({"campaign_ids": batch_ids}), 
        "page_size": 1000,
    }

    perf_list = fetch_all_pages(session, params_perf)
    
    results_in_batch = {}
    for cid in batch_ids:
        results_in_batch[cid] = {
            "campaign_id": cid,
            "campaign_name": campaign_name_map.get(cid, "N/A"),
            "performance_data": [],
        }

    for record in perf_list:
        cid = record.get("dimensions", {}).get("campaign_id")
        if cid in results_in_batch:
            results_in_batch[cid]["performance_data"].append(record)
            
    return list(results_in_batch.values())

def fetch_item_details(session, campaign_id, item_group_id, start_date, end_date):
    """Lấy thông tin chi tiết item cho một cặp (campaign_id, item_group_id)."""
    params = {
        "advertiser_id": ADVERTISER_ID,
        "store_ids": json.dumps([STORE_ID]),
        "start_date": start_date,
        "end_date": end_date,
        "dimensions": json.dumps(["item_id"]),
        "metrics": json.dumps([
            "title", "tt_account_name", "tt_account_profile_image_url",
            "tt_account_authorization_type", "shop_content_type"
        ]),
        "filtering": json.dumps({
            "campaign_ids": [campaign_id],
            "item_group_ids": [item_group_id]
        }),
        "page_size": 1000,
    }
    return fetch_all_pages(session, params)

def enrich_results_with_item_details(session, results, start_date, end_date):
    """
    Làm giàu kết quả bằng cách thêm thông tin chi tiết item một cách TUẦN TỰ.
    """
    print("\nBước 3: Bắt đầu làm giàu dữ liệu chi tiết Item (quảng cáo)...")
    
    tasks = []
    for campaign_result in results:
        for perf_record in campaign_result.get("performance_data", []):
            dims = perf_record.get("dimensions", {})
            cid = dims.get("campaign_id")
            item_group_id = dims.get("item_group_id")
            if cid and item_group_id:
                tasks.append((perf_record, cid, item_group_id))

    if not tasks:
        print("==> Không có cặp (campaign, item_group) nào để làm giàu dữ liệu.")
        return results

    print(f"==> Chuẩn bị gọi API tuần tự cho {len(tasks)} cặp (campaign, item_group)...")
    
    # Vòng lặp tuần tự thay cho ThreadPoolExecutor
    for i, (perf_record, cid, item_group_id) in enumerate(tasks, 1):
        try:
            item_details_list = fetch_item_details(session, cid, item_group_id, start_date, end_date)
            perf_record["item_details"] = item_details_list
            print(f"   [ENRICH] Đã xử lý {i}/{len(tasks)} cặp...", end='\r')
        except Exception as e:
            print(f"\n   [LỖI ENRICH] Lỗi khi xử lý cặp ({cid}, {item_group_id}): {e}")
            perf_record["item_details"] = [] # Ghi nhận lỗi
    
    print(f"\n==> Hoàn thành làm giàu dữ liệu chi tiết item.")
    return results

# --- LUỒNG CHÍNH ---

if __name__ == "__main__":
    start_time = time.perf_counter()
    date_chunks = generate_monthly_date_chunks(START_DATE, END_DATE)
    print(f"Đã chia khoảng thời gian thành {len(date_chunks)} chunk.")

    all_final_results = []
    
    with requests.Session() as main_session:
        main_session.headers.update(HEADERS)
    
        for chunk in date_chunks:
            chunk_start = chunk['start']
            chunk_end = chunk['end']
            print(f"\n--- BẮT ĐẦU XỬ LÝ CHUNK: {chunk_start} to {chunk_end} ---")
            
            # Bước 1: Lấy tất cả campaign trong chunk
            campaigns_map = get_all_campaigns(main_session, chunk_start, chunk_end)

            if campaigns_map:
                campaign_ids = list(campaigns_map.keys())
                campaign_batches = list(chunk_list(campaign_ids, 20))
                
                print(f"Bước 2: Lấy dữ liệu hiệu suất cho {len(campaign_ids)} campaigns (chia thành {len(campaign_batches)} lô).")
                
                chunk_results = []
                
                # Vòng lặp tuần tự thay cho ThreadPoolExecutor
                for i, batch in enumerate(campaign_batches, 1):
                    print(f"   Đang xử lý lô {i}/{len(campaign_batches)}...")
                    batch_result = fetch_data_for_batch(main_session, batch, campaigns_map, chunk_start, chunk_end) 
                    for result in batch_result:
                        if result.get("performance_data"):
                            chunk_results.append(result)

                # Bước 3: Thêm bước làm giàu dữ liệu sau khi có kết quả của chunk
                if chunk_results:
                    enriched_chunk_results = enrich_results_with_item_details(main_session, chunk_results, chunk_start, chunk_end)
                    all_final_results.extend(enriched_chunk_results)
                else:
                    print("==> Chunk này không có dữ liệu hiệu suất để xử lý.")

    # --- TÍNH TOÁN VÀ LƯU FILE ---

    if not all_final_results:
        print("\n--- KHÔNG CÓ DỮ LIỆU ---")
        print("Không tìm thấy dữ liệu nào phù hợp với các tiêu chí.")
    else:
        total_cost = 0
        for campaign_result in all_final_results:
            for perf_record in campaign_result.get("performance_data", []):
                try:
                    cost_value = float(perf_record.get("metrics", {}).get("cost", 0))
                    total_cost += cost_value
                except (ValueError, TypeError):
                    continue
        
        print(f"\n💰 Tổng chi phí (cost) của tất cả các campaign đã xử lý: {total_cost:,.0f} VND")
        print("\n--- HOÀN THÀNH TOÀN BỘ ---")
        print(f"Đã xử lý và giữ lại kết quả từ {len(all_final_results)} lượt campaign có dữ liệu đầy đủ.")
        
        output_filename = "tiktok_results_ENRICHED_SEQUENTIAL.json"
        with open(output_filename, "w", encoding="utf-8") as f:
            json.dump(all_final_results, f, ensure_ascii=False, indent=4)
        print(f"Kết quả đã được ghi vào file '{output_filename}'")
    
    end_time = time.perf_counter()
    print(f"\nTổng thời gian thực thi: {end_time - start_time:.2f} giây.")