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
START_DATE = "2025-09-01"
END_DATE = "2025-09-18"
API_URL = "https://business-api.tiktok.com/open_api/v1.3/gmv_max/report/get/"

HEADERS = {
    "Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json",
}

# --- CÁC HÀM TIỆN ÍCH ---

def chunk_list(data, size):
    """Chia một danh sách thành các danh sách con có kích thước `size`."""
    for i in range(0, len(data), size):
        yield data[i:i + size]

def generate_monthly_date_chunks(start_date_str, end_date_str):
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

# --- CÁC HÀM GỌI API VÀ XỬ LÝ DỮ LIỆU ---

def make_api_request_with_backoff(session, params, max_retries=5, base_delay=3):
    """Thực hiện một yêu cầu API với cơ chế thử lại khi gặp lỗi."""
    for attempt in range(max_retries):
        try:
            response = session.get(API_URL, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
            if data.get("code") == 0: return data
            if "Too many requests" in data.get("message", ""):
                print(f"  [RATE LIMIT] Gặp lỗi (lần {attempt + 1}/{max_retries})...")
            else:
                print(f"  [LỖI API] {data.get('message')}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"  [LỖI MẠNG] (lần {attempt + 1}/{max_retries}): {e}")
        if attempt < max_retries - 1:
            delay = (base_delay ** attempt) + random.uniform(0, 1)
            print(f"  Thử lại sau {delay:.2f} giây.")
            time.sleep(delay)
    print("  [THẤT BẠI] Đã thử lại tối đa.")
    return None

def fetch_all_pages(session, params):
    """Lấy dữ liệu từ tất cả các trang của một yêu cầu API."""
    all_results = []
    current_page = 1
    while True:
        params['page'] = current_page
        data = make_api_request_with_backoff(session, params)
        if not data or data.get("code") != 0: break
        
        page_data = data.get("data", {})
        result_list = page_data.get("list", [])
        all_results.extend(result_list)
        
        page_info = page_data.get("page_info", {})
        total_pages = page_info.get("total_page", 1)
        print(f"  [PHÂN TRANG] Đã lấy trang {current_page}/{total_pages}...")
        
        if current_page >= total_pages: break
        current_page += 1
        time.sleep(1.2)
    return all_results

def enrich_with_creative_details(product_perf_list, creative_api_results):
    """Làm giàu dữ liệu sản phẩm bằng cách thêm chi tiết creative."""
    creative_details_map = {}
    for creative_result in creative_api_results:
        dimensions = creative_result.get("dimensions", {})
        product_id = dimensions.get("item_group_id")
        if not product_id: continue
        
        creative_info = {"item_id": dimensions.get("item_id"), "metrics": creative_result.get("metrics", {})}
        
        if product_id not in creative_details_map:
            creative_details_map[product_id] = []
        creative_details_map[product_id].append(creative_info)

    for product_perf in product_perf_list:
        current_product_id = product_perf.get("dimensions", {}).get("item_group_id")
        enriched_data = creative_details_map.get(current_product_id, [])
        product_perf["creative_details"] = enriched_data
        
    return product_perf_list

def filter_empty_creatives(enriched_campaign_data):
    """Lọc bỏ các creative không có bất kỳ chỉ số hiệu suất nào."""
    print("Bắt đầu lọc các creative không có hiệu suất...")
    ZERO_METRICS = {
        "cost", "orders", "gross_revenue", "product_clicks", 
        "product_impressions", "ad_video_view_rate_2s"
    }
    
    for campaign in enriched_campaign_data:
        for product in campaign.get("performance_data", []):
            if "creative_details" in product:
                filtered_creatives = []
                for creative in product["creative_details"]:
                    metrics = creative.get("metrics", {})
                    is_all_zero = True
                    for key, value in metrics.items():
                        if key in ZERO_METRICS and float(value) != 0:
                            is_all_zero = False
                            break
                    if not is_all_zero:
                        filtered_creatives.append(creative)
                product["creative_details"] = filtered_creatives
    return enriched_campaign_data


def process_campaign_batch(campaign_batch, start_date, end_date):
    """Xử lý một lô campaigns (ví dụ: 2 campaign một lúc)."""
    batch_ids = [c[0] for c in campaign_batch]
    batch_names = [c[1] for c in campaign_batch]
    print(f"  [BẮT ĐẦU BATCH] Xử lý {len(batch_ids)} campaigns: {', '.join(batch_names)}")
    
    batch_results = {
        cid: {"campaign_id": cid, "campaign_name": cname, "performance_data": []}
        for cid, cname in campaign_batch
    }

    with requests.Session() as session:
        session.headers.update(HEADERS)

        # 1. Lấy tất cả sản phẩm cho cả lô campaign này
        params_product = {
            "advertiser_id": ADVERTISER_ID, "store_ids": json.dumps([STORE_ID]),
            "start_date": start_date, "end_date": end_date,
            "dimensions": json.dumps(["campaign_id", "item_group_id"]),
            "metrics": json.dumps(["cost", "orders", "gross_revenue"]),
            "filtering": json.dumps({"campaign_ids": batch_ids}),
            "page_size": 1000,
        }
        product_perf_list = fetch_all_pages(session, params_product)

        if not product_perf_list:
            print(f"  [KẾT THÚC BATCH] Lô campaigns không có dữ liệu sản phẩm.")
            return list(batch_results.values())

        # 2. Lấy chi tiết creative cho tất cả sản phẩm trong lô
        product_ids = list(set([p["dimensions"]["item_group_id"] for p in product_perf_list]))
        product_id_chunks = list(chunk_list(product_ids, 20)) # Chia lô 20 sản phẩm/lần
        
        all_creative_results = []
        print(f"  Tìm thấy {len(product_ids)} sản phẩm duy nhất, chia thành {len(product_id_chunks)} lô để lấy creative.")
        for p_chunk in product_id_chunks:
            params_creative = {
                "advertiser_id": ADVERTISER_ID, "store_ids": json.dumps([STORE_ID]),
                "start_date": start_date, "end_date": end_date,
                "dimensions": json.dumps(["campaign_id", "item_group_id", "item_id"]),
                "metrics": json.dumps(["cost","orders","cost_per_order","gross_revenue","roi","product_impressions","product_clicks","product_click_rate","ad_conversion_rate","creative_delivery_status","ad_video_view_rate_2s","ad_video_view_rate_6s","ad_video_view_rate_p25","ad_video_view_rate_p50","ad_video_view_rate_p75","ad_video_view_rate_p100"]),
                "filtering": json.dumps({"campaign_ids": batch_ids, "item_group_ids": p_chunk}),
                "page_size": 1000,
            }
            creative_results = fetch_all_pages(session, params_creative)
            all_creative_results.extend(creative_results)
            time.sleep(1.2)

        # 3. Làm giàu và phân loại lại kết quả vào đúng campaign
        enriched_product_list = enrich_with_creative_details(product_perf_list, all_creative_results)
        
        for product_record in enriched_product_list:
            cid = product_record.get("dimensions", {}).get("campaign_id")
            if cid in batch_results:
                batch_results[cid]["performance_data"].append(product_record)

    print(f"  [HOÀN THÀNH BATCH] Đã xử lý xong lô: {', '.join(batch_names)}")
    return list(batch_results.values())

# --- HÀM CHÍNH ĐỂ CHẠY ---

if __name__ == "__main__":
    start_time = time.perf_counter()
    date_chunks = generate_monthly_date_chunks(START_DATE, END_DATE)
    print(f"Đã chia khoảng thời gian thành {len(date_chunks)} chunk.")

    all_enriched_results = []
    
    for chunk in date_chunks:
        chunk_start, chunk_end = chunk['start'], chunk['end']
        print(f"\n--- BẮT ĐẦU XỬ LÝ CHUNK: {chunk_start} to {chunk_end} ---")
        
        campaigns_map = {}
        with requests.Session() as session:
            session.headers.update(HEADERS)
            params = {
                "advertiser_id": ADVERTISER_ID, "store_ids": json.dumps([STORE_ID]),
                "start_date": chunk_start, "end_date": chunk_end,
                "dimensions": json.dumps(["campaign_id"]), "metrics": json.dumps(["campaign_name"]),
                "filtering": json.dumps({"gmv_max_promotion_types": ["PRODUCT"]}), "page_size": 1000,
            }
            all_campaign_items = fetch_all_pages(session, params)
            if all_campaign_items:
                campaigns_map = {item["dimensions"]["campaign_id"]: item["metrics"]["campaign_name"] for item in all_campaign_items}
        
        print(f"==> Tìm thấy {len(campaigns_map)} campaigns trong chunk này.")

        if campaigns_map:
            campaign_list = list(campaigns_map.items())
            campaign_batches = list(chunk_list(campaign_list, 5)) # Chia thành các lô 5 campaign
            
            max_workers = 1
            print(f"Bắt đầu xử lý {len(campaign_batches)} lô song song với {max_workers} luồng...")
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_batch = {
                    executor.submit(process_campaign_batch, batch, chunk_start, chunk_end): batch
                    for batch in campaign_batches
                }
                
                for future in as_completed(future_to_batch):
                    try:
                        batch_result = future.result()
                        for campaign_result in batch_result:
                             if campaign_result and campaign_result.get("performance_data"):
                                all_enriched_results.append(campaign_result)
                    except Exception as exc:
                        print(f"  [LỖI LUỒNG] Lô {future_to_batch[future]} tạo ra lỗi: {exc}")

    # Lọc các creative không có hiệu suất
    final_filtered_results = filter_empty_creatives(all_enriched_results)

    # Tính tổng cost của các creative CÒN LẠI sau khi lọc
    total_creative_cost = 0
    for campaign in final_filtered_results:
        for product in campaign.get("performance_data", []):
            for creative in product.get("creative_details", []):
                try:
                    cost_value = float(creative.get("metrics", {}).get("cost", 0))
                    total_creative_cost += cost_value
                except (ValueError, TypeError):
                    continue
    
    # Ghi kết quả cuối cùng đã được lọc ra file
    output_filename = "tiktok_final_results.json"
    with open(output_filename, "w", encoding="utf-8") as f:
        json.dump(final_filtered_results, f, ensure_ascii=False, indent=4)
    
    print("\n--- HOÀN THÀNH TOÀN BỘ ---")
    print(f"Đã xử lý và lưu kết quả của {len(final_filtered_results)} campaigns vào file '{output_filename}'")
    print(f"💰 Tổng chi phí (cost) của các creatives CÓ HIỆU SUẤT: {total_creative_cost:,.0f} VND")
    
    end_time = time.perf_counter()
    print(f"\nTổng thời gian thực thi: {end_time - start_time:.2f} giây.")