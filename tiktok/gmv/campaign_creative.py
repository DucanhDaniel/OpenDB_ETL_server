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
PERFORMANCE_API_URL = "https://business-api.tiktok.com/open_api/v1.3/gmv_max/report/get/"
CATALOG_API_URL = "https://business-api.tiktok.com/open_api/v1.3/store/product/get/"
BC_API_URL = "https://business-api.tiktok.com/open_api/v1.3/bc/get/"

HEADERS = {
    "Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json",
}

# --- CÁC HÀM TIỆN ÍCH ---
def chunk_list(data, size):
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
        chunks.append({'start': chunk_start.strftime('%Y-%m-%d'), 'end': chunk_end.strftime('%Y-%m-%d')})
        next_month = cursor_date.month + 1
        next_year = cursor_date.year
        if next_month > 12:
            next_month = 1
            next_year += 1
        cursor_date = date(next_year, next_month, 1)
    return chunks

# --- CÁC HÀM GỌI API VÀ XỬ LÝ DỮ LIỆU ---

def make_api_request_with_backoff(session, url, params, max_retries=5, base_delay=3):
    for attempt in range(max_retries):
        try:
            response = session.get(url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
            if data.get("code") == 0: return data
            if "Too many requests" in data.get("message", ""):
                print(f"  [RATE LIMIT] Gặp lỗi (lần {attempt + 1}/{max_retries})...")
            else:
                print(f"  [LỖI API] {data.get('message')}")
                # Đối với lỗi quyền cụ thể, trả về data để hàm bên ngoài xử lý
                if data.get("code") == 40105: return data 
                return None
        except requests.exceptions.RequestException as e:
            print(f"  [LỖI MẠNG] (lần {attempt + 1}/{max_retries}): {e}")
        if attempt < max_retries - 1:
            delay = (base_delay ** attempt) + random.uniform(0, 1)
            print(f"  Thử lại sau {delay:.2f} giây.")
            time.sleep(delay)
    print("  [THẤT BẠI] Đã thử lại tối đa.")
    return None

def fetch_all_pages(session, url, params):
    all_results = []
    current_page = 1
    while True:
        params['page'] = current_page
        data = make_api_request_with_backoff(session, url, params)
        if not data or data.get("code") != 0: break
        
        page_data = data.get("data", {})
        result_list = page_data.get("list", []) or page_data.get("store_products", []) # Hỗ trợ cả hai loại response
        all_results.extend(result_list)
        
        page_info = page_data.get("page_info", {})
        total_pages = page_info.get("total_page", 1)
        print(f"  [PHÂN TRANG] Đã lấy trang {current_page}/{total_pages}...")
        
        if current_page >= total_pages: break
        current_page += 1
        time.sleep(1.2)
    return all_results

def get_bc_ids(access_token):
    # (Code hàm get_bc_ids của bạn ở đây - không thay đổi)
    url = BC_API_URL
    headers = {'Access-Token': access_token}
    print("Đang lấy danh sách BC ID...")
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        if data.get("code") == 0:
            bc_list = data.get("data", {}).get("list", [])
            bc_ids = [bc.get("bc_info", {}).get("bc_id") for bc in bc_list if bc.get("bc_info", {}).get("bc_id")]
            print(f"Đã lấy thành công {len(bc_ids)} BC ID.")
            return bc_ids
    except requests.exceptions.RequestException as e:
        print(f"Lỗi kết nối khi lấy BC ID: {e}")
    print("Không thể lấy danh sách BC ID.")
    return []

def fetch_all_tiktok_products(bc_id, store_id, access_token):
    # (Code hàm fetch_all_tiktok_products của bạn, có chỉnh sửa nhỏ để tái sử dụng hàm gọi API)
    all_products = []
    print(f"--- Bắt đầu lấy dữ liệu sản phẩm cho BC ID: {bc_id} ---")
    with requests.Session() as session:
        session.headers.update(HEADERS)
        params = {'bc_id': bc_id, 'store_id': store_id, 'page_size': 100}
        all_products = fetch_all_pages(session, CATALOG_API_URL, params)
    print(f"--- Hoàn tất lấy sản phẩm cho BC ID: {bc_id}. Tổng cộng: {len(all_products)} sản phẩm. ---")
    return all_products

def create_product_info_map(product_list):
    """
    Tạo một dictionary để tra cứu thông tin sản phẩm từ item_group_id.
    """
    product_map = {}
    for product in product_list:
        # Sửa lại key từ "product_id" thành "item_group_id"
        product_id = product.get("item_group_id")
        if product_id:
            product_map[product_id] = {
                "product_title": product.get("title"),
                "product_status": product.get("status"),
                # Lấy trực tiếp từ key "product_image_url"
                "product_image_url": product.get("product_image_url")
            }
    return product_map

def enrich_with_product_details(enriched_results, product_info_map):
    """Làm giàu báo cáo hiệu suất với thông tin chi tiết sản phẩm."""
    print("Bắt đầu làm giàu dữ liệu với thông tin chi tiết sản phẩm...")
    for campaign in enriched_results:
        for product_perf in campaign.get("performance_data", []):
            item_group_id = product_perf.get("dimensions", {}).get("item_group_id")
            product_details = product_info_map.get(item_group_id, {})
            product_perf["product_details"] = product_details
    return enriched_results

# (Các hàm process_campaign_batch, enrich_with_creative_details, filter_empty_creatives giữ nguyên như trước)
def enrich_with_creative_details(product_perf_list, creative_api_results):
    """
    Làm giàu dữ liệu sản phẩm bằng cách thêm chi tiết creative.
    *** Đã cập nhật để khớp theo cả campaign_id và item_group_id. ***
    """
    creative_details_map = {}
    for creative_result in creative_api_results:
        dimensions = creative_result.get("dimensions", {})
        
        # THAY ĐỔI 1: Tạo khóa σύνθετη từ campaign_id và item_group_id
        campaign_id = dimensions.get("campaign_id")
        product_id = dimensions.get("item_group_id")
        
        if not campaign_id or not product_id:
            continue
            
        composite_key = f"{campaign_id}_{product_id}" # Ví dụ: "1837..._1729..."
        
        creative_info = {"item_id": dimensions.get("item_id"), "metrics": creative_result.get("metrics", {})}
        
        if composite_key not in creative_details_map:
            creative_details_map[composite_key] = []
        creative_details_map[composite_key].append(creative_info)

    for product_perf in product_perf_list:
        dimensions = product_perf.get("dimensions", {})
        
        # THAY ĐỔI 2: Sử dụng cùng một khóa σύνθετη để tra cứu
        campaign_id = dimensions.get("campaign_id")
        product_id = dimensions.get("item_group_id")
        
        if campaign_id and product_id:
            composite_key_to_find = f"{campaign_id}_{product_id}"
            enriched_data = creative_details_map.get(composite_key_to_find, [])
            product_perf["creative_details"] = enriched_data
        else:
            # Nếu thiếu thông tin, gán một danh sách rỗng
            product_perf["creative_details"] = []
            
    return product_perf_list

def filter_empty_creatives(enriched_campaign_data):
    print("Bắt đầu lọc các creative không có hiệu suất...")
    ZERO_METRICS = {"cost", "orders", "gross_revenue", "product_clicks", "product_impressions", "ad_video_view_rate_2s"}
    for campaign in enriched_campaign_data:
        for product in campaign.get("performance_data", []):
            if "creative_details" in product:
                product["creative_details"] = [
                    creative for creative in product["creative_details"]
                    if not all(float(creative.get("metrics", {}).get(m, 0)) == 0 for m in ZERO_METRICS)
                ]
    return enriched_campaign_data

def process_campaign_batch(campaign_batch, start_date, end_date):
    batch_ids = [c[0] for c in campaign_batch]
    batch_names = [c[1] for c in campaign_batch]
    print(f"  [BẮT ĐẦU BATCH] Xử lý {len(batch_ids)} campaigns: {', '.join(batch_names)}")
    # batch_results = {cid: {"campaign_id": cid, "campaign_name": cname, "performance_data": []} for cid, cname in campaign_batch}
    batch_results = {
        cid: {
            "campaign_id": cid,
            "campaign_name": cname,
            "start_date": start_date, 
            "end_date": end_date,     
            "performance_data": []
        }
        for cid, cname in campaign_batch
    }
    with requests.Session() as session:
        session.headers.update(HEADERS)
        params_product = {"advertiser_id": ADVERTISER_ID, "store_ids": json.dumps([STORE_ID]),"start_date": start_date, "end_date": end_date,"dimensions": json.dumps(["campaign_id", "item_group_id"]),"metrics": json.dumps(["cost", "orders", "gross_revenue"]),"filtering": json.dumps({"campaign_ids": batch_ids}),"page_size": 1000,}
        product_perf_list = fetch_all_pages(session, PERFORMANCE_API_URL, params_product)
        if not product_perf_list:
            print(f"  [KẾT THÚC BATCH] Lô campaigns không có dữ liệu sản phẩm.")
            return list(batch_results.values())
        product_ids = list(set([p["dimensions"]["item_group_id"] for p in product_perf_list]))
        product_id_chunks = list(chunk_list(product_ids, 20))
        all_creative_results = []
        print(f"  Tìm thấy {len(product_ids)} sản phẩm duy nhất, chia thành {len(product_id_chunks)} lô để lấy creative.")
        for p_chunk in product_id_chunks:
            params_creative = {"advertiser_id": ADVERTISER_ID, "store_ids": json.dumps([STORE_ID]),"start_date": start_date, "end_date": end_date,"dimensions": json.dumps(["campaign_id", "item_group_id", "item_id"]),"metrics": json.dumps(["cost","orders","cost_per_order","gross_revenue","roi","product_impressions","product_clicks","product_click_rate","ad_conversion_rate","creative_delivery_status","ad_video_view_rate_2s","ad_video_view_rate_6s","ad_video_view_rate_p25","ad_video_view_rate_p50","ad_video_view_rate_p75","ad_video_view_rate_p100"]),"filtering": json.dumps({"campaign_ids": batch_ids, "item_group_ids": p_chunk}),"page_size": 1000,}
            creative_results = fetch_all_pages(session, PERFORMANCE_API_URL, params_creative)
            all_creative_results.extend(creative_results)
            time.sleep(1.2)
        enriched_product_list = enrich_with_creative_details(product_perf_list, all_creative_results)
        for product_record in enriched_product_list:
            cid = product_record.get("dimensions", {}).get("campaign_id")
            if cid in batch_results:
                batch_results[cid]["performance_data"].append(product_record)
    print(f"  [HOÀN THÀNH BATCH] Đã xử lý xong lô: {', '.join(batch_names)}")
    return list(batch_results.values())
    
# --- HÀM CHÍNH ĐỂ CHẠY ---

if __name__ == "__main__":

    if not ACCESS_TOKEN:
        print("Lỗi: Vui lòng thiết lập biến môi trường TIKTOK_ACCESS_TOKEN.")
    else:
        start_time = time.perf_counter()
        
        # === GIAI ĐOẠN 1: LẤY DỮ LIỆU HIỆU SUẤT ===
        print("--- GIAI ĐOẠN 1: BẮT ĐẦU LẤY DỮ LIỆU HIỆU SUẤT ---")
        date_chunks = generate_monthly_date_chunks(START_DATE, END_DATE)
        all_performance_results = []
        for chunk in date_chunks:
            chunk_start, chunk_end = chunk['start'], chunk['end']
            print(f"\n--- XỬ LÝ CHUNK: {chunk_start} to {chunk_end} ---")
            campaigns_map = {}
            with requests.Session() as session:
                session.headers.update(HEADERS)
                params = {"advertiser_id": ADVERTISER_ID, "store_ids": json.dumps([STORE_ID]),"start_date": chunk_start, "end_date": chunk_end,"dimensions": json.dumps(["campaign_id"]), "metrics": json.dumps(["campaign_name"]),"filtering": json.dumps({"gmv_max_promotion_types": ["PRODUCT"]}), "page_size": 1000,}
                all_campaign_items = fetch_all_pages(session, PERFORMANCE_API_URL, params)
                if all_campaign_items:
                    campaigns_map = {item["dimensions"]["campaign_id"]: item["metrics"]["campaign_name"] for item in all_campaign_items}
            print(f"==> Tìm thấy {len(campaigns_map)} campaigns trong chunk này.")
            if campaigns_map:
                campaign_list = list(campaigns_map.items())
                campaign_batches = list(chunk_list(campaign_list, 10))
                with ThreadPoolExecutor(max_workers=1) as executor:
                    future_to_batch = {executor.submit(process_campaign_batch, batch, chunk_start, chunk_end): batch for batch in campaign_batches}
                    for future in as_completed(future_to_batch):
                        try:
                            batch_result = future.result()
                            all_performance_results.extend([res for res in batch_result if res.get("performance_data")])
                        except Exception as exc:
                            print(f"  [LỖI LUỒNG] Lô {future_to_batch[future]} tạo ra lỗi: {exc}")
        print("\n--- HOÀN TẤT GIAI ĐOẠN 1: ĐÃ LẤY XONG DỮ LIỆU HIỆU SUẤT ---")
        
        # === GIAI ĐOẠN 2: LẤY DANH MỤC SẢN PHẨM ===
        print("\n--- GIAI ĐOẠN 2: BẮT ĐẦU LẤY DANH MỤC SẢN PHẨM ---")
        bc_ids_list = get_bc_ids(ACCESS_TOKEN)
        product_catalog = []
        if bc_ids_list:
            for bc_id in bc_ids_list:
                products = fetch_all_tiktok_products(bc_id, STORE_ID, ACCESS_TOKEN)
                if products:
                    print(f"\n=> Tìm thấy BC ID hợp lệ: {bc_id}. Đã lấy {len(products)} sản phẩm.")
                    product_catalog = products
                    break
        if not product_catalog:
            print("CẢNH BÁO: Không thể lấy danh mục sản phẩm. Dữ liệu cuối cùng sẽ không có chi tiết sản phẩm.")
        
        # === GIAI ĐOẠN 3: LÀM GIÀU DỮ LIỆU VÀ HOÀN TẤT ===
        print("\n--- GIAI ĐOẠN 3: BẮT ĐẦU LÀM GIÀU DỮ LIỆU ---")
        # Tạo map tra cứu thông tin sản phẩm
        product_info_map = create_product_info_map(product_catalog)
        # Làm giàu với thông tin chi tiết sản phẩm
        final_data = enrich_with_product_details(all_performance_results, product_info_map)
        # Lọc bỏ các creative không có hiệu suất
        final_filtered_data = filter_empty_creatives(final_data)

        # Tính tổng cost
        total_creative_cost = sum(
            float(creative.get("metrics", {}).get("cost", 0))
            for campaign in final_filtered_data
            for product in campaign.get("performance_data", [])
            for creative in product.get("creative_details", [])
            if float(creative.get("metrics", {}).get("cost", 0)) > 0
        )
        
        # Ghi file và kết thúc
        output_filename = "GMV_Campaign_creative_detail.json"
        with open(output_filename, "w", encoding="utf-8") as f:
            json.dump(final_filtered_data, f, ensure_ascii=False, indent=4)
        
        print("\n--- HOÀN THÀNH TOÀN BỘ ---")
        print(f"Đã xử lý và lưu kết quả của {len(final_filtered_data)} campaigns vào file '{output_filename}'")
        print(f"💰 Tổng chi phí (cost) của các creatives có hiệu suất: {total_creative_cost:,.0f} VND")
        
        end_time = time.perf_counter()
        print(f"\nTổng thời gian thực thi: {end_time - start_time:.2f} giây.")
        
