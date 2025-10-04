import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any
from .gmv_reporter import GMVReporter
from dotenv import load_dotenv

# Tải các biến môi trường một lần khi module được import
load_dotenv()

class GMVCampaignProductDetailReporter(GMVReporter):
    """
    Lấy và kết hợp dữ liệu hiệu suất chiến dịch với thông tin chi tiết sản phẩm
    từ TikTok Marketing API.
    
    Đã được nâng cấp với cơ chế backoff và throttling để tăng độ ổn định.
    """

    def __init__(self, access_token: str, advertiser_id: str, store_id: str, progress_callback=None, job_id: str = None, redis_client=None):
        """
        Khởi tạo reporter.

        Args:
            access_token (str): Access token để xác thực với API.
            advertiser_id (str): ID của tài khoản quảng cáo.
            store_id (str): ID của cửa hàng TikTok Shop.
        """
        
        super().__init__(access_token, advertiser_id, store_id, progress_callback, job_id, redis_client)
        

    def _get_product_map(self) -> dict | None:
        """Lấy toàn bộ sản phẩm và chuyển thành một dictionary để tra cứu nhanh."""
        print("\n--- BƯỚC 1: LẤY VÀ CHUẨN BỊ DỮ LIỆU SẢN PHẨM ---")
        bc_ids = self._get_bc_ids()
        if not bc_ids:
            return None

        all_products = []
        for bc_id in bc_ids:
            products_list = self._fetch_all_tiktok_products(bc_id)
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
                "performance_data": [],
                "start_date": start_date, 
                "end_date": end_date
            }
        
        for record in perf_list:
            cid = record["dimensions"]["campaign_id"]
            if cid in results:
                results[cid]["performance_data"].append(record)
        return list(results.values())

    def _enrich_campaign_data(self, campaign_results, product_map):
        """
        Làm phẳng và gộp dữ liệu. Mỗi bản ghi hiệu suất sẽ là một mục riêng biệt
        chứa đầy đủ thông tin campaign và sản phẩm.
        """
        print("\n--- BƯỚC 3: LÀM PHẲNG VÀ GỘP DỮ LIỆU ---")
        if not product_map:
            print("   -> Cảnh báo: Không có bản đồ sản phẩm. Dữ liệu sẽ không được làm giàu.")
            # Vẫn trả về dữ liệu thô nếu không có product_map
            return campaign_results
            
        flattened_records = []

        # Lặp qua từng kết quả campaign từ mỗi chunk thời gian
        for campaign_chunk in campaign_results:
            # Lấy thông tin chung của campaign từ chunk này
            # Quan trọng: start_date và end_date ở đây là của chunk hiện tại
            campaign_info = {
                "campaign_id": campaign_chunk.get("campaign_id"),
                "campaign_name": campaign_chunk.get("campaign_name"),
                "operation_status": campaign_chunk.get("operation_status"),
                "bid_type": campaign_chunk.get("bid_type"),
                "start_date": campaign_chunk.get("start_date"),
                "end_date": campaign_chunk.get("end_date")
            }

            # Nếu không có dữ liệu hiệu suất, bỏ qua
            if not campaign_chunk.get("performance_data"):
                continue

            # Lặp qua từng bản ghi hiệu suất trong chunk
            for perf_record in campaign_chunk["performance_data"]:
                item_group_id = perf_record.get("dimensions", {}).get("item_group_id")
                
                # Lấy thông tin sản phẩm tương ứng
                product_info = {}
                if item_group_id:
                    product_info = product_map.get(item_group_id, {"title": f"Không tìm thấy thông tin cho ID {item_group_id}"})

                # Tạo một bản ghi phẳng cuối cùng
                final_record = {
                    **campaign_info,  # Thông tin campaign (có start/end date đúng)
                    "stat_time_day": perf_record.get("dimensions", {}).get("stat_time_day"),
                    "item_group_id": item_group_id,
                    "metrics": perf_record.get("metrics", {}),
                    "product_info": product_info
                }
                flattened_records.append(final_record)
                
        print(f"   -> Đã làm phẳng và gộp thành công {len(flattened_records)} bản ghi chi tiết.")
        return flattened_records
    
    def get_data(self, date_chunks) -> list:
        """
        Hàm chính để chạy toàn bộ quy trình: lấy sản phẩm, lấy hiệu suất
        chiến dịch, và gộp chúng lại.
        """
        # BƯỚC 1: Lấy dữ liệu sản phẩm
        self._report_progress("Đang lấy dữ liệu sản phẩm", 5)
        product_map = self._get_product_map()
        if not product_map:
            print("Không thể lấy dữ liệu sản phẩm. Dừng thực thi.")
            return []

        # BƯỚC 2: Lấy dữ liệu campaign
        print("\n--- BƯỚC 2: LẤY DỮ LIỆU CAMPAIGN ---")
        self._report_progress("Bắt đầu lấy dữ liệu campaign", 15)
        # date_chunks = self._generate_monthly_date_chunks(start_date, end_date)
        all_campaign_results = []

        for chunk in date_chunks:
            print(f"\n>> Xử lý chunk: {chunk['start']} to {chunk['end']}")
            self._report_progress(f"Xử lý chunk: {chunk['start']} to {chunk['end']}", 60)
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
                    all_campaign_results.extend(future.result())

        # BƯỚC 3: Gộp dữ liệu
        self._report_progress("Bắt đầu gộp dữ liệu...", 80)
        final_data = self._enrich_campaign_data(all_campaign_results, product_map)
        return final_data

def _flatten_product_report(
    campaign_data_list: List[Dict[str, Any]],
    context: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Flattens the raw product report data into a list of rows, ready for a spreadsheet.
    """
    flattened_data = []
    for campaign in campaign_data_list:
        row = {
                # General info from context
                "start_date": campaign.get("start_date"),
                "end_date": campaign.get("end_date"),
                "advertiser_id": context.get("advertiser_id"),
                "advertiser_name": context.get("advertiser_name"),
                "store_id": context.get("store_id"),
                "store_name": context.get("store_name"),

                # Campaign info
                "campaign_id": campaign.get("campaign_id"),
                "campaign_name": campaign.get("campaign_name"),
                "operation_status": campaign.get("operation_status"),
                "bid_type": campaign.get("bid_type"),

                # Product info and dimensions
                "item_group_id": campaign.get("item_group_id"),
                "stat_time_day": campaign.get("stat_time_day"),
                "product_name": campaign.get("product_info", {}).get("title"),
                "product_image_url": campaign.get("product_info", {}).get("product_image_url"),
                "product_status": campaign.get("product_info", {}).get("status"),
                "product_img": campaign.get("product_info", {}).get("product_image_url"),
            }
            # Add all metrics dynamically
        row.update(campaign.get("metrics", {}))
        flattened_data.append(row)
            
    return flattened_data

import os
if __name__ == "__main__":
    ACCESS_TOKEN = os.getenv("TIKTOK_ACCESS_TOKEN")
    ADVERTISER_ID = "7137968211592495105"
    STORE_ID = "7494588040522401840"
    START_DATE = "2025-09-01"
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
                    float(campaign.get("metrics", {}).get("cost", 0))
                    for campaign in enriched_results
                    # for perf in campaign.get("performance_data", [])
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