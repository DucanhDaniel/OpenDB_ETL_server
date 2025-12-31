import json
import time
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from .gmv_reporter import GMVReporter

load_dotenv()

class GMVCampaignCreativeDetailReporter(GMVReporter):
    """
    Một class để lấy, xử lý và làm giàu dữ liệu báo cáo hiệu suất GMV Max 
    từ TikTok Marketing API.
    
    Bao gồm việc lấy dữ liệu hiệu suất theo campaign, sản phẩm, creative,
    kết hợp với thông tin chi tiết từ danh mục sản phẩm.
    """

    def __init__(self, access_token: str, advertiser_id: str, store_id: str, progress_callback=None,
                 job_id: str = None, redis_client=None):
        """
        Khởi tạo reporter.

        Args:
            access_token (str): Access token để xác thực với API.
            advertiser_id (str): ID của tài khoản quảng cáo.
            store_id (str): ID của cửa hàng TikTok Shop.
        """
        
        super().__init__(access_token, advertiser_id, store_id, progress_callback, job_id, redis_client)

 
    def _fetch_creative_metadata(self, campaign_id: str, item_group_id: str, start_date: str, end_date: str) -> list:
        """Lấy thông tin metadata của creative cho một cặp (campaign_id, item_group_id)."""
        params = {
            "advertiser_id": self.advertiser_id,
            "store_ids": json.dumps([self.store_id]),
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
        
        return self._fetch_all_pages(self.PERFORMANCE_API_URL, params, throttling_delay=0.3)

    def _get_product_catalog(self) -> list:
        """Lấy danh mục sản phẩm từ BC ID hợp lệ đầu tiên tìm thấy."""
        bc_ids_list = self._get_bc_ids()
        if not bc_ids_list:
            return []
        products = []
        for bc_id in bc_ids_list:
            bc_products = self._fetch_all_tiktok_products(bc_id)
            if products:
                print(f"\n=> Tìm thấy BC ID hợp lệ: {bc_id}. Đã lấy {len(products)} sản phẩm.")
                self._report_progress(f"Đã lấy {len(products)} sản phẩm.", 80)
                # return products
            products.extend(bc_products)
        return products

    def _process_campaign_batch(self, campaign_batch: list[tuple], start_date: str, end_date: str) -> list:
        """Xử lý một lô campaign để lấy dữ liệu hiệu suất sản phẩm và creative."""
        batch_ids = [c[0] for c in campaign_batch]
        batch_names = [c[1] for c in campaign_batch]
        print(f"  [BẮT ĐẦU BATCH] Xử lý {len(batch_ids)} campaigns: {', '.join(batch_names)}")
        
        batch_results = {
            cid: {"campaign_id": cid, "campaign_name": cname, "start_date": start_date, "end_date": end_date, "performance_data": []}
            for cid, cname in campaign_batch
        }

        params_product = {
            "advertiser_id": self.advertiser_id, "store_ids": json.dumps([self.store_id]),
            "start_date": start_date, "end_date": end_date,
            "dimensions": json.dumps(["campaign_id", "item_group_id"]),
            "metrics": json.dumps(["cost", "orders", "gross_revenue"]),
            "filtering": json.dumps({"campaign_ids": batch_ids}),
            "page_size": 1000,
        }
        product_perf_list = self._fetch_all_pages(self.PERFORMANCE_API_URL, params_product)

        if not product_perf_list:
            print(f"  [KẾT THÚC BATCH] Lô campaigns không có dữ liệu sản phẩm.")
            return list(batch_results.values())

        product_ids = list(set([p["dimensions"]["item_group_id"] for p in product_perf_list]))
        product_id_chunks = list(self._chunk_list(product_ids, 20))
        all_creative_results = []
        
        print(f"  Tìm thấy {len(product_ids)} sản phẩm duy nhất, chia thành {len(product_id_chunks)} lô để lấy creative.")
        for p_chunk in product_id_chunks:
            params_creative = {
                "advertiser_id": self.advertiser_id, "store_ids": json.dumps([self.store_id]),
                "start_date": start_date, "end_date": end_date,
                "dimensions": json.dumps(["campaign_id", "item_group_id", "item_id"]),
                "metrics": json.dumps(["cost","orders","cost_per_order","gross_revenue","roi","product_impressions","product_clicks","product_click_rate","ad_conversion_rate","creative_delivery_status","ad_video_view_rate_2s","ad_video_view_rate_6s","ad_video_view_rate_p25","ad_video_view_rate_p50","ad_video_view_rate_p75","ad_video_view_rate_p100"]),
                "filtering": json.dumps({"campaign_ids": batch_ids, "item_group_ids": p_chunk}),
                "page_size": 1000,
            }
            creative_results = self._fetch_all_pages(self.PERFORMANCE_API_URL, params_creative)
            all_creative_results.extend(creative_results)
            time.sleep(1.2)
        
        enriched_product_list = self._enrich_with_creative_details(product_perf_list, all_creative_results)
        
        for product_record in enriched_product_list:
            cid = product_record.get("dimensions", {}).get("campaign_id")
            if cid in batch_results:
                batch_results[cid]["performance_data"].append(product_record)

        print(f"  [HOÀN THÀNH BATCH] Đã xử lý xong lô: {', '.join(batch_names)}")
        return list(batch_results.values())

    # --- CÁC HÀM LÀM GIÀU DỮ LIỆU (STATIC) ---
    @staticmethod
    def _create_product_info_map(product_list: list) -> dict:
        """Tạo một dictionary để tra cứu thông tin sản phẩm từ item_group_id."""
        product_map = {}
        for product in product_list:
            product_id = product.get("item_group_id")
            if product_id:
                product_map[product_id] = {
                    "product_title": product.get("title"),
                    "product_status": product.get("status"),
                    "product_image_url": product.get("product_image_url")
                }
        return product_map

    @staticmethod
    def _enrich_with_product_details(performance_results: list, product_info_map: dict) -> list:
        """Làm giàu báo cáo hiệu suất với thông tin chi tiết sản phẩm."""
        print("Bắt đầu làm giàu dữ liệu với thông tin chi tiết sản phẩm...")
        for campaign in performance_results:
            for product_perf in campaign.get("performance_data", []):
                item_group_id = product_perf.get("dimensions", {}).get("item_group_id")
                product_details = product_info_map.get(item_group_id, {})
                product_perf["product_details"] = product_details
        return performance_results
    
    @staticmethod
    def _enrich_with_creative_details(product_perf_list: list, creative_api_results: list) -> list:
        """Làm giàu dữ liệu sản phẩm bằng cách thêm chi tiết creative."""
        creative_details_map = {}
        for creative_result in creative_api_results:
            dimensions = creative_result.get("dimensions", {})
            campaign_id = dimensions.get("campaign_id")
            product_id = dimensions.get("item_group_id")
            
            if not campaign_id or not product_id:
                continue
            
            composite_key = f"{campaign_id}_{product_id}"
            creative_info = {"item_id": dimensions.get("item_id"), "metrics": creative_result.get("metrics", {})}
            
            if composite_key not in creative_details_map:
                creative_details_map[composite_key] = []
            creative_details_map[composite_key].append(creative_info)

        for product_perf in product_perf_list:
            dimensions = product_perf.get("dimensions", {})
            campaign_id = dimensions.get("campaign_id")
            product_id = dimensions.get("item_group_id")
            
            if campaign_id and product_id:
                composite_key_to_find = f"{campaign_id}_{product_id}"
                product_perf["creative_details"] = creative_details_map.get(composite_key_to_find, [])
            else:
                product_perf["creative_details"] = []
                
        return product_perf_list

    def _enrich_with_creative_metadata(self, performance_results: list) -> list:
        """
        Làm giàu dữ liệu hiệu suất bằng cách thêm metadata cho từng creative.
        Xử lý gọi API một cách tuần tự.
        """
        print("Bắt đầu làm giàu dữ liệu với metadata của creative (tuần tự)...")
        self._report_progress("Làm giàu dữ liệu với metadata của creative")
        # Tạo danh sách các cặp (campaign, product) cần lấy metadata
        tasks = []
        for campaign in performance_results:
            start_date = campaign.get("start_date")
            end_date = campaign.get("end_date")
            for product_perf in campaign.get("performance_data", []):
                item_group_id = product_perf.get("dimensions", {}).get("item_group_id")
                campaign_id = product_perf.get("dimensions", {}).get("campaign_id")
                # Chỉ thêm vào danh sách nếu có creative cần làm giàu
                if campaign_id and item_group_id and product_perf.get("creative_details"):
                    tasks.append((product_perf, campaign_id, item_group_id, start_date, end_date))

        # Xử lý tuần tự
        self.is_fetching_creative = True
        for i, (product_perf, cid, igid, s_date, e_date) in enumerate(tasks, 1):
            print(f"   Đang lấy metadata cho cặp ({cid}, {igid}) - {i}/{len(tasks)}...", end='\r')
            if (i % 10 == 0):
                self._report_progress(f"Lấy metadata: {i}/{len(tasks)}", 80)
            metadata_list = self._fetch_creative_metadata(cid, igid, s_date, e_date)
            
            # Tạo map để tra cứu nhanh metadata theo item_id
            metadata_map = {
                item.get("dimensions", {}).get("item_id"): item.get("metrics", {})
                for item in metadata_list
            }
            
            # Gắn metadata vào từng creative
            for creative in product_perf.get("creative_details", []):
                item_id = creative.get("item_id")
                if item_id in metadata_map:
                    creative["metadata"] = metadata_map[item_id]
        self.is_fetching_creative = False
        print(f"\nHoàn thành làm giàu metadata cho {len(tasks)} cặp sản phẩm.")
        return performance_results

    @staticmethod
    def _filter_empty_creatives(enriched_campaign_data: list) -> list:
        """Lọc bỏ các creative không có bất kỳ chỉ số hiệu suất nào."""
        print("Bắt đầu lọc các creative không có hiệu suất...")
        # ZERO_METRICS = {"cost", "orders", "gross_revenue", "product_clicks", "product_impressions", "ad_video_view_rate_2s"}
        ZERO_METRICS = {"cost", "orders"}
        for campaign in enriched_campaign_data:
            for product in campaign.get("performance_data", []):
                if "creative_details" in product:
                    product["creative_details"] = [
                        creative for creative in product["creative_details"]
                        if not all(float(creative.get("metrics", {}).get(m, 0)) == 0 for m in ZERO_METRICS)
                    ]
        return enriched_campaign_data
    
    # --- HÀM CÔNG KHAI (PUBLIC) ---
    def get_data(self, date_chunks) -> list:
        """
        Hàm chính để chạy toàn bộ quy trình lấy và xử lý dữ liệu.

        Args:
            start_date (str): Ngày bắt đầu lấy dữ liệu (YYYY-MM-DD).
            end_date (str): Ngày kết thúc lấy dữ liệu (YYYY-MM-DD).

        Returns:
            list: Một danh sách các dictionary, mỗi dictionary chứa dữ liệu
                  đã được làm giàu của một campaign.
        """
        # === GIAI ĐOẠN 1: LẤY DỮ LIỆU HIỆU SUẤT ===
        print("--- GIAI ĐOẠN 1: BẮT ĐẦU LẤY DỮ LIỆU HIỆU SUẤT ---")
        self._report_progress("Bắt đầu lấy dữ liệu hiệu suất GMV...", 5)

        # date_chunks = self._generate_monthly_date_chunks(start_date, end_date)
        all_performance_results = []
        
        for chunk in date_chunks:
            chunk_start, chunk_end = chunk['start'], chunk['end']
            print(f"\n--- XỬ LÝ CHUNK: {chunk_start} to {chunk_end} ---")
            self._report_progress(f"Xử lý chunk: {chunk_start} to {chunk_end}")
            params = {
                "advertiser_id": self.advertiser_id, 
                "store_ids": json.dumps([self.store_id]),
                "start_date": chunk_start, 
                "end_date": chunk_end,
                "dimensions": json.dumps(["campaign_id"]), 
                "metrics": json.dumps(["campaign_name"]),
                "filtering": json.dumps({"gmv_max_promotion_types": ["PRODUCT"]}), 
                "page_size": 1000,
            }
            all_campaign_items = self._fetch_all_pages(self.PERFORMANCE_API_URL, params)
            
            if not all_campaign_items:
                print(f"==> Không tìm thấy campaign nào trong chunk này.")
                continue

            campaigns_map = {item["dimensions"]["campaign_id"]: item["metrics"]["campaign_name"] for item in all_campaign_items}
            print(f"==> Tìm thấy {len(campaigns_map)} campaigns trong chunk này.")
            
            campaign_list = list(campaigns_map.items())
            campaign_batches = list(self._chunk_list(campaign_list, 10))
            
            with ThreadPoolExecutor(max_workers=1) as executor: # Giữ max_workers=1 để tránh rate limit
                future_to_batch = {executor.submit(self._process_campaign_batch, batch, chunk_start, chunk_end): batch for batch in campaign_batches}
                for future in as_completed(future_to_batch):
                    batch_result = future.result()
                    # Chỉ thêm các campaign có dữ liệu
                    all_performance_results.extend([res for res in batch_result if res.get("performance_data")])

        print("\n--- HOÀN TẤT GIAI ĐOẠN 1: ĐÃ LẤY XONG DỮ LIỆU HIỆU SUẤT ---")
        
        # === GIAI ĐOẠN 2: LẤY DANH MỤC SẢN PHẨM ===
        print("\n--- GIAI ĐOẠN 2: BẮT ĐẦU LẤY DANH MỤC SẢN PHẨM ---")
        self._report_progress("Bắt đầu lấy dữ liệu sản phẩm...", 50)

        product_catalog = self._get_product_catalog()
        if not product_catalog:
            print("CẢNH BÁO: Không thể lấy danh mục sản phẩm. Dữ liệu cuối cùng sẽ không có chi tiết sản phẩm.")

        # === GIAI ĐOẠN 3: LÀM GIÀU DỮ LIỆU VÀ HOÀN TẤT ===
        print("\n--- GIAI ĐOẠN 3: BẮT ĐẦU LÀM GIÀU DỮ LIỆU ---")
        self._report_progress("Bắt đầu làm giàu dữ liệu...", 90)

        product_info_map = self._create_product_info_map(product_catalog)
        final_data = self._enrich_with_product_details(all_performance_results, product_info_map)
        final_filtered_data = self._filter_empty_creatives(final_data)
        final_filtered_data = self._enrich_with_creative_metadata(final_filtered_data)
        

        return final_filtered_data
    
def _flatten_creative_report(
    campaign_data_list: List[Dict[str, Any]],
    context: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Flattens the raw creative report data into a list of rows.
    This logic mirrors the Google Apps Script _flattenTiktokCreativeReport function.
    """
    flattened_data = []
    for campaign in campaign_data_list:
        if not campaign.get("performance_data"):
            continue
        
        for perf_group in campaign["performance_data"]:
            if not perf_group.get("creative_details"):
                continue

            for creative in perf_group["creative_details"]:
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

                    # Product Group & Details info
                    "item_group_id": perf_group.get("dimensions", {}).get("item_group_id"),
                    "product_name": perf_group.get("product_details", {}).get("product_title"),
                    "product_status": perf_group.get("product_details", {}).get("product_status"),
                    "product_image_url": perf_group.get("product_details", {}).get("product_image_url"),
                    
                    # Creative Info
                    "item_id": creative.get("item_id"),
                    "video_url": "https://www.tiktok.com/@user/video/" + creative.get("item_id"),
                    "title": creative.get("metadata", {}).get("title"),
                    "tt_account_name": creative.get("metadata", {}).get("tt_account_name"),
                    "tt_account_profile_image_url": creative.get("metadata", {}).get("tt_account_profile_image_url"),
                    "product_img": creative.get("metadata", {}).get("product_img") or perf_group.get("product_details", {}).get("product_image_url"),
                }
                # Add all metrics dynamically
                row.update(creative.get("metrics", {}))
                flattened_data.append(row)
                
    return flattened_data


# --- HÀM CHÍNH ĐỂ CHẠY (VÍ DỤ SỬ DỤNG) ---
import os
if __name__ == "__main__":
    ACCESS_TOKEN = os.getenv("TIKTOK_ACCESS_TOKEN")
    ADVERTISER_ID = "6967547145545105410"
    STORE_ID = "7494600253418473607"
    START_DATE = "2025-09-01"
    END_DATE = "2025-09-18"

    if not ACCESS_TOKEN:
        print("Lỗi: Vui lòng thiết lập biến môi trường TIKTOK_ACCESS_TOKEN trong file .env")
    else:
        start_time = time.perf_counter()
        
        try:
            # 1. Khởi tạo đối tượng reporter với các ID và key cần thiết
            reporter = GMVCampaignCreativeDetailReporter(
                access_token=ACCESS_TOKEN,
                advertiser_id=ADVERTISER_ID,
                store_id=STORE_ID
            )
            
            # 2. Gọi hàm get_data để thực hiện toàn bộ quy trình
            final_data = reporter.get_data([{
                'start': START_DATE,
                'end': END_DATE
            }])

            # 3. Xử lý kết quả trả về (tính toán, lưu file, etc.)
            if final_data:
                # Tính tổng cost
                total_creative_cost = sum(
                    float(creative.get("metrics", {}).get("cost", 0))
                    for campaign in final_data
                    for product in campaign.get("performance_data", [])
                    for creative in product.get("creative_details", [])
                )
                
                # Ghi file
                output_filename = "GMV_Campaign_creative_detail.json"
                with open(output_filename, "w", encoding="utf-8") as f:
                    json.dump(final_data, f, ensure_ascii=False, indent=4)
                
                print("\n--- HOÀN THÀNH TOÀN BỘ ---")
                print(f"Đã xử lý và lưu kết quả của {len(final_data)} campaigns vào file '{output_filename}'")
                print(f"💰 Tổng chi phí (cost) của các creatives có hiệu suất: {total_creative_cost:,.0f} VND")
            else:
                print("\n--- HOÀN THÀNH ---")
                print("Không có dữ liệu nào được trả về.")

        except ValueError as ve:
            print(f"Lỗi khởi tạo: {ve}")
        except Exception as e:
            print(f"Một lỗi không mong muốn đã xảy ra: {e}")
            raise Exception(f"{e}")

        end_time = time.perf_counter()
        print(f"\nTổng thời gian thực thi: {end_time - start_time:.2f} giây.")
        
