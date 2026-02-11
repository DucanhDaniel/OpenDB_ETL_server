"""
Facebook Performance Report - Class Implementation
Lấy dữ liệu tổng hợp (không breakdown theo ngày) từ Facebook Graph API
"""

from typing import List, Dict, Any, Optional
from .base_processor import FacebookAdsBaseReporter
import logging
import json, time
from .constant import CONVERSION_METRICS_MAP, EFFECTIVE_STATUS_FILTERS


logger = logging.getLogger("FacebookPerformanceReport")


class FacebookPerformanceReporter(FacebookAdsBaseReporter):
    """
    Class để lấy Performance Report từ Facebook (tổng hợp theo time range).
    Hỗ trợ các level: account, campaign, adset, ad
    Khác biệt với Daily Report: KHÔNG có time_increment, lấy tổng cho toàn bộ khoảng thời gian
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.page_map = {}  # Cache for page info
    
    
    def _create_nested_level_url(
        self,
        account: Dict[str, str],
        start_date: str,
        end_date: str,
        level: str,
        template_config: Dict[str, Any],
        selected_fields: List[str]
    ) -> str:
        """
        Tạo URL cho adset/ad/campaign level (cấu trúc lồng nhau).
        PERFORMANCE MODE: Không có time_increment
        
        Returns:
            Relative URL string
        """
        object_fields_key = f"{level}_fields"
        api_object_fields = template_config.get(object_fields_key, [])
        
        final_object_fields = set(["id", "name"])
        final_insight_fields = set(["account_id"])
        needs_creative_fields = False
        
        # Process selected fields
        for field in selected_fields:
            if field.startswith('creative_') or field == 'page_name' or field == 'actor_id':
                needs_creative_fields = True
            
            if field in CONVERSION_METRICS_MAP:
                # Parse api_field để lấy parent field
                api_field = CONVERSION_METRICS_MAP[field]["api_field"]
                if api_field.startswith("actions:"):
                    final_insight_fields.add("actions")
                elif api_field.startswith("action_values:"):
                    final_insight_fields.add("action_values")
                elif api_field.startswith("cost_per_action_type:"):
                    final_insight_fields.add("cost_per_action_type")
                elif api_field == "purchase_roas":
                    final_insight_fields.add("purchase_roas")
                else:
                    # Video metrics, etc.
                    parent_field = CONVERSION_METRICS_MAP[field].get("parent_field")
                    if parent_field:
                        final_insight_fields.add(parent_field)
                    else:
                        final_insight_fields.add(api_field)
            elif field in ["campaign_name", "campaign_id"]:
                final_object_fields.add("campaign{name,id}")
            elif field == "objective":
                if level == "campaign":
                    final_object_fields.add("objective")
            elif level == "ad" and field in ["adset_name", "adset_id"]:
                final_object_fields.add("adset{name,id}")
            elif field in api_object_fields:
                final_object_fields.add(field)
            elif field in template_config.get("insight_fields", []):
                final_insight_fields.add(field)
        
        # Add creative fields if needed
        if needs_creative_fields and level == "ad":
            creative_field = next((f for f in api_object_fields if f.startswith("creative{")), None)
            if creative_field:
                final_object_fields.add(creative_field)
        
        # Build fields string with insights
        # KHÁC BIỆT: Không có time_increment(1)
        time_range_param = f"time_range({{'since':'{start_date}','until':'{end_date}'}})"
        insight_fields_str = ",".join(final_insight_fields)
        fields_str = ",".join(final_object_fields)
        
        # PERFORMANCE MODE: Chỉ có time_range, KHÔNG có time_increment
        fields_str += f",insights.{time_range_param}{{{insight_fields_str}}}"
        
        # Build params
        params = {"fields": fields_str, "limit": 500}
        
        # Add effective_status filter
        status_filter = EFFECTIVE_STATUS_FILTERS.get(level)
        if status_filter:
            params["effective_status"] = json.dumps(status_filter)
        
        from urllib.parse import urlencode
        query_string = urlencode(params, safe='{}(),')
        
        url = f"{account['id']}/{level}s?{query_string}"
        logger.debug(f"Created performance URL: {url}")
        return url
    
    def _prepare_initial_requests(
        self,
        accounts_to_process: List[Dict[str, str]],
        start_date: str,
        end_date: str,
        level: str,
        template_config: Dict[str, Any],
        selected_fields: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Chuẩn bị tất cả requests ban đầu.
        PERFORMANCE MODE: Mỗi account chỉ có 1 request (không chia chunks)
        
        Returns:
            List of {"url": str, "metadata": dict}
        """
        all_requests = []
        
        for account in accounts_to_process:
            url = self._create_nested_level_url(
                account, 
                start_date,
                end_date,
                level, 
                template_config, 
                selected_fields
            )
            
            if url:
                all_requests.append({
                    "url": url,
                    "metadata": {
                        "account": account,
                        "level": level,
                        "start_date": start_date,
                        "end_date": end_date
                    }
                })
        
        return all_requests
    
    # ==================== RESPONSE PROCESSING ====================
    
    def _process_response(
        self,
        response_body: Dict[str, Any],
        request_metadata: Dict[str, Any],
        selected_fields: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Xử lý response cho performance report.
        Insights data không có breakdown theo ngày, chỉ có 1 record tổng hợp
        """
        extracted_rows = []
        
        if not response_body.get("data"):
            return extracted_rows
        
        level = request_metadata["level"]
        
        for item in response_body["data"]:
            # Base info từ object level (ad, adset, campaign)
            final_row = {k: v for k, v in item.items() if k != "insights"}
            
            # Extract insights data (chỉ có 1 record)
            insight_data = item.get("insights", {}).get("data", [])
            if insight_data:
                # Insights chỉ có 1 item (tổng hợp)
                final_row.update(insight_data[0])
            
            # Process creative fields
            if item.get("creative"):
                creative = item["creative"]
                final_row["creative_id"] = creative.get("id", "")
                final_row["actor_id"] = str(creative.get("actor_id", ""))
                final_row["page_name"] = self.page_map.get(str(creative.get("actor_id", "")), "Page không xác định")
                final_row["creative_title"] = creative.get("title", "")
                final_row["creative_body"] = creative.get("body", "")
                final_row["creative_thumbnail_url"] = f"=IMAGE(\"{creative.get('thumbnail_url', '')}\")" if creative.get('thumbnail_url') else ""
                final_row["creative_thumbnail_raw_url"] = creative.get("thumbnail_url", "")
                final_row["creative_link"] = f"https://facebook.com/{creative.get('object_story_id', '')}" if creative.get('object_story_id') else ""
            
            # Map Campaign ID/Name
            if level == "campaign":
                if not final_row.get("campaign_id") and final_row.get("id"):
                    final_row["campaign_id"] = final_row["id"]
                if not final_row.get("campaign_name") and final_row.get("name"):
                    final_row["campaign_name"] = final_row["name"]
            elif item.get("campaign"):
                final_row["campaign_name"] = item["campaign"].get("name")
                final_row["campaign_id"] = item["campaign"].get("id")
            
            # Map Adset ID/Name
            if item.get("adset"):
                final_row["adset_name"] = item["adset"].get("name")
                final_row["adset_id"] = item["adset"].get("id")
            
            # Add account info
            final_row["account_id"] = request_metadata["account"]["id"]
            final_row["account_name"] = request_metadata["account"]["name"]
            
            # Add date range (PERFORMANCE: date_start/stop là toàn bộ khoảng)
            final_row["date_start"] = request_metadata["start_date"]
            final_row["date_stop"] = request_metadata["end_date"]
            
            # Cleanup nested objects
            final_row.pop("insights", None)
            final_row.pop("creative", None)
            final_row.pop("campaign", None)
            final_row.pop("adset", None)
            
            # Flatten action metrics
            extracted_rows.append(self._flatten_action_metrics(final_row, selected_fields))
        
        return extracted_rows
    
    def _extract_pagination_urls(
        self,
        response_body: Dict[str, Any],
        request_metadata: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Extract pagination URLs (chỉ top-level, không có nested insights pagination)
        """
        pagination_requests = []
        
        # Top-level pagination only
        top_level_next = response_body.get("paging", {}).get("next")
        if top_level_next:
            pagination_requests.append({
                "url": self._get_relative_url(top_level_next),
                "metadata": request_metadata
            })
            logger.debug(f"  → Top-level pagination detected")
        
        return pagination_requests
    
    def _process_wave_responses(
        self,
        all_responses: List[Dict[str, Any]],
        selected_fields: List[str]
    ) -> Dict[str, Any]:
        """
        Xử lý tất cả responses từ một wave.
        
        Returns:
            {
                "data_rows": List,
                "next_wave_requests": List,
                "failed_requests": List
            }
        """
        data_rows = []
        next_wave_requests = []
        failed_requests = []
        
        logger.info(f"Xử lý {len(all_responses)} responses...")
        
        for response in all_responses:
            request_metadata = response["metadata"]
            
            # Handle errors với logging chi tiết
            if response["status_code"] != 200:
                error_detail = response.get("error", {})
                if (response["status_code"] == 403 or response["status_code"] == 400):
                    raise Exception(response["error"]["message"])
                
                self._report_progress(message = 
                    "\n  ✗ Request thất bại:"
                    + f"\n     Status Code: {response['status_code']}"
                    + f"\n     URL: {response.get('original_url', 'N/A')}"
                    + f"\n     Error Message: {error_detail.get('message', 'Unknown error')}"
                    + f"\n     Error Type: {error_detail.get('type', 'N/A')}"
                    + f"\n     Error Code: {error_detail.get('code', 'N/A')}"
                )
                
                if 500 <= response["status_code"] < 600:
                    failed_requests.append({
                        "url": response["original_url"],
                        "metadata": request_metadata
                    })
                continue
            
            response_body = response.get("data")
            if not response_body:
                logger.warning(f"  ⚠ Response có status 200 nhưng không có data")
                continue
            
            # Process data
            rows = self._process_response(response_body, request_metadata, selected_fields)
            data_rows.extend(rows)
            logger.info(f"  ✓ Extracted {len(rows)} rows from response")
            
            # Handle pagination (chỉ top-level)
            pagination_urls = self._extract_pagination_urls(response_body, request_metadata)
            next_wave_requests.extend(pagination_urls)
        
        return {
            "data_rows": data_rows,
            "next_wave_requests": next_wave_requests,
            "failed_requests": failed_requests
        }
    
    def _retry_failed_requests(
        self,
        failed_requests: List[Dict[str, Any]],
        selected_fields: List[str],
        output_callback: callable = None
    ) -> int:
        """
        Retry các requests thất bại.
        Simplified version - không có auto-reduce cho performance report
        """
        if not failed_requests:
            return 0
        
        logger.info(f"\n===== RETRY {len(failed_requests)} failed requests =====")
        time.sleep(3)
        
        BATCH_SIZE = 10
        MAX_RETRIES = 3
        total_retry_written = 0
        
        queue = [
            {"url": req["url"], "metadata": req["metadata"], "retry_count": 0}
            for req in failed_requests
        ]
        
        while queue:
            current_batch = queue[:BATCH_SIZE]
            queue = queue[BATCH_SIZE:]
            
            batch_urls = [item["url"] for item in current_batch]
            current_batch_rows = []
            
            logger.info(f"\n➤ Retry batch of {len(current_batch)} items")
            
            try:
                response_json = self._send_batch_request(batch_urls)
                
                if not response_json or "results" not in response_json:
                    logger.error("Batch request failed")
                    for item in current_batch:
                        if item["retry_count"] < MAX_RETRIES:
                            item["retry_count"] += 1
                            queue.append(item)
                    time.sleep(5)
                    continue

                # backoff retry
                if hasattr(self, 'backoff_handler'):
                    self.backoff_handler.analyze_and_backoff(
                        responses=response_json["results"],
                        summary=response_json.get("summary")
                    )
                else:
                    # Fallback to old logic if backoff_handler not initialized
                    if "summary" in response_json:
                        # print("Tồn tại summary: ", response_json["summary"])
                        self._perform_backoff_if_needed(response_json["summary"])
                
                for index, result in enumerate(response_json["results"]):
                    queue_item = current_batch[index]

                    if result["status_code"] == 200 and result.get("data"):
                        rows = self._process_response(
                            result["data"],
                            queue_item["metadata"],
                            selected_fields
                        )
                        
                        if rows:
                            current_batch_rows.extend(rows)
                        
                        # Handle pagination
                        pagination = self._extract_pagination_urls(
                            result["data"],
                            queue_item["metadata"]
                        )
                        
                        for pag_req in pagination:
                            queue.append({
                                "url": pag_req["url"],
                                "metadata": pag_req["metadata"],
                                "retry_count": 0
                            })
                    else:
                        # Retry
                        if queue_item["retry_count"] < MAX_RETRIES:
                            queue_item["retry_count"] += 1
                            queue.append(queue_item)
                
                # Write incremental
                if current_batch_rows:
                    logger.info(f"  ✎ Writing {len(current_batch_rows)} rows...")
                    if output_callback:
                        output_callback(current_batch_rows)
                    total_retry_written += len(current_batch_rows)
                
            except Exception as e:
                logger.error(f"Batch error: {e}")
                for item in current_batch:
                    if item["retry_count"] < MAX_RETRIES:
                        item["retry_count"] += 1
                        queue.append(item)
            
            if queue:
                time.sleep(2)
        
        logger.info(f"✓ Retry completed: {total_retry_written} rows written")
        return total_retry_written
    
    # ==================== MAIN FUNCTION ====================
    
    def get_report(
        self,
        accounts_to_process: List[Dict[str, str]],
        start_date: str,
        end_date: str,
        template_name: str,
        selected_fields: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Lấy Performance Report data (tổng hợp theo time range, không breakdown ngày).
        
        Args:
            accounts_to_process: List of {"id": "act_xxx", "name": "Account Name"}
            start_date: YYYY-MM-DD
            end_date: YYYY-MM-DD
            template_name: Name of template
            selected_fields: List of fields to retrieve
            
        Returns:
            List of data rows
        """
        template_config = FacebookAdsBaseReporter.get_facebook_template_config_by_name(template_name)
        
        if not template_config:
            raise ValueError(f"Template '{template_name}' not found")
        
        logger.info(f"Bắt đầu lấy Performance Report từ {start_date} đến {end_date}")
        logger.info(f"Template: {template_name}, Level: {template_config['api_params']['level']}")
        self._report_progress("Bắt đầu lấy Performance Report...", 5)
        
        # Load page map if needed
        if "page_name" in selected_fields:
            logger.info("Loading page map...")
            self.page_map = self.get_accessible_page_map()
        
        # Prepare initial requests
        level = template_config["api_params"]["level"]
        all_initial_requests = self._prepare_initial_requests(
            accounts_to_process,
            start_date,
            end_date,
            level,
            template_config,
            selected_fields
        )
        
        logger.info(f"✓ Đã chuẩn bị {len(all_initial_requests)} requests ban đầu.")
        if all_initial_requests:
            logger.info(f"Sample URL: {all_initial_requests[0]['url'][:300]}...")
        
        self._report_progress(f"Đã chuẩn bị {len(all_initial_requests)} requests", 10)
        
        # Process wave-by-wave
        requests_for_current_wave = all_initial_requests
        wave_count = 1
        all_data_rows = []
        all_failed_requests = []
        
        while requests_for_current_wave:
            self._report_progress(f"Đang xử lý đợt {wave_count}...", 20 + (wave_count * 10))
            
            try:
                # Execute wave
                all_responses_for_wave = self._execute_wave(
                    requests_for_current_wave,
                    self.DEFAULT_BATCH_SIZE,
                    self.DEFAULT_SLEEP_TIME,
                    wave_count
                )
                
                # Process responses
                wave_result = self._process_wave_responses(
                    all_responses_for_wave,
                    selected_fields
                )
                
                # Collect data
                all_data_rows.extend(wave_result["data_rows"])
                all_failed_requests.extend(wave_result["failed_requests"])
                
                logger.info(f"--> Sóng {wave_count} ghi {len(wave_result['data_rows'])} dòng.")
                
                # Prepare next wave
                requests_for_current_wave = wave_result["next_wave_requests"]
                wave_count += 1
                
            except Exception as e:
                raise Exception(f"❌ DỪNG XỬ LÝ: {e}")
                
                if "Rate limit backoff quá lâu" in str(e):
                    raise Exception("API đang quá tải, vui lòng thử lại sau.")
                
                logger.warning(f"Bỏ qua wave {wave_count} do lỗi: {e}")
                break
        
        # Retry failed requests
        if all_failed_requests:
            logger.info(f"\n⚠ Có {len(all_failed_requests)} requests thất bại. Bắt đầu retry...")
            
            def write_callback(rows):
                nonlocal all_data_rows
                all_data_rows.extend(rows)
            
            retry_rows = self._retry_failed_requests(
                all_failed_requests,
                selected_fields,
                output_callback=write_callback
            )
            
            logger.info(f"✓ Retry đã ghi thêm {retry_rows} rows")
        
        logger.info(f"✓ Hoàn thành với {len(all_data_rows)} rows")
        self._report_progress("Hoàn thành!", 100)
        
        return all_data_rows

def write_to_file(output_filename, data): 

    # 3. Ghi dữ liệu ra file
    try:
        with open(output_filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"Đã ghi thành công dữ liệu vào file: {output_filename}")
    except Exception as e:
        print(f"Có lỗi khi ghi file: {e}")
        
# Example usage
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    ACCESS_TOKEN = os.getenv("FACEBOOK_ACCESS_TOKEN")
    
    reporter = FacebookPerformanceReporter(
        access_token=ACCESS_TOKEN,
        email="test@example.com"
    )
    
    template_name = "Ad Creative Report"
    accounts = [
        {"id": "act_650248897235348", "name": "Test Account"}
    ]
    
    data = reporter.get_report(
        accounts_to_process=accounts,
        start_date="2025-12-01",
        end_date="2025-12-31",
        template_name=template_name,
        selected_fields=["id", "name", "adset_id", "adset_name", "campaign_id", "campaign_name", "account_id", "account_name", "status", "effective_status", 
"creative_id", "actor_id", "page_name", "creative_title", "creative_body", "creative_thumbnail_url", "creative_thumbnail_raw_url", "creative_link", "spend", "impressions", 
"Leads", "Cost Leads", "reach", "clicks", "ctr", "cpc", "cpm", "New Messaging Connections", "Cost per New Messaging", "New Messaging Connections (N)", 
"Cost per New Messaging (N)", "Purchases", "Purchase Value", "Purchase ROAS", "Hoàn tất đăng ký", "Chi phí / Hoàn tất đăng ký", "ThruPlay", "Chi phí / ThruPlay", "date_start", "date_stop"
]
    )
    
    print(f"Got {len(data)} rows")
    
    total_spend = 0
    for val in data:    
        if (val.get("spend")):
            total_spend += int(val.get("spend"))
    print("Total Spend: ", total_spend)
    
    write_to_file(f"data/{template_name}.json", data)