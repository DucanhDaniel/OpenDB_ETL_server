"""
Facebook Breakdown Report - Class Implementation
Lấy dữ liệu breakdown theo dimensions từ Facebook Graph API
Examples: age, gender, placement, country, device, etc.
"""

from typing import List, Dict, Any, Optional
from services.facebook.base_processor import FacebookAdsBaseReporter
import logging
import json, time
from services.facebook.constant import CONVERSION_METRICS_MAP
from .helper import write_to_file


logger = logging.getLogger("FacebookBreakdownReport")


class FacebookBreakdownReporter(FacebookAdsBaseReporter):
    """
    Class để lấy Breakdown Report từ Facebook.
    
    Breakdown dimensions examples:
    - age, gender
    - placement
    - country, region
    - device_platform
    - publisher_platform
    - hourly_stats_aggregated_by_audience_time_zone
    
    Response structure: FLAT (không có nested insights)
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def _create_breakdown_url(
        self,
        account: Dict[str, str],
        start_date: str,
        end_date: str,
        template_config: Dict[str, Any],
        selected_fields: List[str]
    ) -> str:
        """
        Tạo URL cho breakdown report.
        
        Breakdown report có cấu trúc PHẲNG:
        - Không dùng nested insights
        - Gọi trực tiếp /insights với breakdowns param
        - Response.data chứa records đã breakdown
        
        Returns:
            Relative URL string
        """
        api_params = template_config.get("api_params", {})
        level = api_params.get("level", "ad")
        breakdowns = api_params.get("breakdowns", [])
        time_increment = api_params.get("time_increment")
        
        if not breakdowns:
            raise ValueError(f"Template config must have 'breakdowns' parameter")
        
        # Build fields set
        fields_for_api = set([
            f"{level}_id",
            f"{level}_name", 
            "account_id",
            "account_name"
        ])
        
        # Add date fields if time_increment exists
        if time_increment:
            fields_for_api.add("date_start")
            fields_for_api.add("date_stop")
        
        # Process selected fields
        for field in selected_fields:
            if field in CONVERSION_METRICS_MAP:
                # Parse api_field to get parent field
                api_field = CONVERSION_METRICS_MAP[field]["api_field"]
                if api_field.startswith("actions:"):
                    fields_for_api.add("actions")
                elif api_field.startswith("action_values:"):
                    fields_for_api.add("action_values")
                elif api_field.startswith("cost_per_action_type:"):
                    fields_for_api.add("cost_per_action_type")
                elif api_field == "purchase_roas":
                    fields_for_api.add("purchase_roas")
                else:
                    # Video metrics, etc.
                    parent_field = CONVERSION_METRICS_MAP[field].get("parent_field")
                    if parent_field:
                        fields_for_api.add(parent_field)
                    else:
                        fields_for_api.add(api_field)
            elif field in template_config.get("insight_fields", []):
                fields_for_api.add(field)
        
        # Format breakdowns param
        if isinstance(breakdowns, list):
            breakdowns_param = ",".join(breakdowns)
        else:
            breakdowns_param = breakdowns
        
        # Build params (inherit from template config)
        params = {
            **api_params,  # Inherit all params including time_increment
            "level": level,
            "breakdowns": breakdowns_param,
            "fields": ",".join(fields_for_api),
            "time_range": json.dumps({"since": start_date, "until": end_date}),
            "use_account_attribution_setting": "true",
            "limit": 500  # Breakdown có nhiều rows, giảm limit
        }
        
        from urllib.parse import urlencode
        query_string = urlencode(params)
        
        url = f"{account['id']}/insights?{query_string}"
        logger.debug(f"Created breakdown URL: {url}")
        return url
    
    def _prepare_initial_requests(
        self,
        accounts_to_process: List[Dict[str, str]],
        start_date: str,
        end_date: str,
        template_config: Dict[str, Any],
        selected_fields: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Chuẩn bị tất cả requests ban đầu.
        Mỗi account = 1 request
        
        Returns:
            List of {"url": str, "metadata": dict}
        """
        all_requests = []
        level = template_config["api_params"]["level"]
        
        for account in accounts_to_process:
            url = self._create_breakdown_url(
                account,
                start_date,
                end_date,
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
        Xử lý response cho breakdown report.
        
        Breakdown response structure (FLAT):
        {
          "data": [
            {
              "ad_id": "123",
              "ad_name": "Ad 1",
              "age": "18-24",  ← Breakdown dimension
              "gender": "male",  ← Breakdown dimension
              "spend": "100",
              "impressions": "5000",
              "date_start": "2025-01-01",  ← If time_increment=1
              "date_stop": "2025-01-01"
            }
          ]
        }
        """
        extracted_rows = []
        
        if not response_body.get("data"):
            return extracted_rows
        
        insights_data = response_body["data"]
        
        for row in insights_data:
            final_row = {**row}
            
            # Add account info
            final_row["account_id"] = request_metadata["account"]["id"]
            final_row["account_name"] = request_metadata["account"]["name"]
            
            # Handle date fields
            # Ưu tiên date từ row data (nếu có time_increment)
            # Nếu không có thì dùng date từ metadata
            if not final_row.get("date_start"):
                final_row["date_start"] = request_metadata["start_date"]
            if not final_row.get("date_stop"):
                final_row["date_stop"] = request_metadata["end_date"]
            
            # Special handling for hourly breakdown
            if final_row.get("hourly_stats_aggregated_by_audience_time_zone"):
                final_row["hour_of_day"] = final_row["hourly_stats_aggregated_by_audience_time_zone"]
            else:
                final_row["hour_of_day"] = ""
            
            # Flatten action metrics
            extracted_rows.append(self._flatten_action_metrics(final_row, selected_fields))
        
        return extracted_rows
    
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
            
            # Handle errors
            if response["status_code"] != 200:
                error_detail = response.get("error", {})

                if (response["status_code"] == 403 or response["status_code"] == 400):
                    raise Exception(response["error"]["message"])

                self._report_progress(message=
                    f"\n  ✗ Request thất bại:"
                    + f"\n     Status Code: {response['status_code']}"
                    + f"\n     Error: {error_detail.get('message', 'Unknown error')}"
                )
                
                if 500 <= response["status_code"] < 600:
                    failed_requests.append({
                        "url": response.get("original_url"),
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
            
            # Handle pagination (chỉ có top-level)
            next_url = response_body.get("paging", {}).get("next")
            if next_url:
                next_wave_requests.append({
                    "url": self._get_relative_url(next_url),
                    "metadata": request_metadata  # Giữ nguyên metadata
                })
                logger.debug(f"  → Pagination detected")
        
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
        """Retry các requests thất bại (simplified version)"""
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
                        next_url = result["data"].get("paging", {}).get("next")
                        if next_url:
                            queue.append({
                                "url": self._get_relative_url(next_url),
                                "metadata": queue_item["metadata"],
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
        Lấy Breakdown Report data.
        
        Args:
            accounts_to_process: List of {"id": "act_xxx", "name": "Account Name"}
            start_date: YYYY-MM-DD
            end_date: YYYY-MM-DD
            template_name: Name of template (must have breakdowns config)
            selected_fields: List of fields to retrieve
            
        Returns:
            List of data rows
        """
        template_config = FacebookAdsBaseReporter.get_facebook_template_config_by_name(template_name)
        
        if not template_config:
            raise ValueError(f"Template '{template_name}' not found")
        
        if not template_config.get("api_params", {}).get("breakdowns"):
            raise ValueError(f"Template '{template_name}' must have 'breakdowns' parameter")
        
        logger.info(f"Bắt đầu lấy Breakdown Report từ {start_date} đến {end_date}")
        logger.info(f"Template: {template_name}, Breakdowns: {template_config['api_params']['breakdowns']}")
        self._report_progress("Bắt đầu lấy Breakdown Report...", 5)
        
        # Prepare initial requests
        all_initial_requests = self._prepare_initial_requests(
            accounts_to_process,
            start_date,
            end_date,
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


# Example usage
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    ACCESS_TOKEN = os.getenv("FACEBOOK_ACCESS_TOKEN")
    
    reporter = FacebookBreakdownReporter(
        access_token=ACCESS_TOKEN,
        email="test@example.com"
    )
    
    # Example: Age & Gender breakdown
    template_name = "Campaign Performance by Age"
    accounts = [
        {"id": "act_650248897235348", "name": "Test Account"}
    ]
    
    data = reporter.get_report(
        accounts_to_process=accounts,
        start_date="2025-12-01",
        end_date="2025-12-31",
        template_name=template_name,
        selected_fields=[
"campaign_id", "campaign_name", "account_id", "account_name", "age", "spend", "New Messaging Connections", "Cost per New Messaging", "New Messaging Connections (N)", "Cost per New Messaging (N)", 
"Leads", "Cost Leads", "Purchases", "Cost Purchases", "Purchase Value", "Purchase ROAS", "Website Purchases", "On-Facebook Purchases", "Hoàn tất đăng ký", "Chi phí / Hoàn tất đăng ký", 
"ThruPlay", "Chi phí / ThruPlay", "date_start", "date_stop"
        ]
    )
    
    print(f"Got {len(data)} rows")
    print("Sample:", data[0] if data else "No data")
    
    total_spend = 0
    for val in data:
        total_spend += int(val.get("spend"))
    print("Total Spend: ", total_spend)
    
    write_to_file(f'data/{template_name}.json', data)