"""
Facebook Daily Report - Class Implementation
Lấy dữ liệu chi tiết theo ngày từ Facebook Graph API
"""

from typing import List, Dict, Any, Optional
from .base_processor import FacebookAdsBaseReporter
import logging
import json, time
from .constant import CONVERSION_METRICS_MAP, EFFECTIVE_STATUS_FILTERS


logger = logging.getLogger("FacebookDailyReport")


class FacebookDailyReporter(FacebookAdsBaseReporter):
    """
    Class để lấy Daily Report từ Facebook với breakdown theo ngày.
    Hỗ trợ các level: account, campaign, adset, ad
    """

    # Constants cho field sanitization
    IMPRESSION_BASED_METRICS = [
        'spend', 'impressions', 'cpm', 'cpp', 'ctr', 'reach', 'frequency'
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.page_map = {}  # Cache for page info

    # ==================== HELPER METHODS ====================

    @staticmethod
    def _resolve_api_field_name(header_name: str) -> str:
        """
        Resolve tên API field từ header name.
        Nếu field nằm trong CONVERSION_METRICS_MAP, trả về api_field hoặc parent_field.
        """
        if header_name in CONVERSION_METRICS_MAP:
            mapped = CONVERSION_METRICS_MAP[header_name]
            return mapped.get("api_field") or mapped.get("parent_field")
        return header_name


    # ==================== URL CREATION ====================

    def _create_flat_level_url(
        self,
        account: Dict[str, str],
        chunk: Dict[str, str],
        template_config: Dict[str, Any],
        selected_fields: List[str]
    ) -> str:
        """
        Tạo URL cho account/campaign level (cấu trúc phẳng).

        Returns:
            Relative URL string
        """
        level = template_config["api_params"]["level"]
        breakdowns = template_config["api_params"].get("breakdowns")
        is_daily_report = bool(template_config["api_params"].get("time_increment"))

        # Build fields set
        final_fields = set()
        has_impression_metrics = False

        for field in selected_fields:
            api_field_name = self._resolve_api_field_name(field)

            # --- CHECK IMPRESSION METRICS ---
            if api_field_name in self.IMPRESSION_BASED_METRICS or field in self.IMPRESSION_BASED_METRICS:
                has_impression_metrics = True

            # --- ADD FIELD ---
            if field in CONVERSION_METRICS_MAP:
                final_fields.add(CONVERSION_METRICS_MAP[field]["parent_field"])
            elif field in template_config.get("insight_fields", []):
                final_fields.add(field)

        # Add required fields
        for f in ["account_id", "account_name", "campaign_id", "campaign_name", "date_start", "date_stop"]:
            final_fields.add(f)

        # Build params
        params = {
            **template_config["api_params"],
            "fields": ",".join(final_fields),
            "time_range": json.dumps({"since": chunk["start"], "until": chunk["end"]}),
            "limit": 1000,
            "filtering":[{'field':'spend','operator':'GREATER_THAN','value':'0'}]
        }

        # --- REMOVE action_report_time if has impression metrics ---
        if has_impression_metrics and params.get("action_report_time") == "conversion":
            logger.debug(f"  ⊗ Removing action_report_time (has impression metrics)")
            params.pop("action_report_time", None)

        # Create query string
        from urllib.parse import urlencode
        query_string = urlencode(params)

        return f"{account['id']}/insights?{query_string}"
    
    def _create_nested_level_url(
        self,
        account: Dict[str, str],
        chunk: Dict[str, str],
        level: str,
        template_config: Dict[str, Any],
        selected_fields: List[str]
    ) -> str:
        """
        Tạo URL cho adset/ad level (cấu trúc lồng nhau).

        Returns:
            Relative URL string
        """
        object_fields_key = f"{level}_fields"
        api_object_fields = template_config.get(object_fields_key, [])
        breakdowns = template_config["api_params"].get("breakdowns")
        is_daily_report = bool(template_config["api_params"].get("time_increment"))

        # === OPTION A: Auto-load ALL template object fields ===
        # Template config is the source of truth for object structure
        final_object_fields = set(["id", "name"])
        for template_field in api_object_fields:
            final_object_fields.add(template_field)

        final_insight_fields = set(["account_id", "date_start", "date_stop"])
        has_impression_metrics = False

        # Process selected fields to build insight fields only
        for field in selected_fields:
            api_field_name = self._resolve_api_field_name(field)

            # --- CHECK IMPRESSION METRICS ---
            if api_field_name in self.IMPRESSION_BASED_METRICS or field in self.IMPRESSION_BASED_METRICS:
                has_impression_metrics = True

            # --- ADD INSIGHT FIELD ---
            if field in CONVERSION_METRICS_MAP:
                request_field = CONVERSION_METRICS_MAP[field].get("parent_field") or CONVERSION_METRICS_MAP[field].get("api_field")
                if request_field:
                    # Nếu là actions:xxx thì chỉ add "actions"
                    if request_field.startswith("actions:"):
                        final_insight_fields.add("actions")
                    else:
                        final_insight_fields.add(request_field)
            elif field in template_config.get("insight_fields", []):
                final_insight_fields.add(field)

        # Build fields string with insights
        time_range_param = f"time_range({{'since':'{chunk['start']}','until':'{chunk['end']}'}})"
        insight_fields_str = ",".join(final_insight_fields)
        fields_str = ",".join(final_object_fields)

        # Build insight function string conditionally
        time_increment = template_config["api_params"].get("time_increment", 1)
        insight_func_str = f"insights.{time_range_param}.time_increment({time_increment})"

        # Add action_report_time if exists and NOT has impression metrics
        action_report_time = template_config["api_params"].get("action_report_time")
        if action_report_time:
            if not has_impression_metrics or action_report_time != "conversion":
                insight_func_str += f".action_report_time({action_report_time})"
            else:
                logger.debug(f"  ⊗ Skipping action_report_time (has impression metrics)")

        # Add breakdowns if exists
        if breakdowns:
            breakdowns_str = ','.join(breakdowns) if isinstance(breakdowns, list) else breakdowns
            insight_func_str += f".breakdowns({breakdowns_str})"

        fields_str += f",{insight_func_str}{{{insight_fields_str}}}"

        # Build params
        params = {"fields": fields_str, "limit": 200, "filtering":[{'field':'spend','operator':'GREATER_THAN','value':'0'}]}

        # Add effective_status filter
        status_filter = EFFECTIVE_STATUS_FILTERS.get(level)
        if status_filter:
            params["effective_status"] = json.dumps(status_filter)

        from urllib.parse import urlencode
        query_string = urlencode(params, safe='{}(),')

        return f"{account['id']}/{level}s?{query_string}"
    
    def _prepare_initial_requests(
        self,
        accounts_to_process: List[Dict[str, str]],
        date_chunks: List[Dict[str, str]],
        level: str,
        template_config: Dict[str, Any],
        selected_fields: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Chuẩn bị tất cả requests ban đầu.
        
        Returns:
            List of {"url": str, "metadata": dict}
        """
        all_requests = []
        
        for account in accounts_to_process:
            for chunk in date_chunks:
                if level in ["account", "campaign"]:
                    url = self._create_flat_level_url(account, chunk, template_config, selected_fields)
                else:
                    url = self._create_nested_level_url(account, chunk, level, template_config, selected_fields)
                
                if url:
                    all_requests.append({
                        "url": url,
                        "metadata": {
                            "account": account,
                            "level": level
                        }
                    })
        
        return all_requests
    
    # ==================== RESPONSE PROCESSING ====================
    
    def _process_flat_level_response(
        self,
        response_body: Dict[str, Any],
        request_metadata: Dict[str, Any],
        selected_fields: List[str]
    ) -> List[Dict[str, Any]]:
        """Xử lý response cho flat level (account/campaign)"""
        extracted_rows = []
        daily_insights = response_body.get("data", [])
        level = request_metadata["level"]
        # print(daily_insights)
        for daily_data in daily_insights:
            final_row = {
                **daily_data,
                "account_id": request_metadata["account"]["id"],
                "account_name": request_metadata["account"]["name"]
            }
            
            # Map id/name sang campaign_id/campaign_name nếu là campaign
            if level == "campaign":
                if not final_row.get("campaign_id") and final_row.get("id"):
                    final_row["campaign_id"] = final_row["id"]
                if not final_row.get("campaign_name") and final_row.get("name"):
                    final_row["campaign_name"] = final_row["name"]
            
            extracted_rows.append(self._flatten_action_metrics(final_row, selected_fields))
        
        return extracted_rows
    
    def _process_nested_level_response(
        self,
        response_body: Dict[str, Any],
        request_metadata: Dict[str, Any],
        selected_fields: List[str]
    ) -> List[Dict[str, Any]]:
        """Xử lý response cho nested level (adset/ad)"""
        extracted_rows = []
        
        if not response_body.get("data"):
            return extracted_rows
        
        for item in response_body["data"]:
            if not item.get("insights") or not item["insights"].get("data"):
                continue
            
            parent_info = {k: v for k, v in item.items() if k != "insights"}
            
            # Process creative fields
            if item.get("creative"):
                creative = item["creative"]
                # print(creative)
                parent_info["creative_id"] = creative.get("id", "")
                parent_info["actor_id"] = str(creative.get("actor_id", ""))
                parent_info["page_name"] = self.page_map.get(str(creative.get("actor_id", "")), "Page không xác định")
                parent_info["creative_title"] = creative.get("title", "")
                parent_info["creative_name"] = creative.get("name", "")
                parent_info["creative_body"] = creative.get("body", "")
                parent_info["creative_thumbnail_url"] = f"=IMAGE(\"{creative.get('thumbnail_url', '')}\")" if creative.get('thumbnail_url') else ""
                parent_info["creative_thumbnail_raw_url"] = creative.get("thumbnail_url", "")
                parent_info["creative_link"] = f"https://facebook.com/{creative.get('object_story_id', '')}" if creative.get('object_story_id') else ""
                parent_info.pop("creative", None)
            
            # Process campaign/adset info
            if item.get("campaign"):
                parent_info["campaign_name"] = item["campaign"].get("name")
                parent_info["campaign_id"] = item["campaign"].get("id")
                parent_info.pop("campaign", None)
            
            if item.get("adset"):
                # print(item.get("adset"))
                parent_info["adset_name"] = item["adset"].get("name")
                parent_info["adset_id"] = item["adset"].get("id")
                if item["adset"].get("bid_strategy"):
                    parent_info["adset_bid_strategy"] = item["adset"]["bid_strategy"]
                bid = item["adset"].get("bid_amount") or item["adset"].get("daily_budget") or item["adset"].get("lifetime_budget")
                if bid:
                    parent_info["adset_bid_amount"] = bid
                parent_info.pop("adset", None)
            
            # Combine with insights data
            for daily_data in item["insights"]["data"]:
                final_row = {
                    **parent_info,
                    **daily_data,
                    "account_id": request_metadata["account"]["id"],
                    "account_name": request_metadata["account"]["name"]
                }
                extracted_rows.append(self._flatten_action_metrics(final_row, selected_fields))
        
        return extracted_rows
    
    def _process_wave_responses(
        self,
        all_responses: List[Dict[str, Any]],
        selected_fields: List[str]
    ) -> Dict[str, Any]:
        """
        Xử lý tất cả responses từ một wave.
        """
        data_rows = []
        next_wave_requests = []
        failed_requests = []
        
        logger.info(f"Xử lý {len(all_responses)} responses...")
        
        # write_to_file(f"data/debug/debug_wave_{int(time.time())}.json", all_responses) # Debug
        
        for response in all_responses:
            request_metadata = response["metadata"]
            level = request_metadata["level"]
            
            # --- HANDLE ERRORS ---
            if response["status_code"] != 200:
                if response["status_code"] in [400, 403]:
                    error_msg = response.get("error", {}).get("message", "Unknown Error")
                    raise Exception(error_msg)
                
                self._report_progress(message=f"  ✗ Request thất bại (Code: {response['status_code']})")
                print(response)
                if 500 <= response["status_code"] < 600:
                    failed_requests.append({
                        "url": response["original_url"],
                        "metadata": request_metadata
                    })
                continue
            
            response_body = response.get("data")
            if not response_body:
                continue
            
            try:
                processed = self._handle_successful_response(
                    response["data"], 
                    response["metadata"], 
                    selected_fields
                )
                data_rows.extend(processed["rows"])
                next_wave_requests.extend(processed["next_requests"])
            except Exception as e:
                logger.error(f"Error processing wave response: {e}")

        return {
            "data_rows": data_rows,
            "next_wave_requests": next_wave_requests,
            "failed_requests": failed_requests
        }

    def _handle_successful_response(
        self, 
        response_body: Dict[str, Any], 
        metadata: Dict[str, Any], 
        selected_fields: List[str]
    ) -> Dict[str, Any]:
        """
        Helper function: Xử lý 1 response thành công (200 OK).
        Tự động chuẩn hóa dữ liệu nested pagination và trích xuất next links.
        
        Returns:
            {
                "rows": List[Dict],
                "next_requests": List[Dict]  # List các {url, metadata} cho trang tiếp theo
            }
        """
        rows = []
        next_requests = []
        level = metadata["level"]
        is_nested_pagination = "parent_id" in metadata
        
        # 1. CHUẨN HÓA DATA (Logic fix nested pagination)
        processing_body = response_body
        
        if is_nested_pagination:
            flat_insights_data = response_body.get("data", [])
            
            # Tái tạo object cha
            reconstructed_item = {
                "id": metadata.get("parent_id"),
                "name": metadata.get("parent_name"),
                "insights": {"data": flat_insights_data}
            }
            
            # Map lại info cha từ metadata
            for key in ["creative_info", "campaign_info", "adset_info"]:
                if key in metadata:
                    clean_key = key.replace("_info", "")
                    reconstructed_item[clean_key] = metadata[key]
            
            processing_body = {"data": [reconstructed_item]}

        # 2. PROCESS ROWS
        if level in ["account", "campaign"]:
            rows = self._process_flat_level_response(processing_body, metadata, selected_fields)
        else:
            rows = self._process_nested_level_response(processing_body, metadata, selected_fields)
            
        # 3. EXTRACT PAGINATION (Next Links)
        
        # 3a. Top-level pagination (Chỉ khi không phải nested pagination)
        if not is_nested_pagination:
            top_level_next = response_body.get("paging", {}).get("next")
            if top_level_next:
                next_requests.append({
                    "url": self._get_relative_url(top_level_next),
                    "metadata": metadata
                })

        # 3b. Nested pagination (Insights next link)
        if is_nested_pagination:
            # Link nằm ngay ở root response body
            paging_info = response_body.get("paging", {})
            if paging_info.get("next"):
                next_requests.append({
                    "url": self._get_relative_url(paging_info["next"]),
                    "metadata": metadata
                })
        else:
            # Request gốc: Link nằm sâu trong từng item
            if response_body.get("data"):
                for item in response_body["data"]:
                    insights = item.get("insights", {})
                    insights_next = insights.get("paging", {}).get("next")
                    
                    if insights_next:
                        nested_metadata = {
                            **metadata,
                            "parent_id": item.get("id"),
                            "parent_name": item.get("name"),
                        }
                        # Lưu info cha để dùng lại
                        if item.get("campaign"): nested_metadata["campaign_info"] = item["campaign"]
                        if item.get("adset"): nested_metadata["adset_info"] = item["adset"]
                        if item.get("creative"): nested_metadata["creative_info"] = item["creative"]
                        
                        next_requests.append({
                            "url": self._get_relative_url(insights_next),
                            "metadata": nested_metadata
                        })

        return {"rows": rows, "next_requests": next_requests}
    
    def _retry_failed_requests(
        self,
        failed_requests: List[Dict[str, Any]],
        selected_fields: List[str],
        output_callback: callable = None
    ) -> int:
        """
        Retry các requests thất bại với batch processing.
        Sử dụng lại logic xử lý response chuẩn từ _handle_successful_response.
        """
        if not failed_requests:
            return 0
        
        logger.info(f"\n===== BATCH RETRY PROCESS ({len(failed_requests)} initial requests) =====")
        time.sleep(3)
        
        BATCH_SIZE = 10
        MAX_RETRIES = 3
        total_retry_written = 0
        
        # Queue với retry count
        queue = [
            {
                "url": req["url"],
                "metadata": req["metadata"],
                "retry_count": 0
            }
            for req in failed_requests
        ]
        
        while queue:
            current_batch = queue[:BATCH_SIZE]
            queue = queue[BATCH_SIZE:]
            
            batch_urls = [item["url"] for item in current_batch]
            current_batch_rows = []
            
            logger.info(f"\n➤ Processing batch of {len(current_batch)} items. Queue remaining: {len(queue)}")
            self._report_progress(message = f"\n➤ Processing batch of {len(current_batch)} items. Queue remaining: {len(queue)}")
            try:
                response_json = self._send_batch_request(batch_urls)
                

                if not response_json or "results" not in response_json:
                    logger.error("  ✗ Batch request failed completely. Re-queuing batch.")
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
                        print("Tồn tại summary: ", response_json["summary"])
                        self._perform_backoff_if_needed(response_json["summary"])
                
                # Process từng result
                for index, result in enumerate(response_json["results"]):
                    queue_item = current_batch[index]
                    
                    # CASE 1: SUCCESS (200 OK)
                    if result["status_code"] == 200 and result.get("data"):
                        try:
                            # [REFACTOR] Gọi hàm xử lý chung
                            processed_result = self._handle_successful_response(
                                result["data"], 
                                queue_item["metadata"], 
                                selected_fields
                            )
                            
                            # 1. Gộp rows
                            if processed_result["rows"]:
                                current_batch_rows.extend(processed_result["rows"])
                            
                            # 2. Xử lý Pagination (Next Links)
                            # Các link tiếp theo cũng được coi là task cần thực hiện, đưa vào queue
                            for next_req in processed_result["next_requests"]:
                                queue.append({
                                    "url": next_req["url"],
                                    "metadata": next_req["metadata"],
                                    "retry_count": 0 # Reset count cho trang mới
                                })
                                
                        except Exception as e:
                             logger.error(f"Error processing retry response: {e}")
                             # Nếu lỗi parsing, coi như request lỗi, cho retry lại
                             if queue_item["retry_count"] < MAX_RETRIES:
                                queue_item["retry_count"] += 1
                                queue.append(queue_item)

                    # CASE 2: REDUCE DATA ERROR
                    elif (result["status_code"] == 500 and 
                          result.get("error", {}).get("message", "").lower().find("reduce the amount of data") != -1):
                        
                        logger.warning(f"  ⚠ Reduce Data: {queue_item['metadata']['account']['name']}")
                        reduce_result = self._reduce_time_range_in_url(queue_item["url"], 2)
                        
                        if len(reduce_result["urls"]) > 1:
                            for split_url in reduce_result["urls"]:
                                queue.append({
                                    "url": split_url,
                                    "metadata": queue_item["metadata"],
                                    "retry_count": 0
                                })
                        else:
                            if queue_item["retry_count"] < MAX_RETRIES:
                                queue_item["retry_count"] += 1
                                queue.append(queue_item)
                    
                    # CASE 3: OTHER ERRORS
                    else:
                        if queue_item["retry_count"] < MAX_RETRIES:
                            queue_item["retry_count"] += 1
                            queue.append(queue_item)
                
                # Ghi data ngay sau khi xong batch
                if current_batch_rows:
                    logger.info(f"  ✎ Writing {len(current_batch_rows)} rows from this batch...")
                    self._report_progress(message = f"\nNhận được {len(current_batch_rows)} dòng")
                    if output_callback:
                        output_callback(current_batch_rows)
                    total_retry_written += len(current_batch_rows)
                    current_batch_rows = None
                
            except Exception as e:
                logger.error(f"  ✗ Batch processing error: {e}")
                for item in current_batch:
                    if item["retry_count"] < MAX_RETRIES:
                        item["retry_count"] += 1
                        queue.append(item)
            
            if queue:
                time.sleep(2)
        
        logger.info(f"✓ Batch Retry hoàn tất: Đã ghi thêm {total_retry_written} dòng.")
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
        Lấy Daily Report data.
        
        Args:
            accounts_to_process: List of {"id": "act_xxx", "name": "Account Name"}
            start_date: YYYY-MM-DD
            end_date: YYYY-MM-DD
            template_name: Name of template Facebook to process
            selected_fields: List of fields to retrieve
            load_page_map: Whether to load page map for creative fields
            
        Returns:
            List of data rows
        """
        template_config = FacebookAdsBaseReporter.get_facebook_template_config_by_name(template_name)
        
        logger.info(f"Bắt đầu lấy Daily Report từ {start_date} đến {end_date}")
        self._report_progress("Bắt đầu lấy Daily Report...", 5)
        
        # Load page map if needed
        if "page_name" in selected_fields:
            logger.info("Loading page map...")
            self.page_map = self.get_accessible_page_map()
        
        # Prepare date chunks
        date_chunks = self._generate_monthly_date_chunks(start_date, end_date)
        logger.info(f"Chia thành {len(date_chunks)} date chunks")
        
        # Prepare initial requests
        level = template_config["api_params"]["level"]
        all_initial_requests = self._prepare_initial_requests(
            accounts_to_process,
            date_chunks,
            level,
            template_config,
            selected_fields
        )
        
        logger.info(f"✓ Đã chuẩn bị {len(all_initial_requests)} requests ban đầu.")
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
        
        # TODO: Retry failed requests
        if all_failed_requests:
            logger.info(f"\n⚠ Có {len(all_failed_requests)} requests thất bại. Bắt đầu retry...")
            
            # Define callback để ghi incremental
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
    
    reporter = FacebookDailyReporter(
        access_token=ACCESS_TOKEN,
        email="test@example.com"
    )
    
    # Template config example
    template_name = "AGE & GENDER_DETAILED_REPORT"
    accounts = [
        {"id": "act_948290596967304", "name": "25. Cara Luna 02 - "}
    ]
    
    data = reporter.get_report(
        accounts_to_process=accounts,
        start_date="2025-12-31",
        end_date="2025-12-31",
        template_name=template_name,
        selected_fields=["date_start", "date_stop", "account_id", "account_name", "campaign_name", "adset_name", "ad_name", "id", "adset_bid_strategy", "adset_bid_amount", 
"age", "gender", "creative_id", "creative_name", "creative_thumbnail_url", "creative_link", "spend", "impressions", "reach", "clicks", 
"cpc", "cpm", "ctr", "frequency", "inline_link_clicks", "unique_inline_link_clicks", "outbound_clicks", "unique_outbound_clicks", "inline_link_click_ctr", "outbound_click_ctr", 
"Messaging conversations started", "New messaging contacts", "Cost per messaging conversation started", "Post engagements", "Post reactions", "Post comments", "Post saves", "Post shares", "Photo views", "Landing page views", 
"Cost per landing page view", "ThruPlay", "Chi phí / ThruPlay", "Video Views (25%)", "Video Views (50%)", "Video Views (75%)", "Video Views (95%)", "Video Views (100%)"
]
    )
    
    print(f"Got {len(data)} rows")
    
    total_spend = 0
    for val in data:
        total_spend += int(val.get("spend"))
    print("Total Spend: ", total_spend)
    
    write_to_file(f"data/{template_name}.json", data)
