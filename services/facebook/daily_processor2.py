"""
Enhanced Facebook Daily Reporter
Two-Phase Fetching Strategy:
1. Fetch insights data (with filtering)
2. Fetch object metadata (ads/campaigns/etc)
3. Join by ID
"""

from typing import List, Dict, Any, Optional
from services.facebook.base_processor import FacebookAdsBaseReporter
import logging
import json
from services.facebook.constant import CONVERSION_METRICS_MAP, EFFECTIVE_STATUS_FILTERS

logger = logging.getLogger(__name__)


class FacebookDailyReporterV2(FacebookAdsBaseReporter):
    """
    Enhanced reporter với two-phase fetching:
    - Phase 1: /insights endpoint (filterable)
    - Phase 2: /ads endpoint (metadata)
    - Join by ad_id
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.page_map = {}
    
    # ==================== PHASE 1: INSIGHTS ====================
    
    def _create_insights_url(
        self,
        account: Dict[str, str],
        chunk: Dict[str, str],
        template_config: Dict[str, Any],
        selected_fields: List[str]
    ) -> str:
        """
        Create URL for /insights endpoint.
        This allows filtering by spend!
        
        Example:
        /insights?level=ad&time_range={...}&time_increment=1
                 &breakdowns=age,gender
                 &filtering=[{'field':'spend','operator':'GREATER_THAN','value':'0'}]
                 &fields=ad_id,spend,impressions,...
        """
        level = template_config["api_params"]["level"]
        breakdowns = template_config["api_params"].get("breakdowns")
        time_increment = template_config["api_params"].get("time_increment", 1)
        
        # Build insight fields
        insight_fields = set(["account_id", "date_start", "date_stop"])
        
        # Add level_id field
        insight_fields.add(f"{level}_id")
        
        # Add selected insight fields
        for field in selected_fields:
            if field in CONVERSION_METRICS_MAP:
                parent_field = CONVERSION_METRICS_MAP[field].get("parent_field")
                api_field = CONVERSION_METRICS_MAP[field].get("api_field")
                
                if parent_field:
                    if parent_field.startswith("actions:"):
                        insight_fields.add("actions")
                    elif parent_field.startswith("action_values:"):
                        insight_fields.add("action_values")
                    else:
                        insight_fields.add(parent_field)
                elif api_field:
                    if api_field.startswith("actions:"):
                        insight_fields.add("actions")
                    else:
                        insight_fields.add(api_field)
            elif field in template_config.get("insight_fields", []):
                insight_fields.add(field)
        
        # Build params
        params = {
            "level": level,
            "time_range": json.dumps({"since": chunk["start"], "until": chunk["end"]}),
            "time_increment": time_increment,
            "fields": ",".join(insight_fields),
            # "filtering": json.dumps([{
            #     "field": "spend",
            #     "operator": "GREATER_THAN",
            #     "value": "0"
            # }]),
            "limit": 500
        }
        
        # Add breakdowns
        if breakdowns:
            if isinstance(breakdowns, list):
                params["breakdowns"] = ",".join(breakdowns)
            else:
                params["breakdowns"] = breakdowns
        
        from urllib.parse import urlencode
        query_string = urlencode(params)
        
        url = f"{account['id']}/insights?{query_string}"
        logger.debug(f"Insights URL: {url}")
        return url
    
    # ==================== PHASE 2: METADATA ====================
    
    def _create_metadata_url(
        self,
        account: Dict[str, str],
        level: str,
        template_config: Dict[str, Any],
        selected_fields: List[str]
    ) -> str:
        """
        Create URL for /ads (or /campaigns, etc) endpoint.
        This gets object metadata (name, status, creative, etc).
        
        Example:
        /ads?fields=id,name,status,effective_status,
                    adset{id,name,bid_strategy,...},
                    campaign{id,name},
                    creative{...}
             &effective_status=[...]
             &limit=500
        """
        object_fields_key = f"{level}_fields"
        api_object_fields = template_config.get(object_fields_key, [])
        
        # Build fields
        final_fields = set(["id", "name"])
        
        # Add ALL template object fields
        for field in api_object_fields:
            final_fields.add(field)
        
        # Build params
        params = {
            "fields": ",".join(final_fields),
            "limit": 500
        }
        
        # Add effective_status filter
        status_filter = EFFECTIVE_STATUS_FILTERS.get(level)
        if status_filter:
            params["effective_status"] = json.dumps(status_filter)
        
        from urllib.parse import urlencode
        query_string = urlencode(params, safe='{}(),')
        
        url = f"{account['id']}/{level}s?{query_string}"
        logger.debug(f"Metadata URL: {url}")
        return url
    
    # ==================== REQUEST PREPARATION ====================
    
    def _prepare_insights_requests(
        self,
        accounts_to_process: List[Dict[str, str]],
        date_chunks: List[Dict[str, str]],
        template_config: Dict[str, Any],
        selected_fields: List[str]
    ) -> List[Dict[str, Any]]:
        """Prepare insights requests (Phase 1)"""
        requests = []
        
        for account in accounts_to_process:
            for chunk in date_chunks:
                url = self._create_insights_url(
                    account, chunk, template_config, selected_fields
                )
                
                requests.append({
                    "url": url,
                    "metadata": {
                        "account": account,
                        "level": template_config["api_params"]["level"],
                        "phase": "insights",
                        "chunk": chunk
                    }
                })
        
        return requests
    
    def _prepare_metadata_requests(
        self,
        accounts_to_process: List[Dict[str, str]],
        template_config: Dict[str, Any],
        selected_fields: List[str]
    ) -> List[Dict[str, Any]]:
        """Prepare metadata requests (Phase 2)"""
        requests = []
        level = template_config["api_params"]["level"]
        
        for account in accounts_to_process:
            url = self._create_metadata_url(
                account, level, template_config, selected_fields
            )
            
            requests.append({
                "url": url,
                "metadata": {
                    "account": account,
                    "level": level,
                    "phase": "metadata"
                }
            })
        
        return requests
    
    # ==================== PAGINATION HELPERS ====================
    
    @staticmethod
    def _extract_next_url_from_cursors(
        response_body: Dict[str, Any],
        original_url: str
    ) -> Optional[str]:
        """
        Extract next URL from cursor-based pagination.
        
        Facebook API returns cursors in two formats:
        1. Direct next URL: {"paging": {"next": "full_url"}}
        2. Cursor only: {"paging": {"cursors": {"after": "..."}}}
        
        Args:
            response_body: Response data
            original_url: Original request URL (to build next URL if needed)
            
        Returns:
            Next URL or None
        """
        paging = response_body.get("paging", {})
        
        # Method 1: Direct next URL (most common)
        if paging.get("next"):
            return paging["next"]
        
        # Method 2: Cursor-based (need to build URL)
        cursors = paging.get("cursors", {})
        after_cursor = cursors.get("after")
        
        if not after_cursor:
            return None
        
        # Build next URL with after cursor
        from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
        
        try:
            parsed = urlparse(original_url)
            params = parse_qs(parsed.query, keep_blank_values=True)
            
            # Update/add after cursor
            params["after"] = [after_cursor]
            
            # Remove before cursor if exists
            params.pop("before", None)
            
            # Rebuild query string
            new_query = urlencode(params, doseq=True)
            
            # Rebuild URL
            new_parsed = parsed._replace(query=new_query)
            next_url = urlunparse(new_parsed)
            
            logger.debug(f"Built cursor URL with after={after_cursor[:10]}...")
            return next_url
            
        except Exception as e:
            logger.warning(f"Error building cursor URL: {e}")
            return None
    
    # ==================== RESPONSE PROCESSING ====================
    
    def _process_insights_response(
        self,
        response_body: Dict[str, Any],
        request_metadata: Dict[str, Any],
        selected_fields: List[str]
    ) -> List[Dict[str, Any]]:
        """Process insights response (Phase 1)"""
        extracted_rows = []
        level = request_metadata["level"]
        id_field = f"{level}_id"
        
        for item in response_body.get("data", []):
            # Flatten action metrics
            flattened = self._flatten_action_metrics(item, selected_fields)
            
            # Add account info
            flattened["account_id"] = request_metadata["account"]["id"]
            flattened["account_name"] = request_metadata["account"]["name"]
            
            # Ensure ID field exists
            if id_field not in flattened and "id" in flattened:
                flattened[id_field] = flattened["id"]
            
            extracted_rows.append(flattened)
        
        return extracted_rows
    
    def _process_metadata_response(
        self,
        response_body: Dict[str, Any],
        request_metadata: Dict[str, Any]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Process metadata response (Phase 2).
        
        Returns:
            Dict mapping {ad_id: metadata_dict}
        """
        metadata_map = {}
        level = request_metadata["level"]
        
        for item in response_body.get("data", []):
            object_id = item.get("id")
            if not object_id:
                continue
            
            metadata = {"id": object_id, "name": item.get("name")}
            
            # Extract nested fields
            if item.get("campaign"):
                metadata["campaign_id"] = item["campaign"].get("id")
                metadata["campaign_name"] = item["campaign"].get("name")
            
            if item.get("adset"):
                metadata["adset_id"] = item["adset"].get("id")
                metadata["adset_name"] = item["adset"].get("name")
                metadata["adset_bid_strategy"] = item["adset"].get("bid_strategy")
                
                # Get bid amount from multiple sources
                bid = (item["adset"].get("bid_amount") or 
                       item["adset"].get("daily_budget") or 
                       item["adset"].get("lifetime_budget"))
                if bid:
                    metadata["adset_bid_amount"] = bid
            
            if item.get("creative"):
                creative = item["creative"]
                metadata["creative_id"] = creative.get("id", "")
                metadata["creative_name"] = creative.get("name", "")
                metadata["creative_title"] = creative.get("title", "")
                metadata["creative_body"] = creative.get("body", "")
                
                # Actor ID & Page Name
                actor_id = str(creative.get("actor_id", ""))
                metadata["actor_id"] = actor_id
                metadata["page_name"] = self.page_map.get(actor_id, "Page không xác định")
                
                # Thumbnail
                thumbnail_url = creative.get("thumbnail_url", "")
                metadata["creative_thumbnail_url"] = f'=IMAGE("{thumbnail_url}")' if thumbnail_url else ""
                metadata["creative_thumbnail_raw_url"] = thumbnail_url
                
                # Link
                object_story_id = creative.get("object_story_id", "")
                metadata["creative_link"] = f"https://facebook.com/{object_story_id}" if object_story_id else ""
            
            # Add other fields from template
            for key, value in item.items():
                if key not in ["id", "name", "campaign", "adset", "creative"]:
                    metadata[key] = value
            
            metadata_map[object_id] = metadata
        
        return metadata_map
    
    # ==================== JOIN LOGIC ====================
    
    def _join_insights_with_metadata(
        self,
        insights_data: List[Dict[str, Any]],
        metadata_map: Dict[str, Dict[str, Any]],
        level: str
    ) -> List[Dict[str, Any]]:
        """
        Join insights data with metadata.
        
        Args:
            insights_data: List of insights rows
            metadata_map: Dict of {object_id: metadata}
            level: ad, adset, campaign, etc.
            
        Returns:
            Combined data
        """
        id_field = f"{level}_id"
        joined_data = []
        
        for insight_row in insights_data:
            object_id = insight_row.get(id_field)
            
            if not object_id:
                logger.warning(f"Missing {id_field} in insight row")
                joined_data.append(insight_row)
                continue
            
            # Get metadata
            metadata = metadata_map.get(object_id, {})
            
            # Join: metadata first, then insight (insight overrides)
            combined_row = {**metadata, **insight_row}
            
            # Rename id → {level}_id, name → {level}_name
            if "id" in combined_row and level != "account":
                combined_row[f"{level}_id"] = combined_row["id"]
                combined_row[f"{level}_name"] = combined_row.get("name", "")
            
            joined_data.append(combined_row)
        
        return joined_data
    
    # ==================== WAVE PROCESSING ====================
    
    def _process_wave_responses(
        self,
        all_responses: List[Dict[str, Any]],
        selected_fields: List[str]
    ) -> Dict[str, Any]:
        """Process wave responses with phase detection"""
        data_rows = []
        metadata_rows = []
        next_wave_requests = []
        failed_requests = []
        
        for response in all_responses:
            metadata = response["metadata"]
            phase = metadata.get("phase")
            
            # Handle errors
            if response["status_code"] != 200:
                error_data = response.get("error", {})
                logger.warning(f"Request failed: {error_data.get('message')}")
                
                if 500 <= response["status_code"] < 600:
                    failed_requests.append({
                        "url": response["original_url"],
                        "metadata": metadata
                    })
                continue
            
            response_body = response.get("data")
            if not response_body:
                continue
            
            # Process based on phase
            if phase == "insights":
                rows = self._process_insights_response(
                    response_body, metadata, selected_fields
                )
                data_rows.extend(rows)
                
                # Handle cursor-based pagination
                next_url = self._extract_next_url_from_cursors(
                    response_body,
                    response.get("original_url", "")
                )
                
                if next_url:
                    logger.debug(f"Insights pagination: {len(rows)} rows, has next")
                    next_wave_requests.append({
                        "url": self._get_relative_url(next_url),
                        "metadata": metadata
                    })
            
            elif phase == "metadata":
                metadata_map = self._process_metadata_response(
                    response_body, metadata
                )
                metadata_rows.append(metadata_map)
                
                # Handle cursor-based pagination
                next_url = self._extract_next_url_from_cursors(
                    response_body,
                    response.get("original_url", "")
                )
                
                if next_url:
                    logger.debug(f"Metadata pagination: {len(metadata_map)} objects, has next")
                    next_wave_requests.append({
                        "url": self._get_relative_url(next_url),
                        "metadata": metadata
                    })
        
        return {
            "data_rows": data_rows,
            "metadata_rows": metadata_rows,
            "next_wave_requests": next_wave_requests,
            "failed_requests": failed_requests
        }
    
    # ==================== RETRY LOGIC ====================
    
    def _retry_failed_requests(
        self,
        failed_requests: List[Dict[str, Any]],
        selected_fields: List[str],
        phase: str
    ) -> Dict[str, Any]:
        """
        Retry failed requests với batch processing.
        
        Args:
            failed_requests: List of failed request dicts
            selected_fields: Selected fields
            phase: "insights" or "metadata"
            
        Returns:
            {"data_rows": [...], "metadata_rows": [...]}
        """
        if not failed_requests:
            return {"data_rows": [], "metadata_rows": []}
        
        logger.info(f"\n===== RETRY {phase.upper()}: {len(failed_requests)} requests =====")
        import time
        time.sleep(3)
        
        BATCH_SIZE = 10
        MAX_RETRIES = 3
        
        all_data_rows = []
        all_metadata_rows = []
        
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
            
            logger.info(f"➤ Retry batch: {len(current_batch)} items, {len(queue)} remaining")
            
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
                
                # Process results
                for index, result in enumerate(response_json["results"]):
                    queue_item = current_batch[index]
                    
                    if result["status_code"] == 200 and result.get("data"):
                        # Attach metadata
                        result["metadata"] = queue_item["metadata"]
                        result["original_url"] = queue_item["url"]
                        
                        # Process based on phase
                        wave_result = self._process_wave_responses([result], selected_fields)
                        
                        all_data_rows.extend(wave_result.get("data_rows", []))
                        all_metadata_rows.extend(wave_result.get("metadata_rows", []))
                        
                        # Handle pagination
                        for next_req in wave_result.get("next_wave_requests", []):
                            queue.append({
                                "url": next_req["url"],
                                "metadata": next_req["metadata"],
                                "retry_count": 0
                            })
                    else:
                        # Retry
                        if queue_item["retry_count"] < MAX_RETRIES:
                            queue_item["retry_count"] += 1
                            queue.append(queue_item)
                
            except Exception as e:
                logger.error(f"Batch error: {e}")
                for item in current_batch:
                    if item["retry_count"] < MAX_RETRIES:
                        item["retry_count"] += 1
                        queue.append(item)
            
            if queue:
                time.sleep(2)
        
        logger.info(f"✓ Retry complete: {len(all_data_rows)} data rows, {len(all_metadata_rows)} metadata objects")
        
        return {
            "data_rows": all_data_rows,
            "metadata_rows": all_metadata_rows
        }
    
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
        Main function với two-phase fetching và retry logic.
        """
        template_config = FacebookAdsBaseReporter.get_facebook_template_config_by_name(template_name)
        level = template_config["api_params"]["level"]
        
        logger.info(f"Starting two-phase daily report: {start_date} → {end_date}")
        self._report_progress("Bắt đầu lấy dữ liệu...", 5)
        
        # Load page map if needed
        if "page_name" in selected_fields:
            self.page_map = self.get_accessible_page_map()
        
        # Prepare date chunks
        if (template_name == "LOCATION_DETAILED_REPORT" or template_name == "AGE & GENDER_DETAILED_REPORT"):
            date_chunks = self._generate_monthly_date_chunks(start_date, end_date, factor = 6)
        else:
            date_chunks = self._generate_monthly_date_chunks(start_date, end_date, factor = 2)
        
        # ===== PHASE 1: FETCH INSIGHTS =====
        logger.info("\n===== PHASE 1: FETCHING INSIGHTS =====")
        self._report_progress("Đang lấy insights data...", 20)
        
        insights_requests = self._prepare_insights_requests(
            accounts_to_process, date_chunks, template_config, selected_fields
        )
        
        all_insights_data = []
        all_insights_failed = []
        requests_for_wave = insights_requests
        wave_count = 1
        
        while requests_for_wave:
            logger.info(f"Processing insights wave {wave_count}...")
            
            responses = self._execute_wave(
                requests_for_wave,
                self.DEFAULT_BATCH_SIZE,
                self.DEFAULT_SLEEP_TIME,
                wave_count
            )
            
            wave_result = self._process_wave_responses(responses, selected_fields)
            all_insights_data.extend(wave_result["data_rows"])
            all_insights_failed.extend(wave_result["failed_requests"])
            requests_for_wave = wave_result["next_wave_requests"]
            wave_count += 1
        
        logger.info(f"✓ Phase 1 complete: {len(all_insights_data)} rows, {len(all_insights_failed)} failed")
        
        # Retry insights failures
        if all_insights_failed:
            logger.info(f"⚠ Retrying {len(all_insights_failed)} failed insights requests...")
            retry_result = self._retry_failed_requests(
                all_insights_failed,
                selected_fields,
                phase="insights"
            )
            all_insights_data.extend(retry_result["data_rows"])
            logger.info(f"✓ Insights retry added {len(retry_result['data_rows'])} rows")
        
        # ===== PHASE 2: FETCH METADATA =====
        logger.info("\n===== PHASE 2: FETCHING METADATA =====")
        self._report_progress("Đang lấy object metadata...", 60)
        
        metadata_requests = self._prepare_metadata_requests(
            accounts_to_process, template_config, selected_fields
        )
        
        all_metadata_maps = []
        all_metadata_failed = []
        requests_for_wave = metadata_requests
        wave_count = 1
        
        while requests_for_wave:
            logger.info(f"Processing metadata wave {wave_count}...")
            
            responses = self._execute_wave(
                requests_for_wave,
                self.DEFAULT_BATCH_SIZE,
                self.DEFAULT_SLEEP_TIME,
                wave_count
            )
            
            wave_result = self._process_wave_responses(responses, selected_fields)
            all_metadata_maps.extend(wave_result["metadata_rows"])
            all_metadata_failed.extend(wave_result["failed_requests"])
            requests_for_wave = wave_result["next_wave_requests"]
            wave_count += 1
        
        # Retry metadata failures
        if all_metadata_failed:
            logger.info(f"⚠ Retrying {len(all_metadata_failed)} failed metadata requests...")
            retry_result = self._retry_failed_requests(
                all_metadata_failed,
                selected_fields,
                phase="metadata"
            )
            all_metadata_maps.extend(retry_result["metadata_rows"])
            logger.info(f"✓ Metadata retry added {len(retry_result['metadata_rows'])} objects")
        
        # Merge all metadata maps
        combined_metadata = {}
        for metadata_map in all_metadata_maps:
            combined_metadata.update(metadata_map)
        
        logger.info(f"✓ Phase 2 complete: {len(combined_metadata)} objects")
        
        # ===== PHASE 3: JOIN =====
        logger.info("\n===== PHASE 3: JOINING DATA =====")
        self._report_progress("Đang join data...", 90)
        
        final_data = self._join_insights_with_metadata(
            all_insights_data,
            combined_metadata,
            level
        )
        
        logger.info(f"✓ Complete: {len(final_data)} final rows")
        self._report_progress("Hoàn thành!", 100)
        
        return final_data

if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    from .helper import write_to_file    
    load_dotenv()
    
    ACCESS_TOKEN = os.getenv("FACEBOOK_ACCESS_TOKEN")
    
    reporter = FacebookDailyReporterV2(
        access_token=ACCESS_TOKEN,
        email="test@example.com"
    )
    
    # Template config example
    template_name = "LOCATION_DETAILED_REPORT"
    accounts = [
        {"id": "act_650248897235348", "name": "Cara Luna 02"}
    ]
    
    data = reporter.get_report(
        accounts_to_process=accounts,
        start_date="2025-12-01",
        end_date="2025-12-31",
        template_name=template_name,
        selected_fields=["date_start", "date_stop", "account_id", "account_name", "campaign_name", "adset_name", "ad", "id", "adset_bid_strategy", "adset_bid_amount", 
"country", "region", "creative_id", "creative_name", "creative_thumbnail_url", "spend", "impressions", "reach", "clicks", "cpc", 
"cpm", "ctr", "frequency", "inline_link_clicks", "outbound_clicks", "Messaging conversations started", "New messaging contacts", "Cost per messaging conversation started", "Post engagements", "Post reactions", 
"Post comments", "Post saves", "Post shares", "Landing page views", "Cost per landing page view", "Video Plays", "ThruPlays", "Photo views"]
    )
    
    print(f"Got {len(data)} rows")
    
    total_spend = 0
    for val in data:
        total_spend += int(val.get("spend"))
    print("Total Spend: ", total_spend)
    
    write_to_file(f"data/{template_name}.json", data)