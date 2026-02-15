import gspread
from google.oauth2.service_account import Credentials
import os
import time
import logging
from datetime import datetime
from typing import List, Dict, Any, Callable

logger = logging.getLogger(__name__)


class GoogleSheetWriter:
    """
    Writer với retry mechanism cho tất cả operations.
    Xử lý APIError 502, 429, timeout, etc.
    """
    
    # Retry configuration
    MAX_RETRIES = 5
    BASE_BACKOFF = 2  # seconds
    MAX_BACKOFF = 60  # seconds
    
    def __init__(self, credentials_path: str, spreadsheet_id: str):
        """
        Khởi tạo writer và xác thực với Google.
        """
        if not os.path.exists(credentials_path):
            raise FileNotFoundError(f"Không tìm thấy file credentials tại: {credentials_path}")
            
        logger.info("Đang xác thực với Google Sheets API...")
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_file(credentials_path, scopes=scopes)
        client = gspread.authorize(creds)
        
        try:
            self.spreadsheet = client.open_by_key(spreadsheet_id)
            logger.info(f"Đã mở thành công spreadsheet: '{self.spreadsheet.title}'")
        except Exception as e:
            raise Exception(f"Lỗi khi mở spreadsheet {spreadsheet_id}: {str(e)}") from e

    def _retry_operation(
        self, 
        operation: Callable, 
        operation_name: str,
        *args, 
        **kwargs
    ) -> Any:
        """
        Wrapper để retry bất kỳ operation nào với exponential backoff.
        
        Args:
            operation: Function to execute
            operation_name: Name for logging
            *args, **kwargs: Arguments to pass to operation
            
        Returns:
            Result from operation
            
        Raises:
            Exception: After max retries exceeded
        """
        last_exception = None
        
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                result = operation(*args, **kwargs)
                
                if attempt > 1:
                    logger.info(f"✓ {operation_name} succeeded on attempt {attempt}")
                
                return result
                
            except gspread.exceptions.APIError as e:
                last_exception = e
                error_msg = str(e)
                
                # Check error type
                is_retryable = (
                    "502" in error_msg or  # Server Error
                    "503" in error_msg or  # Service Unavailable
                    "429" in error_msg or  # Too Many Requests
                    "500" in error_msg or  # Internal Server Error
                    "RESOURCE_EXHAUSTED" in error_msg or
                    "DEADLINE_EXCEEDED" in error_msg
                )
                
                if not is_retryable:
                    logger.error(f"✗ {operation_name} failed with non-retryable error: {error_msg}")
                    raise
                
                if attempt >= self.MAX_RETRIES:
                    logger.error(f"✗ {operation_name} failed after {self.MAX_RETRIES} attempts")
                    raise
                
                # Calculate backoff time
                backoff = min(
                    self.BASE_BACKOFF * (2 ** (attempt - 1)),
                    self.MAX_BACKOFF
                )
                
                logger.warning(
                    f"⚠ {operation_name} failed (attempt {attempt}/{self.MAX_RETRIES}): {error_msg[:100]}..."
                )
                logger.info(f"  Waiting {backoff}s before retry...")
                
                time.sleep(backoff)
                
            except Exception as e:
                last_exception = e
                logger.error(f"✗ {operation_name} failed with unexpected error: {e}")
                
                if attempt >= self.MAX_RETRIES:
                    raise
                
                # Still retry for unexpected errors
                backoff = min(self.BASE_BACKOFF * (2 ** (attempt - 1)), self.MAX_BACKOFF)
                logger.info(f"  Waiting {backoff}s before retry...")
                time.sleep(backoff)
        
        raise last_exception

    def _get_or_create_worksheet(self, sheet_name: str) -> gspread.Worksheet:
        """Lấy worksheet với retry"""
        def _get():
            try:
                return self.spreadsheet.worksheet(sheet_name)
            except gspread.WorksheetNotFound:
                logger.info(f"Sheet '{sheet_name}' không tồn tại. Đang tạo mới...")
                return self.spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=50)
        
        return self._retry_operation(
            _get,
            f"Get/Create worksheet '{sheet_name}'"
        )

    def _format_columns(self, worksheet: gspread.Worksheet, headers: list):
        """Định dạng các cột với retry"""
        text_columns = {
            "advertiser_id", "campaign_id", "store_id", "item_group_id", 
            "item_id", "tt_user_id", "video_id", "id", "adset_id",
            "account_id", "ad_id", "creative_id"
        }
        number_columns = {
            "New Messaging Connections", "Cost Purchases", "Website Purchases", 
            "On-Facebook Purchases", "Leads", "Purchases", "Cost Leads", 
            "Cost per New Messaging", "Purchase Value", "Purchase ROAS", 
            "frequency", "ctr", "spend", "cpc", "cpm", "cost_per_conversion",
            "total_onsite_shopping_value", "cost", "cost_per_order", 
            "gross_revenue", "net_cost", "roas_bid", "target_roi_budget",
            "max_delivery_budget", "daily_budget", "budget_remaining",
            "lifetime_budget", "roi"
        }
        integer_columns = {
            "reach", "impressions", "clicks", "conversion", 
            "video_play_actions", "orders", "product_impressions", 
            "product_clicks"
        }

        requests = []
        for i, header in enumerate(headers):
            format_pattern = None
            
            if header in text_columns:
                format_pattern = {"type": "TEXT", "pattern": "@"}
            elif header in number_columns:
                format_pattern = {"type": "NUMBER", "pattern": "#,##0.00"}
            elif header in integer_columns:
                format_pattern = {"type": "NUMBER", "pattern": "#,##0"}

            if format_pattern:
                requests.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": worksheet.id,
                            "startColumnIndex": i,
                            "endColumnIndex": i + 1,
                            "startRowIndex": 1
                        },
                        "cell": {"userEnteredFormat": {"numberFormat": format_pattern}},
                        "fields": "userEnteredFormat.numberFormat"
                    }
                })
        
        if requests:
            def _format():
                return self.spreadsheet.batch_update({"requests": requests})
            
            self._retry_operation(
                _format,
                f"Format {len(requests)} columns"
            )
            logger.info(f"Đã áp dụng định dạng cho {len(requests)} cột.")

    def write_data(self, data_to_write: list, headers: list, options: dict) -> int:
        """
        Hàm chính để ghi dữ liệu vào sheet với retry.
        """
        sheet_name = options.get('sheetName')
        is_overwrite = options.get('isOverwrite', False)
        is_first_chunk = options.get('isFirstChunk', False)
        
        if not sheet_name:
            raise ValueError("Thiếu 'sheetName' trong options.")

        # Filter rows with spend
        if 'spend' in headers:
            original_count = len(data_to_write)
            data_to_write = [row for row in data_to_write if 'spend' in row]
            logger.info(f"Filtered data: {original_count} → {len(data_to_write)} rows (có spend)")

        worksheet = self._get_or_create_worksheet(sheet_name)
        
        # Ensure enough columns
        if len(headers) > worksheet.col_count:
            cols_to_add = len(headers) - worksheet.col_count
            logger.info(f"Thêm {cols_to_add} cột mới...")
            
            def _add_cols():
                return worksheet.add_cols(cols_to_add)
            
            self._retry_operation(_add_cols, f"Add {cols_to_add} columns")

        is_sheet_empty = worksheet.row_count == 0 or not worksheet.get('A1')

        # ---- OVERWRITE MODE ----
        if is_overwrite:
            logger.info(f"Chế độ Ghi đè. Xóa và ghi lại sheet '{sheet_name}'...")
            
            def _clear():
                return worksheet.clear()
            
            self._retry_operation(_clear, "Clear worksheet")
            
            if not data_to_write and not headers:
                return 0

            # Convert to rows
            rows_data = [
                [
                    self._create_image_formula(row.get(h, '')) if h == 'product_img' 
                    else row.get(h, '') 
                    for h in headers
                ]
                for row in data_to_write
            ]
            rows = [list(headers)] + rows_data
            
            # Write data with retry
            def _update():
                return worksheet.update(
                    range_name='A1', 
                    values=rows, 
                    value_input_option='USER_ENTERED'
                )
            
            self._retry_operation(_update, f"Write {len(rows)} rows (overwrite)")
            
            # Format header
            def _format_header():
                return worksheet.format(
                    "1:1", 
                    {'textFormat': {'bold': True}, 'horizontalAlignment': 'CENTER'}
                )
            
            self._retry_operation(_format_header, "Format header row")
            
            self._format_columns(worksheet, headers)
            return len(data_to_write)

        # ---- APPEND MODE ----
        logger.info(f"Chế độ Ghi tiếp vào sheet '{sheet_name}'...")
        
        if not data_to_write:
            return 0
        
        # Get existing headers with retry
        def _get_headers():
            return worksheet.row_values(1)
        
        existing_headers = self._retry_operation(_get_headers, "Get existing headers")
        
        new_headers_to_add = [h for h in headers if h not in existing_headers]
        final_headers = existing_headers + new_headers_to_add

        # Check columns again after merging headers
        if len(final_headers) > worksheet.col_count:
            cols_to_add = len(final_headers) - worksheet.col_count
            logger.info(f"Append mode: Thêm {cols_to_add} cột mới...")
            
            def _add_cols():
                return worksheet.add_cols(cols_to_add)
            
            self._retry_operation(_add_cols, f"Add {cols_to_add} columns (append)")

        # Add new headers if needed
        if new_headers_to_add:
            logger.info(f"Thêm headers mới: {new_headers_to_add}")
            start_col = len(existing_headers) + 1
            
            def _update_headers():
                return worksheet.update(
                    range_name=gspread.utils.rowcol_to_a1(1, start_col),
                    values=[new_headers_to_add],
                    value_input_option='USER_ENTERED'
                )
            
            self._retry_operation(_update_headers, "Update new headers")
            
            def _format_header():
                return worksheet.format(
                    "1:1",
                    {'textFormat': {'bold': True}, 'horizontalAlignment': 'CENTER'}
                )
            
            self._retry_operation(_format_header, "Format header row")
        
        # Convert to rows
        rows_to_append = [
            [
                self._create_image_formula(row.get(h, '')) if h == 'product_img'
                else row.get(h, '')
                for h in final_headers
            ]
            for row in data_to_write
        ]
        
        # Append rows with retry
        def _append():
            return worksheet.append_rows(
                rows_to_append,
                value_input_option='USER_ENTERED'
            )
        
        self._retry_operation(_append, f"Append {len(rows_to_append)} rows")
        
        self._format_columns(worksheet, final_headers)
        return len(rows_to_append)
    
    def log_progress(self, task_id: str, status: str, message: str, progress: int):
        """Ghi log tiến trình với retry"""
        try:
            worksheet = self._get_or_create_worksheet('CURRENT_TASK_STATUS')

            # Hide sheet if not hidden
            if not worksheet._properties.get('hidden', False):
                def _hide():
                    body = {
                        "requests": [{
                            "updateSheetProperties": {
                                "properties": {
                                    "sheetId": worksheet.id,
                                    "hidden": True
                                },
                                "fields": "hidden"
                            }
                        }]
                    }
                    return self.spreadsheet.batch_update(body)
                
                self._retry_operation(_hide, "Hide status sheet")
                logger.info(f"Sheet CURRENT_TASK_STATUS đã được ẩn.")
            
            # Write progress data
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            data = [
                ['task_id', 'status', 'progress', 'message', 'last_updated'],
                [task_id, status, progress, message, timestamp]
            ]
            
            def _update_progress():
                return worksheet.update(range_name='A1', values=data)
            
            self._retry_operation(_update_progress, f"Log progress for {task_id}")
            logger.info(f"Logged progress: {task_id} - {message}")

        except Exception as e:
            logger.error(f"ERROR: Không thể ghi log cho task {task_id}: {e}")
            # Don't raise - logging failure shouldn't stop the job

    def update_task_history(self, task_id, status, message):
        try:
            worksheet = self._get_or_create_worksheet('hidden_task_history_log')

            # Get column A (taskId) to find the row
            def _get_task_ids():
                return worksheet.col_values(1)
            
            task_ids = self._retry_operation(_get_task_ids, "Get task IDs")
            
            # Find row index (1-based)
            row_index = -1
            str_task_id = str(task_id)
            
            for idx, val in enumerate(task_ids):
                if str(val) == str_task_id:
                    row_index = idx + 1
                    break
            
            if row_index != -1:
                # Update status (Column E) and message (Column F)
                # Headers: taskId(A), timestamp(B), description(C), runType(D), status(E), message(F)
                
                def _update_status():
                    return worksheet.update(
                        range_name=f'E{row_index}:F{row_index}',
                        values=[[status, message]],
                        value_input_option='USER_ENTERED'
                    )
                
                self._retry_operation(_update_status, f"Update history for {task_id}")
                logger.info(f"Updated history for task {task_id}: {status} - {message}")
            else:
                logger.warning(f"Task ID {task_id} not found in hidden_task_history_log")

        except Exception as e:
            logger.error(f"ERROR: Không thể cập nhật history cho task {task_id}: {e}")

            
    def _create_image_formula(self, url: str) -> str:
        """Chuyển URL thành =IMAGE() formula"""
        if url and isinstance(url, str) and url.startswith(('http://', 'https://')):
            return f'=IMAGE("{url}")'
        return ""

    def read_sheet_data(self, sheet_name: str) -> List[Dict[str, Any]]:
        """
        Đọc toàn bộ dữ liệu từ sheet.
        
        Args:
            sheet_name: Tên sheet cần đọc
        
        Returns:
            List[Dict]: Danh sách các dòng dữ liệu (dạng dict)
        """
        try:
            worksheet = self._get_or_create_worksheet(sheet_name)
            
            def _get_all_records():
                return worksheet.get_all_records()
            
            data = self._retry_operation(_get_all_records, f"Read all records from '{sheet_name}'")
            logger.info(f"Đã đọc {len(data)} dòng từ sheet '{sheet_name}'")
            return data
        except Exception as e:
            logger.error(f"Lỗi khi đọc sheet '{sheet_name}': {e}")
            return []