# # services/sheet_writer.py
# import gspread
# from google.oauth2.service_account import Credentials
# import os
# from datetime import datetime

# class GoogleSheetWriter:
#     """
#     Một lớp để kết nối và ghi dữ liệu vào Google Sheets bằng Service Account.
#     Tái tạo lại logic từ các hàm _writeDataToSheet và _formatSheetColumns của Apps Script.
#     """
#     def __init__(self, credentials_path: str, spreadsheet_id: str):
#         """
#         Khởi tạo writer và xác thực với Google.
#         Args:
#             credentials_path (str): Đường dẫn đến file credentials.json của Service Account.
#             spreadsheet_id (str): ID của file Google Sheet cần ghi vào.
#         """
#         if not os.path.exists(credentials_path):
#             raise FileNotFoundError(f"Không tìm thấy file credentials tại: {credentials_path}")
            
#         print("Đang xác thực với Google Sheets API...")
#         scopes = ["https://www.googleapis.com/auth/spreadsheets"]
#         creds = Credentials.from_service_account_file(credentials_path, scopes=scopes)
#         client = gspread.authorize(creds)
        
#         self.spreadsheet = client.open_by_key(spreadsheet_id)
#         print(f"Đã mở thành công spreadsheet: '{self.spreadsheet.title}'")

#     def _get_or_create_worksheet(self, sheet_name: str) -> gspread.Worksheet:
#         """Lấy một worksheet theo tên, hoặc tạo mới nếu chưa tồn tại."""
#         try:
#             return self.spreadsheet.worksheet(sheet_name)
#         except gspread.WorksheetNotFound:
#             print(f"Không tìm thấy sheet '{sheet_name}'. Đang tạo sheet mới...")
#             return self.spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=50)

#     def _format_columns(self, worksheet: gspread.Worksheet, headers: list):
#         """Định dạng các cột dựa trên tên header."""
#         # Định nghĩa các loại cột
#         text_columns = {"advertiser_id", "campaign_id", "store_id", "item_group_id", "item_id", "tt_user_id", "video_id", "id", "adset_id"}
#         number_columns = {"New Messaging Connections", "Cost Purchases", "Website Purchases", "On-Facebook Purchases", "Leads", "Purchases", "Cost Leads", "Cost per New Messaging", "Purchase Value", "Purchase ROAS", "frequency", "ctr","spend", "cpc", "cpm", "cost_per_conversion", "total_onsite_shopping_value", "cost", "cost_per_order", "gross_revenue", "net_cost", "roas_bid", "target_roi_budget", "max_delivery_budget","daily_budget","budget_remaining","lifetime_budget", "roi"}
#         integer_columns = {"reach", "impressions", "clicks", "conversion", "video_play_actions", "orders", "product_impressions", "product_clicks"}
#         # TIKTOK_PERCENT_METRICS cần được định nghĩa ở đâu đó nếu bạn dùng
#         # TIKTOK_PERCENT_METRICS = {"ad_conversion_rate", ...}

#         requests = []
#         for i, header in enumerate(headers):
#             col_letter = gspread.utils.rowcol_to_a1(1, i + 1).rstrip('1')
#             range_to_format = f"{col_letter}2:{col_letter}" # Định dạng từ hàng thứ 2 trở đi
            
#             format_pattern = None
#             if header in text_columns:
#                 format_pattern = {"type": "TEXT", "pattern": "@"}
#             elif header in number_columns:
#                 format_pattern = {"type": "NUMBER", "pattern": "#,##0.00"}
#             # elif header in TIKTOK_PERCENT_METRICS:
#             #     format_pattern = {"type": "NUMBER", "pattern": "0.00%"}
#             elif header in integer_columns:
#                 format_pattern = {"type": "NUMBER", "pattern": "#,##0"}

#             if format_pattern:
#                 requests.append({
#                     "repeatCell": {
#                         "range": {
#                             "sheetId": worksheet.id,
#                             "startColumnIndex": i,
#                             "endColumnIndex": i + 1,
#                             "startRowIndex": 1 # Bắt đầu từ hàng 2
#                         },
#                         "cell": {"userEnteredFormat": {"numberFormat": format_pattern}},
#                         "fields": "userEnteredFormat.numberFormat"
#                     }
#                 })
        
#         if requests:
#             self.spreadsheet.batch_update({"requests": requests})
#             print(f"Đã áp dụng định dạng cho {len(requests)} cột.")

#     def write_data(self, data_to_write: list, headers: list, options: dict) -> int:
#         """
#         Hàm chính để ghi dữ liệu vào sheet, xử lý cả overwrite và append.
#         Tự động mở rộng cột nếu dữ liệu vượt quá giới hạn lưới (Grid limits).
#         """
#         sheet_name = options.get('sheetName')
#         is_overwrite = options.get('isOverwrite', False)
#         is_first_chunk = options.get('isFirstChunk', False)
        
#         if not sheet_name:
#             raise ValueError("Thiếu 'sheetName' trong options.")

#         # Lọc bỏ các dòng không có thuộc tính 'spend' (nếu header có 'spend')
#         if 'spend' in headers:
#             original_count = len(data_to_write)
#             data_to_write = [row for row in data_to_write if 'spend' in row]
#             print(f"Dữ liệu ban đầu: {original_count} dòng. Sau khi lọc 'spend': {len(data_to_write)} dòng.")

#         worksheet = self._get_or_create_worksheet(sheet_name)
        
#         # --- LOGIC MỚI: Đảm bảo đủ số cột cho headers ngay từ đầu ---
#         # Kiểm tra nếu số lượng headers cần ghi lớn hơn số cột hiện có của sheet
#         if len(headers) > worksheet.col_count:
#             cols_to_add = len(headers) - worksheet.col_count
#             print(f"Sheet đang thiếu {cols_to_add} cột. Đang thêm cột mới...")
#             worksheet.add_cols(cols_to_add)
#         # ------------------------------------------------------------

#         is_sheet_empty = worksheet.row_count == 0 or (worksheet.get('A1') is None)

#         # ---- XỬ LÝ GHI ĐÈ (OVERWRITE) ----
#         if is_first_chunk and (is_overwrite or is_sheet_empty):
#             print(f"Chế độ Ghi đè. Đang xóa và ghi lại sheet '{sheet_name}'...")
#             worksheet.clear()
            
#             if not data_to_write and not headers:
#                 return 0

#             # Chuyển đổi list of dicts thành list of lists
#             rows_data = [
#                 [self._create_image_formula(row.get(h, '')) if h == 'product_img' else row.get(h, '') for h in headers]
#                 for row in data_to_write
#             ]
#             rows = [list(headers)] + rows_data
            
#             # Vì đã add_cols ở trên nên lệnh này sẽ không bị lỗi 400 Grid limits nữa
#             worksheet.update(range_name='A1', values=rows, value_input_option='USER_ENTERED')
            
#             # Định dạng header
#             worksheet.format("1:1", {'textFormat': {'bold': True}, 'horizontalAlignment': 'CENTER'})
            
#             self._format_columns(worksheet, headers)
#             return len(data_to_write)

#         # ---- XỬ LÝ GHI TIẾP (APPEND) ----
#         print(f"Chế độ Ghi tiếp vào sheet '{sheet_name}'...")
#         if not data_to_write:
#             return 0
            
#         existing_headers = worksheet.row_values(1)
#         new_headers_to_add = [h for h in headers if h not in existing_headers]
        
#         final_headers = existing_headers + new_headers_to_add

#         # --- LOGIC MỚI CHO APPEND: Kiểm tra lại tổng số cột sau khi gộp header cũ và mới ---
#         # Nếu có header mới, tổng số cột sẽ tăng lên, cần kiểm tra lại lần nữa
#         if len(final_headers) > worksheet.col_count:
#             cols_to_add = len(final_headers) - worksheet.col_count
#             print(f"Phát hiện thêm cột mới trong chế độ Append. Đang thêm {cols_to_add} cột...")
#             worksheet.add_cols(cols_to_add)
#         # -----------------------------------------------------------------------------------

#         if new_headers_to_add:
#             print(f"Phát hiện cột mới: {new_headers_to_add}. Đang thêm header vào sheet...")
#             start_col = len(existing_headers) + 1
#             # Cập nhật header mới vào các ô tiếp theo
#             worksheet.update(range_name=gspread.utils.rowcol_to_a1(1, start_col), values=[new_headers_to_add], value_input_option='USER_ENTERED')
#             worksheet.format(f"1:1", {'textFormat': {'bold': True}, 'horizontalAlignment': 'CENTER'})
        
#         # Chuyển đổi list of dicts thành list of lists theo đúng thứ tự của final_headers
#         rows_to_append = [
#             [self._create_image_formula(row.get(h, '')) if h == 'product_img' else row.get(h, '') for h in final_headers]
#             for row in data_to_write
#         ]
        
#         worksheet.append_rows(rows_to_append, value_input_option='USER_ENTERED')
        
#         self._format_columns(worksheet, final_headers)
#         return len(rows_to_append)
    
#     def log_progress(self, task_id: str, status: str, message: str, progress: int):
#         """
#         Ghi log tiến trình vào một sheet riêng biệt có tên là task_id.
#         """
#         try:
#             # Lấy hoặc tạo một sheet có tên là CURRENT_TASK_STATUS
#             worksheet = self._get_or_create_worksheet('CURRENT_TASK_STATUS')

#             if not worksheet._properties.get('hidden', False):
#                 body = {
#                     "requests": [
#                         {
#                             "updateSheetProperties": {
#                                 "properties": {
#                                     "sheetId": worksheet.id,
#                                     "hidden": True
#                                 },
#                                 "fields": "hidden"
#                             }
#                         }
#                     ]
#                 }
#                 self.spreadsheet.batch_update(body)
#                 print(f"Sheet log '{task_id}' đã được ẩn.")
            
#             # Dữ liệu cần ghi: status, progress, message, và timestamp
#             timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#             data = [
#                 ['task_id', 'status', 'progress', 'message', 'last_updated'],
#                 [task_id, status, progress, message, timestamp]
#             ]
            
#             # Ghi đè vào các ô đầu tiên của sheet
#             worksheet.update(range_name='A1', values=data)
#             print(f"Logged progress for task {task_id}: {message}")

#         except Exception as e:
#             print(f"ERROR: Không thể ghi log tiến trình cho task {task_id}: {e}")
            
#     def _create_image_formula(self, url: str) -> str:
#         """
#         Chuyển đổi một URL thành công thức =IMAGE() của Google Sheets.
#         Bỏ qua nếu URL không hợp lệ.
#         """
#         if url and isinstance(url, str) and url.startswith(('http://', 'https://')):
#             # Bao bọc URL trong dấu ngoặc kép để công thức hoạt động chính xác
#             return f'=IMAGE("{url}")'
#         return ""

# # --- VÍ DỤ SỬ DỤNG ---
# if __name__ == '__main__':
#     # --- Cấu hình để test ---
#     CREDENTIALS_FILE = 'db-connector-v1-b12681524556.json'
#     SPREADSHEET_ID = '1tIKnATQ5886Kguu8AUbZ9XVU1Xww0wa4ido5ELbh0LQ'

#     # --- Chuẩn bị dữ liệu mẫu ---
#     sample_headers = ['campaign_id', 'spend', 'orders', 'ghi_chu', 'product_img']
#     sample_data = [
#         {'campaign_id': '12345', 'spend': 150.75, 'orders': 5, 'product_img': 'https://p16-oec-sg.ibyteimg.com/tos-alisg-i-aphluv4xwc-sg/8f986f8f2dc44cd8a51617e251980174~tplv-aphluv4xwc-origin-jpeg.jpeg?dr=15568&nonce=36762&refresh_token=c6720a102f51284834fc6e94abe59a8d&from=1010592719&idc=my&ps=933b5bde&shcp=9b759fb9&shp=3c3c6bcf&t=555f072d'},
#         {'campaign_id': '67890', 'spend': 200.00, 'orders': 8, 'ghi_chu': 'Test note'},
#         {'campaign_id': 'abcde', 'orders': 2} # Dòng này sẽ bị lọc bỏ
#     ]
#     options_overwrite = {
#         'sheetName': 'TestOverwrite',
#         'isOverwrite': True,
#         'isFirstChunk': True
#     }
#     options_append = {
#         'sheetName': 'TestAppend',
#         'isOverwrite': False,
#         'isFirstChunk': False
#     }

#     try:
#         # 1. Khởi tạo writer
#         writer = GoogleSheetWriter(CREDENTIALS_FILE, SPREADSHEET_ID)

#         # 2. Test chức năng ghi đè
#         print("\n--- BẮT ĐẦU TEST GHI ĐÈ ---")
#         rows_written_overwrite = writer.write_data(sample_data, sample_headers, options_overwrite)
#         print(f"Kết thúc test ghi đè. Đã ghi {rows_written_overwrite} dòng.")
        
#         # 3. Test chức năng ghi tiếp
#         print("\n--- BẮT ĐẦU TEST GHI TIẾP ---")
#         # Giả sử ghi lần đầu
#         writer.write_data(sample_data, sample_headers, {**options_append, 'sheetName': 'TestAppend', 'isFirstChunk': True})
#         # Ghi tiếp với dữ liệu mới và header mới
#         new_data = [{'campaign_id': 'xyz', 'cost': 50, 'roi': 15, 'product_img': 'https://p16-oec-sg.ibyteimg.com/tos-alisg-i-aphluv4xwc-sg/8f986f8f2dc44cd8a51617e251980174~tplv-aphluv4xwc-origin-jpeg.jpeg?dr=15568&nonce=36762&refresh_token=c6720a102f51284834fc6e94abe59a8d&from=1010592719&idc=my&ps=933b5bde&shcp=9b759fb9&shp=3c3c6bcf&t=555f072d'}]
#         new_headers = ['campaign_id', 'cost', 'roi', 'product_img']
#         rows_written_append = writer.write_data(new_data, new_headers, options_append)
#         print(f"Kết thúc test ghi tiếp. Đã ghi {rows_written_append} dòng mới.")

#     except Exception as e:
#         print(f"\nLỗi trong quá trình test: {e}")

# services/sheet_writer/gg_sheet_writer.py
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
        
        self.spreadsheet = client.open_by_key(spreadsheet_id)
        logger.info(f"Đã mở thành công spreadsheet: '{self.spreadsheet.title}'")

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
        if is_first_chunk and (is_overwrite or is_sheet_empty):
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
                logger.info(f"Sheet '{task_id}' đã được ẩn.")
            
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
            
    def _create_image_formula(self, url: str) -> str:
        """Chuyển URL thành =IMAGE() formula"""
        if url and isinstance(url, str) and url.startswith(('http://', 'https://')):
            return f'=IMAGE("{url}")'
        return ""