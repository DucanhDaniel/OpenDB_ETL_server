# services/sheet_writer.py
import gspread
from google.oauth2.service_account import Credentials
import os
from datetime import datetime

class GoogleSheetWriter:
    """
    Một lớp để kết nối và ghi dữ liệu vào Google Sheets bằng Service Account.
    Tái tạo lại logic từ các hàm _writeDataToSheet và _formatSheetColumns của Apps Script.
    """
    def __init__(self, credentials_path: str, spreadsheet_id: str):
        """
        Khởi tạo writer và xác thực với Google.
        Args:
            credentials_path (str): Đường dẫn đến file credentials.json của Service Account.
            spreadsheet_id (str): ID của file Google Sheet cần ghi vào.
        """
        if not os.path.exists(credentials_path):
            raise FileNotFoundError(f"Không tìm thấy file credentials tại: {credentials_path}")
            
        print("Đang xác thực với Google Sheets API...")
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_file(credentials_path, scopes=scopes)
        client = gspread.authorize(creds)
        
        self.spreadsheet = client.open_by_key(spreadsheet_id)
        print(f"Đã mở thành công spreadsheet: '{self.spreadsheet.title}'")

    def _get_or_create_worksheet(self, sheet_name: str) -> gspread.Worksheet:
        """Lấy một worksheet theo tên, hoặc tạo mới nếu chưa tồn tại."""
        try:
            return self.spreadsheet.worksheet(sheet_name)
        except gspread.WorksheetNotFound:
            print(f"Không tìm thấy sheet '{sheet_name}'. Đang tạo sheet mới...")
            return self.spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=50)

    def _format_columns(self, worksheet: gspread.Worksheet, headers: list):
        """Định dạng các cột dựa trên tên header."""
        # Định nghĩa các loại cột
        text_columns = {"advertiser_id", "campaign_id", "store_id", "item_group_id", "item_id", "tt_user_id", "video_id", "id", "adset_id"}
        number_columns = {"New Messaging Connections", "Cost Purchases", "Website Purchases", "On-Facebook Purchases", "Leads", "Purchases", "Cost Leads", "Cost per New Messaging", "Purchase Value", "Purchase ROAS", "frequency", "ctr","spend", "cpc", "cpm", "cost_per_conversion", "total_onsite_shopping_value", "cost", "cost_per_order", "gross_revenue", "net_cost", "roas_bid", "target_roi_budget", "max_delivery_budget","daily_budget","budget_remaining","lifetime_budget", "roi"}
        integer_columns = {"reach", "impressions", "clicks", "conversion", "video_play_actions", "orders", "product_impressions", "product_clicks"}
        # TIKTOK_PERCENT_METRICS cần được định nghĩa ở đâu đó nếu bạn dùng
        # TIKTOK_PERCENT_METRICS = {"ad_conversion_rate", ...}

        requests = []
        for i, header in enumerate(headers):
            col_letter = gspread.utils.rowcol_to_a1(1, i + 1).rstrip('1')
            range_to_format = f"{col_letter}2:{col_letter}" # Định dạng từ hàng thứ 2 trở đi
            
            format_pattern = None
            if header in text_columns:
                format_pattern = {"type": "TEXT", "pattern": "@"}
            elif header in number_columns:
                format_pattern = {"type": "NUMBER", "pattern": "#,##0.00"}
            # elif header in TIKTOK_PERCENT_METRICS:
            #     format_pattern = {"type": "NUMBER", "pattern": "0.00%"}
            elif header in integer_columns:
                format_pattern = {"type": "NUMBER", "pattern": "#,##0"}

            if format_pattern:
                requests.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": worksheet.id,
                            "startColumnIndex": i,
                            "endColumnIndex": i + 1,
                            "startRowIndex": 1 # Bắt đầu từ hàng 2
                        },
                        "cell": {"userEnteredFormat": {"numberFormat": format_pattern}},
                        "fields": "userEnteredFormat.numberFormat"
                    }
                })
        
        if requests:
            self.spreadsheet.batch_update({"requests": requests})
            print(f"Đã áp dụng định dạng cho {len(requests)} cột.")

    def write_data(self, data_to_write: list, headers: list, options: dict) -> int:
        """
        Hàm chính để ghi dữ liệu vào sheet, xử lý cả overwrite và append.
        Tự động mở rộng cột nếu dữ liệu vượt quá giới hạn lưới (Grid limits).
        """
        sheet_name = options.get('sheetName')
        is_overwrite = options.get('isOverwrite', False)
        is_first_chunk = options.get('isFirstChunk', False)
        
        if not sheet_name:
            raise ValueError("Thiếu 'sheetName' trong options.")

        # Lọc bỏ các dòng không có thuộc tính 'spend' (nếu header có 'spend')
        if 'spend' in headers:
            original_count = len(data_to_write)
            data_to_write = [row for row in data_to_write if 'spend' in row]
            print(f"Dữ liệu ban đầu: {original_count} dòng. Sau khi lọc 'spend': {len(data_to_write)} dòng.")

        worksheet = self._get_or_create_worksheet(sheet_name)
        
        # --- LOGIC MỚI: Đảm bảo đủ số cột cho headers ngay từ đầu ---
        # Kiểm tra nếu số lượng headers cần ghi lớn hơn số cột hiện có của sheet
        if len(headers) > worksheet.col_count:
            cols_to_add = len(headers) - worksheet.col_count
            print(f"Sheet đang thiếu {cols_to_add} cột. Đang thêm cột mới...")
            worksheet.add_cols(cols_to_add)
        # ------------------------------------------------------------

        is_sheet_empty = worksheet.row_count == 0 or (worksheet.get('A1') is None)

        # ---- XỬ LÝ GHI ĐÈ (OVERWRITE) ----
        if is_first_chunk and (is_overwrite or is_sheet_empty):
            print(f"Chế độ Ghi đè. Đang xóa và ghi lại sheet '{sheet_name}'...")
            worksheet.clear()
            
            if not data_to_write and not headers:
                return 0

            # Chuyển đổi list of dicts thành list of lists
            rows_data = [
                [self._create_image_formula(row.get(h, '')) if h == 'product_img' else row.get(h, '') for h in headers]
                for row in data_to_write
            ]
            rows = [list(headers)] + rows_data
            
            # Vì đã add_cols ở trên nên lệnh này sẽ không bị lỗi 400 Grid limits nữa
            worksheet.update(range_name='A1', values=rows, value_input_option='USER_ENTERED')
            
            # Định dạng header
            worksheet.format("1:1", {'textFormat': {'bold': True}, 'horizontalAlignment': 'CENTER'})
            
            self._format_columns(worksheet, headers)
            return len(data_to_write)

        # ---- XỬ LÝ GHI TIẾP (APPEND) ----
        print(f"Chế độ Ghi tiếp vào sheet '{sheet_name}'...")
        if not data_to_write:
            return 0
            
        existing_headers = worksheet.row_values(1)
        new_headers_to_add = [h for h in headers if h not in existing_headers]
        
        final_headers = existing_headers + new_headers_to_add

        # --- LOGIC MỚI CHO APPEND: Kiểm tra lại tổng số cột sau khi gộp header cũ và mới ---
        # Nếu có header mới, tổng số cột sẽ tăng lên, cần kiểm tra lại lần nữa
        if len(final_headers) > worksheet.col_count:
            cols_to_add = len(final_headers) - worksheet.col_count
            print(f"Phát hiện thêm cột mới trong chế độ Append. Đang thêm {cols_to_add} cột...")
            worksheet.add_cols(cols_to_add)
        # -----------------------------------------------------------------------------------

        if new_headers_to_add:
            print(f"Phát hiện cột mới: {new_headers_to_add}. Đang thêm header vào sheet...")
            start_col = len(existing_headers) + 1
            # Cập nhật header mới vào các ô tiếp theo
            worksheet.update(range_name=gspread.utils.rowcol_to_a1(1, start_col), values=[new_headers_to_add], value_input_option='USER_ENTERED')
            worksheet.format(f"1:1", {'textFormat': {'bold': True}, 'horizontalAlignment': 'CENTER'})
        
        # Chuyển đổi list of dicts thành list of lists theo đúng thứ tự của final_headers
        rows_to_append = [
            [self._create_image_formula(row.get(h, '')) if h == 'product_img' else row.get(h, '') for h in final_headers]
            for row in data_to_write
        ]
        
        worksheet.append_rows(rows_to_append, value_input_option='USER_ENTERED')
        
        self._format_columns(worksheet, final_headers)
        return len(rows_to_append)
    
    def log_progress(self, task_id: str, status: str, message: str, progress: int):
        """
        Ghi log tiến trình vào một sheet riêng biệt có tên là task_id.
        """
        try:
            # Lấy hoặc tạo một sheet có tên là CURRENT_TASK_STATUS
            worksheet = self._get_or_create_worksheet('CURRENT_TASK_STATUS')

            if not worksheet._properties.get('hidden', False):
                body = {
                    "requests": [
                        {
                            "updateSheetProperties": {
                                "properties": {
                                    "sheetId": worksheet.id,
                                    "hidden": True
                                },
                                "fields": "hidden"
                            }
                        }
                    ]
                }
                self.spreadsheet.batch_update(body)
                print(f"Sheet log '{task_id}' đã được ẩn.")
            
            # Dữ liệu cần ghi: status, progress, message, và timestamp
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            data = [
                ['task_id', 'status', 'progress', 'message', 'last_updated'],
                [task_id, status, progress, message, timestamp]
            ]
            
            # Ghi đè vào các ô đầu tiên của sheet
            worksheet.update(range_name='A1', values=data)
            print(f"Logged progress for task {task_id}: {message}")

        except Exception as e:
            print(f"ERROR: Không thể ghi log tiến trình cho task {task_id}: {e}")
            
    def _create_image_formula(self, url: str) -> str:
        """
        Chuyển đổi một URL thành công thức =IMAGE() của Google Sheets.
        Bỏ qua nếu URL không hợp lệ.
        """
        if url and isinstance(url, str) and url.startswith(('http://', 'https://')):
            # Bao bọc URL trong dấu ngoặc kép để công thức hoạt động chính xác
            return f'=IMAGE("{url}")'
        return ""

# --- VÍ DỤ SỬ DỤNG ---
if __name__ == '__main__':
    # --- Cấu hình để test ---
    CREDENTIALS_FILE = 'db-connector-v1-b12681524556.json'
    SPREADSHEET_ID = '1tIKnATQ5886Kguu8AUbZ9XVU1Xww0wa4ido5ELbh0LQ'

    # --- Chuẩn bị dữ liệu mẫu ---
    sample_headers = ['campaign_id', 'spend', 'orders', 'ghi_chu', 'product_img']
    sample_data = [
        {'campaign_id': '12345', 'spend': 150.75, 'orders': 5, 'product_img': 'https://p16-oec-sg.ibyteimg.com/tos-alisg-i-aphluv4xwc-sg/8f986f8f2dc44cd8a51617e251980174~tplv-aphluv4xwc-origin-jpeg.jpeg?dr=15568&nonce=36762&refresh_token=c6720a102f51284834fc6e94abe59a8d&from=1010592719&idc=my&ps=933b5bde&shcp=9b759fb9&shp=3c3c6bcf&t=555f072d'},
        {'campaign_id': '67890', 'spend': 200.00, 'orders': 8, 'ghi_chu': 'Test note'},
        {'campaign_id': 'abcde', 'orders': 2} # Dòng này sẽ bị lọc bỏ
    ]
    options_overwrite = {
        'sheetName': 'TestOverwrite',
        'isOverwrite': True,
        'isFirstChunk': True
    }
    options_append = {
        'sheetName': 'TestAppend',
        'isOverwrite': False,
        'isFirstChunk': False
    }

    try:
        # 1. Khởi tạo writer
        writer = GoogleSheetWriter(CREDENTIALS_FILE, SPREADSHEET_ID)

        # 2. Test chức năng ghi đè
        print("\n--- BẮT ĐẦU TEST GHI ĐÈ ---")
        rows_written_overwrite = writer.write_data(sample_data, sample_headers, options_overwrite)
        print(f"Kết thúc test ghi đè. Đã ghi {rows_written_overwrite} dòng.")
        
        # 3. Test chức năng ghi tiếp
        print("\n--- BẮT ĐẦU TEST GHI TIẾP ---")
        # Giả sử ghi lần đầu
        writer.write_data(sample_data, sample_headers, {**options_append, 'sheetName': 'TestAppend', 'isFirstChunk': True})
        # Ghi tiếp với dữ liệu mới và header mới
        new_data = [{'campaign_id': 'xyz', 'cost': 50, 'roi': 15, 'product_img': 'https://p16-oec-sg.ibyteimg.com/tos-alisg-i-aphluv4xwc-sg/8f986f8f2dc44cd8a51617e251980174~tplv-aphluv4xwc-origin-jpeg.jpeg?dr=15568&nonce=36762&refresh_token=c6720a102f51284834fc6e94abe59a8d&from=1010592719&idc=my&ps=933b5bde&shcp=9b759fb9&shp=3c3c6bcf&t=555f072d'}]
        new_headers = ['campaign_id', 'cost', 'roi', 'product_img']
        rows_written_append = writer.write_data(new_data, new_headers, options_append)
        print(f"Kết thúc test ghi tiếp. Đã ghi {rows_written_append} dòng mới.")

    except Exception as e:
        print(f"\nLỗi trong quá trình test: {e}")