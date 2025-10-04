import logging
from datetime import datetime
import calendar
logger = logging.getLogger(__name__)


def is_full_month(start_date_str: str, end_date_str: str) -> bool:
    """
    Kiểm tra xem khoảng thời gian có phải là một tháng trọn vẹn hay không.
    Ví dụ: '2025-09-01' đến '2025-09-30' -> True
    '2025-09-15' đến '2025-09-30' -> False
    """
    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()

        # 1. Start date phải là ngày 1
        if start_date.day != 1:
            return False

        # 2. Start date và end date phải cùng tháng, cùng năm
        if start_date.month != end_date.month or start_date.year != end_date.year:
            return False

        # 3. End date phải là ngày cuối cùng của tháng đó
        _, last_day_of_month = calendar.monthrange(end_date.year, end_date.month)
        if end_date.day != last_day_of_month:
            return False

        return True
    except (ValueError, TypeError):
        return False
    
def write_data_to_sheet(job_id, spreadsheet_id, context, flattened_data, writer):
    if not spreadsheet_id:
        raise ValueError("Chưa có spreadsheet_id.")

    # Lấy các tùy chọn ghi từ context được gửi từ Apps Script
    sheet_options = {
        "sheetName": context.get("sheet_name"),
        "isOverwrite": context.get("is_overwrite", False),
        "isFirstChunk": context.get("is_first_chunk", False)
    }
            
    # Lấy selected_fields từ context
    selected_fields = context.get("selected_fields")

    # Ưu tiên dùng selected_fields làm headers, nếu không có thì dùng như cũ để dự phòng
    if selected_fields:
        headers = selected_fields
        logger.info(f"[Job ID: {job_id}] Sử dụng {len(headers)} trường đã chọn làm tiêu đề.")
    else:
        headers = list(flattened_data[0].keys())
        logger.warning(f"[Job ID: {job_id}] Không có selected_fields. Sử dụng tất cả {len(headers)} trường có sẵn làm tiêu đề.")

    # Ghi dữ liệu
    rows_written = writer.write_data(flattened_data, headers, sheet_options)
    final_message = f"Hoàn tất! Đã ghi {rows_written} dòng vào sheet '{sheet_options['sheetName']}'."
    return final_message