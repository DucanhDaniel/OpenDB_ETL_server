import logging
from datetime import datetime
import calendar
logger = logging.getLogger(__name__)
import time


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
    
# def write_data_to_sheet(job_id, spreadsheet_id, context, flattened_data, writer):
#     if not spreadsheet_id:
#         raise ValueError("Chưa có spreadsheet_id.")

#     # Lấy các tùy chọn ghi từ context được gửi từ Apps Script
#     sheet_options = {
#         "sheetName": context.get("sheet_name"),
#         "isOverwrite": context.get("is_overwrite", False),
#         "isFirstChunk": context.get("is_first_chunk", False)
#     }

#     # Lấy selected_fields từ context
#     selected_fields = context.get("selected_fields")

#     # Ưu tiên dùng selected_fields làm headers, nếu không có thì dùng như cũ để dự phòng
#     if selected_fields:
#         headers = selected_fields
#         logger.info(f"[Job ID: {job_id}] Sử dụng {len(headers)} trường đã chọn làm tiêu đề.")
#     else:
#         headers = list(flattened_data[0].keys())
#         logger.warning(f"[Job ID: {job_id}] Không có selected_fields. Sử dụng tất cả {len(headers)} trường có sẵn làm tiêu đề.")

#     # Ghi dữ liệu
#     rows_written = writer.write_data(flattened_data, headers, sheet_options)
#     final_message = f"Hoàn tất! Đã ghi {rows_written} dòng vào sheet '{sheet_options['sheetName']}'."
#     return final_message





def write_data_to_sheet(job_id, spreadsheet_id, context, flattened_data, writer):
    """
    Ghi dữ liệu vào Google Sheet với retry mechanism và chunked writing.
    
    Args:
        job_id: Job ID
        spreadsheet_id: Google Sheet ID
        context: Request context
        flattened_data: Data to write
        writer: GoogleSheetWriter instance
        
    Returns:
        Success message
    """
    if not spreadsheet_id:
        raise ValueError("Chưa có spreadsheet_id.")
    
    if not flattened_data:
        logger.warning(f"[Job {job_id}] No data to write")
        return "No data to write"

    # Sheet options
    sheet_options = {
        "sheetName": context.get("sheet_name"),
        "isOverwrite": context.get("is_overwrite", False),
        "isFirstChunk": context.get("is_first_chunk", False)
    }
    
    # Get headers
    selected_fields = context.get("selected_fields")
    
    if selected_fields:
        headers = selected_fields
        logger.info(f"[Job {job_id}] Using {len(headers)} selected fields as headers")
    else:
        headers = list(flattened_data[0].keys())
        logger.warning(f"[Job {job_id}] No selected_fields. Using all {len(headers)} available fields")
    
    # ========== CHUNKED WRITING STRATEGY ==========
    # Large datasets (>1000 rows) được chia thành chunks
    CHUNK_SIZE = 1000
    total_rows = len(flattened_data)
    
    if total_rows <= CHUNK_SIZE:
        # Small dataset - write once
        logger.info(f"[Job {job_id}] Writing {total_rows} rows in single operation")
        
        rows_written = writer.write_data(flattened_data, headers, sheet_options)
        
        return f"Hoàn tất! Đã ghi {rows_written} dòng vào sheet '{sheet_options['sheetName']}'."
    
    else:
        # Large dataset - write in chunks
        logger.info(f"[Job {job_id}] Writing {total_rows} rows in chunks of {CHUNK_SIZE}")
        
        total_written = 0
        is_first = sheet_options["isFirstChunk"]
        
        for i in range(0, total_rows, CHUNK_SIZE):
            chunk = flattened_data[i:i + CHUNK_SIZE]
            chunk_num = (i // CHUNK_SIZE) + 1
            total_chunks = (total_rows + CHUNK_SIZE - 1) // CHUNK_SIZE
            
            logger.info(f"[Job {job_id}] Writing chunk {chunk_num}/{total_chunks} ({len(chunk)} rows)")
            
            # First chunk uses original options, subsequent chunks are appends
            chunk_options = {
                **sheet_options,
                "isFirstChunk": is_first,
                "isOverwrite": is_first and sheet_options["isOverwrite"]
            }
            
            try:
                rows_written = writer.write_data(chunk, headers, chunk_options)
                total_written += rows_written
                logger.info(f"[Job {job_id}] Chunk {chunk_num} written: {rows_written} rows")
                
                # After first chunk, rest are appends
                is_first = False
                
                # Small delay between chunks to avoid rate limits
                if i + CHUNK_SIZE < total_rows:
                    time.sleep(0.5)
                    
            except Exception as e:
                logger.error(f"[Job {job_id}] Error writing chunk {chunk_num}: {e}")
                
                # If it's not the first chunk, we can continue with partial data
                if not is_first:
                    logger.warning(f"[Job {job_id}] Continuing with {total_written} rows written so far")
                    break
                else:
                    # First chunk failed - re-raise        
                    raise
        
        return f"Hoàn tất! Đã ghi {total_written}/{total_rows} dòng vào sheet '{sheet_options['sheetName']}'."

