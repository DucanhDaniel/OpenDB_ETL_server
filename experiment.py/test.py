import gspread
import time

# --- CẤU HÌNH CẦN THAY ĐỔI ---

# 1. Đặt tên file key của bạn.
SERVICE_ACCOUNT_FILE = 'db-connector-v1-b12681524556.json' 

# 2. Đặt ID của Google Sheet bạn muốn test.
# Bạn có thể lấy ID từ URL của sheet:
# https://docs.google.com/spreadsheets/d/THIS_IS_THE_ID/edit
SPREADSHEET_ID = '1sQ2w0GusBac5esDZSIYVG8pL-_EdjIEO1S2d2nGMpkE'

# 3. Đặt tên sheet (tab) bạn muốn ghi vào.
WORKSHEET_NAME = 'Sheet1' 

# -----------------------------

def test_google_sheets_connection():
    """
    Kết nối tới Google Sheets bằng Service Account và ghi dữ liệu test.
    """
    print("Đang thử kết nối đến Google Sheets...")
    try:
        # Xác thực bằng file service account
        gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
        
        print(f"Xác thực thành công với Service Account.")
        
        # Mở Spreadsheet bằng ID
        sh = gc.open_by_key(SPREADSHEET_ID)
        
        print(f"Mở thành công Spreadsheet: '{sh.title}'")
        
        # Chọn một worksheet (tab) theo tên
        worksheet = sh.worksheet(WORKSHEET_NAME)
        
        print(f"Chọn thành công worksheet: '{worksheet.title}'")
        
        # Chuẩn bị dữ liệu test
        test_data = [
            ['Tên', 'Email', 'Thời gian Test'],
            ['Bot Test', 'bot@example.com', str(time.ctime())]
        ]
        
        # Ghi dữ liệu vào sheet (ví dụ: ghi 2 dòng, bắt đầu từ ô A1)
        worksheet.update(test_data, 'A1')
        
        print("\n" + "="*30)
        print("✅ THÀNH CÔNG!")
        print(f"Đã ghi dữ liệu test vào sheet '{WORKSHEET_NAME}'. Hãy kiểm tra file Google Sheet của bạn.")
        print("="*30)

    except gspread.exceptions.SpreadsheetNotFound:
        print("\n❌ LỖI: Không tìm thấy Spreadsheet.")
        print("Vui lòng kiểm tra lại SPREADSHEET_ID và đảm bảo bạn đã 'Share' Sheet cho email của Service Account.")
    except gspread.exceptions.WorksheetNotFound:
        print(f"\n❌ LỖI: Không tìm thấy worksheet (tab) có tên '{WORKSHEET_NAME}'.")
    except FileNotFoundError:
        print(f"\n❌ LỖI: Không tìm thấy file '{SERVICE_ACCOUNT_FILE}'.")
        print("Hãy đảm bảo file key của bạn có tên đúng và nằm cùng thư mục với script này.")
    # except Exception as e:
    #     print(f"\n❌ Đã xảy ra lỗi không mong muốn:")
    #     print(e)
    #     if "permission_denied" in str(e).lower():
    #         print("LỖI QUYỀN TRUY CẬP (PERMISSION_DENIED).")
    #         print("Rất có thể bạn chưa kích hoạt 'Google Sheets API' trong Google Cloud Console.")
    #     print(e)

if __name__ == "__main__":
    test_google_sheets_connection()