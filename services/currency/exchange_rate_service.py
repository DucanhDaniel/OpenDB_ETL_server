
import logging
from typing import Dict, List, Any
from services.sheet_writer.gg_sheet_writer import GoogleSheetWriter
from dotenv import load_dotenv
load_dotenv()
import os

logger = logging.getLogger(__name__)

CREDENTIALS_PATH = os.getenv('GOOGLE_CREDENTIALS_PATH', 'credentials.json')

class CurrencyExchangeService:
    """
    Service để xử lý quy đổi tiền tệ dựa trên config từ Google Sheet.
    """
    
    CONFIG_SHEET_NAME = "[CONFIG] Currency Exchange"
    
    def __init__(self, spreadsheet_id: str):
        self.spreadsheet_id = spreadsheet_id
        self.exchange_rates: Dict[str, float] = {} # {account_id: multiplier}
        self.is_loaded = False
        
    def load_config(self):
        """Đọc config từ sheet và lưu vào memory"""
        try:
            writer = GoogleSheetWriter(CREDENTIALS_PATH, self.spreadsheet_id)
            records = writer.read_sheet_data(self.CONFIG_SHEET_NAME)
            
            if not records:
                logger.warning(f"Không tìm thấy data trong sheet '{self.CONFIG_SHEET_NAME}' hoặc sheet không tồn tại.")
                return

            count = 0
            for row in records:
                # Expected columns: Account ID, Exchange Rate (Multiplier), Status
                account_id = str(row.get("Account ID", "")).strip()
                rate = row.get("Exchange Rate (Multiplier)")
                status = str(row.get("Status", "")).strip()
                
                # Chỉ lấy Active hoặc config không có cột Status (mặc định active)
                if status and status.lower() != "active":
                    continue
                    
                if not account_id:
                    continue
                    
                try:
                    rate_val = float(rate)
                    self.exchange_rates[account_id] = rate_val
                    count += 1
                except (ValueError, TypeError):
                    logger.warning(f"Invalid Exchange Rate for Account {account_id}: {rate}")
            
            self.is_loaded = True
            logger.info(f"Đã load {count} rule đổi tiền tệ từ Config.")
            
        except Exception as e:
            logger.error(f"Lỗi khi load currency config: {e}")

    def apply_exchange(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Duyệt qua data và nhân spend với exchange rate nếu match Account ID.
        """
        if not self.is_loaded or not self.exchange_rates:
            return data
            
        converted_count = 0
        
        for row in data:
            # Tìm Account ID trong row
            # Các trường có thể là account_id: "account_id", "Account ID", "ad_account_id"
            account_id = str(row.get("account_id") or row.get("Account ID") or "").strip()
            
            if not account_id:
                continue
                
            rate = self.exchange_rates.get(account_id)
            if rate:
                # Apply conversion to 'spend'
                if "spend" in row:
                    try:
                        original_spend = float(row["spend"])
                        row["spend"] = original_spend * rate
                        
                        # Mark as converted (optional debug info)
                        # row["_currency_converted"] = True
                        # row["_exchange_rate"] = rate
                        
                        converted_count += 1
                    except (ValueError, TypeError):
                        pass
                        
                # Apply to other monetary fields? 
                # Hiện tại user chỉ yêu cầu spend. Nếu cần thêm cost, cpc, cpm thì thêm ở đây.
                
        if converted_count > 0:
            logger.info(f"Đã quy đổi tiền tệ cho {converted_count} dòng dữ liệu.")
            
        return data


if __name__ == "__main__":
    service = CurrencyExchangeService("1E-RrnqcPE2SaUnqmWe_O-KyhCNcFlp27FdUb05SrDJQ")
    service.load_config()
    data = [
        {"account_id": "act_948290596967304", "spend": 100},
        {"account_id": "act_948290596967304", "spend": 200},
    ]
    data = service.apply_exchange(data)
    print(data)