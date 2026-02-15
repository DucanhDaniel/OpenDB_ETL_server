
import sys
import os
import logging
from typing import List, Dict, Any
from unittest.mock import MagicMock, patch

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import after path setup
# We need to ensure that when we import CurrencyExchangeService, it can find its dependencies
try:
    from services.currency.exchange_rate_service import CurrencyExchangeService
except ImportError as e:
    logger.error(f"Import Error: {e}")
    sys.exit(1)

def test_currency_exchange():
    print("Testing CurrencyExchangeService...")
    
    # Mock data from sheet
    mock_config_data = [
        {"Account ID": "act_123", "Exchange Rate (Multiplier)": 24000, "Status": "Active"},
        {"Account ID": "act_456", "Exchange Rate (Multiplier)": 0.5, "Status": "Active"},
        {"Account ID": "act_789", "Exchange Rate (Multiplier)": 100, "Status": "Inactive"}, # Should be ignored
    ]
    
    # Patch where the class is USED, not where it is defined
    with patch('services.currency.exchange_rate_service.GoogleSheetWriter') as MockWriter:
        mock_instance = MockWriter.return_value
        mock_instance.read_sheet_data.return_value = mock_config_data
        
        # Initialize Service
        service = CurrencyExchangeService("dummy_spreadsheet_id")
        
        # Load Config
        service.load_config()
        
        # Verify Config Loading
        if service.exchange_rates.get("act_123") == 24000:
            print("✅ Config loaded: act_123 rate is 24000")
        else:
            print(f"❌ Config failed: act_123 rate is {service.exchange_rates.get('act_123')}")
            
        if "act_789" not in service.exchange_rates:
            print("✅ Config loaded: Inactive account ignored")
        else:
            print("❌ Config failed: Inactive account included")

        # Test Data Application
        sample_data = [
            {"account_id": "act_123", "spend": 10},         # Should be 240,000
            {"account_id": "act_456", "spend": 100},        # Should be 50.0
            {"account_id": "act_999", "spend": 50},         # No config, should remain 50
        ]
        
        result = service.apply_exchange(sample_data)
        
        # Verify Results
        row1 = result[0]
        if row1.get("spend") == 240000:
            print("✅ Exchange applied: act_123 spend converted to 240000")
        else:
            print(f"❌ Exchange failed: act_123 spend is {row1.get('spend')}")
            
        row2 = result[1]
        if row2.get("spend") == 50.0:
            print("✅ Exchange applied: act_456 spend converted to 50.0")
        else:
            print(f"❌ Exchange failed: act_456 spend is {row2.get('spend')}")

if __name__ == "__main__":
    test_currency_exchange()
