import unittest
from unittest.mock import MagicMock, patch
import sys
import os

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from services.facebook.err_handler.rate_limit import EnhancedBackoffHandler
from services.facebook.err_handler.facebook_error_handler import FacebookErrorType

class TestEnhancedBackoffHandler(unittest.TestCase):
    def setUp(self):
        self.mock_reporter = MagicMock()
        self.handler = EnhancedBackoffHandler(reporter=self.mock_reporter)

    @patch('time.sleep')
    def test_no_backoff(self, mock_sleep):
        """Test with successful responses and no summary errors"""
        responses = [{'status_code': 200}]
        summary = {'rate_limits': {'app_usage_pct': 10}}
        self.handler.analyze_and_backoff(responses, summary)
        mock_sleep.assert_not_called()

    @patch('time.sleep')
    def test_response_rate_limit_app(self, mock_sleep):
        """Test rate limit detected in response (App Limit - Code 4 -> 300s)"""
        responses = [{
            'status_code': 400,
            'error': {
                'code': 4,
                'message': 'App Rate limit',
                'error_subcode': None
            },
            'requested_url': 'http://test.com'
        }]
        
        self.handler.analyze_and_backoff(responses, summary=None)
        
        # Expected: 300s + 5s buffer = 305s
        # 300s comes from FacebookErrorHandler._rate_limit_error for code 4
        mock_sleep.assert_called_with(305)
        self.mock_reporter._report_progress.assert_called()

    @patch('time.sleep')
    def test_summary_app_usage_high(self, mock_sleep):
        """Test rate limit detected in summary (App usage 96% -> 300s)"""
        responses = [{'status_code': 200}]
        summary = {'rate_limits': {'app_usage_pct': 96}}
        
        self.handler.analyze_and_backoff(responses, summary)
        
        # Expected: 300s + 5s = 305s
        mock_sleep.assert_called_with(305)

    @patch('time.sleep')
    def test_response_exceeds_max_backoff(self, mock_sleep):
        """Test reaction when backoff exceeds MAX_BACKOFF_SECONDS (Code 80000 -> 600s)"""
        # MAX_BACKOFF_SECONDS is 660 in class
        # Code 80000 defaults to 600s in FacebookErrorHandler
        responses = [{
            'status_code': 400,
            'error': {
                'code': 80000,
                'message': 'Ads Insights limit',
                'error_subcode': None
            },
            'requested_url': 'http://test.com'
        }]
        
        self.handler.analyze_and_backoff(responses, summary=None)
        
        mock_sleep.assert_called_with(605)

    @patch('time.sleep')
    def test_combined_backoff_priority(self, mock_sleep):
        """Test that MAX(response_backoff, summary_backoff) is used"""
        # Response: Code 613 -> 180s
        responses = [{
            'status_code': 400,
            'error': {
                'code': 613,
                'message': 'Custom limit',
                'error_subcode': None
            },
            'requested_url': 'http://test.com'
        }]
        
        # Summary: App usage 99% -> 300s
        summary = {'rate_limits': {'app_usage_pct': 99}}
        
        self.handler.analyze_and_backoff(responses, summary)
        
        # Should take max(180, 300) = 300 + 5 = 305
        mock_sleep.assert_called_with(305)

    @patch('time.sleep')
    def test_summary_account_eta(self, mock_sleep):
        """Test summary with explicit ETA from account details"""
        responses = [{'status_code': 200}]
        summary = {
            'rate_limits': {
                'account_details': [{
                    'account_id': '123',
                    'eta_seconds': 150
                }]
            }
        }
        
        self.handler.analyze_and_backoff(responses, summary)
        
        # Expected: 150 + 5 = 155
        mock_sleep.assert_called_with(155)

if __name__ == '__main__':
    unittest.main()
