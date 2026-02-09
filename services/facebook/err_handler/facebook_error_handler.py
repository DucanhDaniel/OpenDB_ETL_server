"""
Facebook API Error Handler
Xử lý và classify errors từ Facebook Graph API với rate limit detection
"""

import logging
from typing import Dict, Any, Optional, Tuple
from enum import Enum

logger = logging.getLogger(__name__)


class FacebookErrorType(Enum):
    """Classification of Facebook API errors"""
    RATE_LIMIT = "rate_limit"
    PERMISSION = "permission"
    INVALID_REQUEST = "invalid_request"
    SERVER_ERROR = "server_error"
    TEMPORARY = "temporary"
    UNKNOWN = "unknown"


class FacebookErrorHandler:
    """Handler để detect và classify Facebook API errors"""
    
    # Rate limit error codes
    RATE_LIMIT_CODES = {
        4: "App rate limit",
        17: "User rate limit",
        32: "Page rate limit",
        613: "Custom rate limit",
        80000: "Ads Insights rate limit",
        80001: "Page (System User token) rate limit",
        80002: "Instagram rate limit",
        80003: "Custom Audience rate limit",
        80004: "Ads Management rate limit",
        80005: "LeadGen rate limit",
        80006: "Messenger rate limit",
        80008: "WhatsApp Business rate limit",
        80009: "Catalog Management rate limit",
        80014: "Catalog Batch rate limit",
    }
    
    RATE_LIMIT_SUBCODES = {
        2446079: "Ads API v3.3+ rate limit",
        1996: "Inconsistent API request volume"
    }
    
    PERMISSION_CODES = {
        10: "Permission denied",
        200: "Permission denied (missing permission)",
        190: "Access token expired/invalid",
        102: "Session expired",
        104: "Incorrect signature"
    }
    
    @classmethod
    def analyze_error(cls, error_response: Dict[str, Any]) -> Dict[str, Any]:
        """Phân tích error response từ Facebook API"""
        if not error_response:
            return cls._unknown_error()
        
        error_code = error_response.get("code", 0)
        error_subcode = error_response.get("error_subcode")
        message = error_response.get("message", "Unknown error")
        
        # Check for rate limit
        if cls._is_rate_limit_error(error_code, error_subcode):
            return cls._rate_limit_error(error_code, error_subcode, message)
        
        # Check for permission errors
        if error_code in cls.PERMISSION_CODES:
            return cls._permission_error(error_code, message)
        
        # Check for server errors (5xx)
        if error_code >= 500:
            return cls._server_error(error_code, message)
        
        # Invalid request (4xx)
        if error_code >= 400 and error_code < 500:
            return cls._invalid_request_error(error_code, message)
        
        return cls._unknown_error(error_code, message)
    
    @classmethod
    def _is_rate_limit_error(cls, code: int, subcode: Optional[int]) -> bool:
        """Check if error is rate limit related"""
        if code in cls.RATE_LIMIT_CODES:
            return True
        if subcode and subcode in cls.RATE_LIMIT_SUBCODES:
            return True
        return False
    
    @classmethod
    def _rate_limit_error(cls, code: int, subcode: Optional[int], message: str) -> Dict[str, Any]:
        """Handle rate limit errors"""
        error_name = cls.RATE_LIMIT_CODES.get(code, "Unknown rate limit")
        
        # Determine backoff time
        if code == 4:
            backoff = 300
        elif code == 17 and subcode == 2446079:
            backoff = 300
        elif code in [80000, 80004]:
            backoff = 600
        else:
            backoff = 180
        
        return {
            "error_type": FacebookErrorType.RATE_LIMIT,
            "is_retryable": True,
            "should_backoff": True,
            "backoff_seconds": backoff,
            "error_code": code,
            "error_subcode": subcode,
            "message": message,
            "user_message": f"Facebook rate limit ({error_name}). Retry sau {backoff}s.",
            "rate_limit_type": error_name
        }
    
    @classmethod
    def _permission_error(cls, code: int, message: str) -> Dict[str, Any]:
        """Handle permission errors"""
        error_name = cls.PERMISSION_CODES.get(code, "Permission error")
        
        return {
            "error_type": FacebookErrorType.PERMISSION,
            "is_retryable": False,
            "should_backoff": False,
            "backoff_seconds": 0,
            "error_code": code,
            "error_subcode": None,
            "message": message,
            "user_message": f"Lỗi quyền truy cập: {error_name}"
        }
    
    @classmethod
    def _server_error(cls, code: int, message: str) -> Dict[str, Any]:
        """Handle server errors"""
        return {
            "error_type": FacebookErrorType.SERVER_ERROR,
            "is_retryable": True,
            "should_backoff": True,
            "backoff_seconds": 30,
            "error_code": code,
            "error_subcode": None,
            "message": message,
            "user_message": f"Facebook server error ({code}). Sẽ retry."
        }
    
    @classmethod
    def _invalid_request_error(cls, code: int, message: str) -> Dict[str, Any]:
        """Handle invalid request errors"""
        return {
            "error_type": FacebookErrorType.INVALID_REQUEST,
            "is_retryable": False,
            "should_backoff": False,
            "backoff_seconds": 0,
            "error_code": code,
            "error_subcode": None,
            "message": message,
            "user_message": f"Request không hợp lệ ({code}): {message}"
        }
    
    @classmethod
    def _unknown_error(cls, code: Optional[int] = None, message: str = "Unknown error") -> Dict[str, Any]:
        """Handle unknown errors"""
        return {
            "error_type": FacebookErrorType.UNKNOWN,
            "is_retryable": True,
            "should_backoff": True,
            "backoff_seconds": 60,
            "error_code": code,
            "error_subcode": None,
            "message": message,
            "user_message": f"Lỗi không xác định: {message}"
        }
    
    @classmethod
    def should_fail_immediately(cls, error_info: Dict[str, Any]) -> bool:
        """Determine if error should fail job immediately"""
        error_type = error_info.get("error_type")
        
        if error_type == FacebookErrorType.PERMISSION:
            return True
        
        if error_type == FacebookErrorType.INVALID_REQUEST:
            error_code = error_info.get("error_code")
            if error_code in [400, 403]:
                return True
        
        return False