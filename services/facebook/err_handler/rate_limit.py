# """
# Enhanced Backoff Logic
# Analyze individual responses for rate limit errors và backoff accordingly
# """

import time
import logging
from typing import Dict, Any, List
from services.facebook.err_handler.facebook_error_handler import FacebookErrorHandler, FacebookErrorType

logger = logging.getLogger(__name__)


class EnhancedBackoffHandler:
    """
    Handler để analyze responses và thực hiện intelligent backoff.
    Combines summary-based backoff (from batch API) với per-response error analysis.
    """
    
    MAX_BACKOFF_SECONDS = 360  # 6 minutes
    PLUS_BACKOFF_SEC = 5  # Extra buffer time
    
    def __init__(self, reporter):
        """
        Args:
            reporter: FacebookAdsBaseReporter instance (để access _report_progress)
        """
        self.reporter = reporter
    
    def analyze_and_backoff(
        self, 
        responses: List[Dict[str, Any]], 
        summary: Dict[str, Any] = None
    ):
        """
        Analyze tất cả responses và summary để quyết định backoff.
        
        Args:
            responses: List of response objects từ batch API
            summary: Summary dict từ batch API (optional)
            
        Raises:
            Exception: Nếu backoff time quá MAX_BACKOFF_SECONDS
        """
        # 1. Analyze individual responses for rate limit errors
        response_backoff = self._analyze_response_errors(responses)
        
        # 2. Analyze summary from batch API (if available)
        summary_backoff = self._calculate_backoff_from_summary(summary) if summary else {
            "should_backoff": False,
            "backoff_seconds": 0,
            "reason": None
        }
        
        # 3. Combine: Take the MAXIMUM backoff needed
        should_backoff = response_backoff["should_backoff"] or summary_backoff["should_backoff"]
        
        if not should_backoff:
            return 0
        
        backoff_seconds = max(
            response_backoff.get("backoff_seconds", 0),
            summary_backoff.get("backoff_seconds", 0)
        )
        
        # Combine reasons
        reasons = []
        if response_backoff.get("reason"):
            reasons.append(response_backoff["reason"])
        if summary_backoff.get("reason"):
            reasons.append(summary_backoff["reason"])
        
        reason = " + ".join(reasons)
        
        # 4. Check if backoff is too long
        if backoff_seconds > self.MAX_BACKOFF_SECONDS:
            error_msg = (
                f"Rate limit backoff quá lâu ({backoff_seconds}s > {self.MAX_BACKOFF_SECONDS}s). "
                f"Lý do: {reason}"
            )
            logger.error(error_msg)
            self.reporter._report_progress(message=error_msg)
            raise Exception(error_msg)
        
        # 5. Perform backoff
        total_backoff = backoff_seconds + self.PLUS_BACKOFF_SEC
        
        logger.warning(f"⚠ Rate limit detected. Chờ {total_backoff}s. Lý do: {reason}")
        self.reporter._report_progress(
            message=f"⚠ Rate limit detected. Chờ {total_backoff}s. Lý do: {reason}"
        )
        
        time.sleep(total_backoff)
        
        logger.info("✓ Backoff hoàn tất, tiếp tục xử lý.")
        self.reporter._report_progress(message="✓ Backoff hoàn tất, tiếp tục xử lý.")

        return total_backoff
    
    def _analyze_response_errors(self, responses: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Scan through responses để tìm rate limit errors.
        
        Returns:
            {
                "should_backoff": bool,
                "backoff_seconds": int,
                "reason": str,
                "rate_limit_errors": List[Dict]  # Detailed error info
            }
        """
        max_backoff = 0
        rate_limit_errors = []
        
        for response in responses:
            # Skip successful responses
            if response.get("status_code") == 200:
                continue
            
            # Get error data
            error_data = response.get("error", {})
            
            if not error_data:
                continue
            
            # Analyze error
            error_info = FacebookErrorHandler.analyze_error(error_data)
            
            # Check if it's a rate limit error
            if error_info["error_type"] == FacebookErrorType.RATE_LIMIT:
                backoff_time = error_info.get("backoff_seconds", 0)
                
                if backoff_time > max_backoff:
                    max_backoff = backoff_time
                
                rate_limit_errors.append({
                    "url": response.get("original_url", "unknown"),
                    "error_code": error_info["error_code"],
                    "error_subcode": error_info.get("error_subcode"),
                    "rate_limit_type": error_info.get("rate_limit_type"),
                    "backoff_seconds": backoff_time
                })
                
                logger.warning(
                    f"  Rate limit in response: {error_info.get('rate_limit_type')} "
                    f"(Code: {error_info['error_code']}, "
                    f"Subcode: {error_info.get('error_subcode')}, "
                    f"Backoff: {backoff_time}s)"
                )
        
        # Build reason
        if rate_limit_errors:
            # Group by type
            types = {}
            for err in rate_limit_errors:
                rate_type = err["rate_limit_type"]
                types[rate_type] = types.get(rate_type, 0) + 1
            
            type_summary = ", ".join([f"{count}x {rtype}" for rtype, count in types.items()])
            reason = f"Response errors: {type_summary}"
        else:
            reason = None
        
        return {
            "should_backoff": max_backoff > 0,
            "backoff_seconds": max_backoff,
            "reason": reason,
            "rate_limit_errors": rate_limit_errors
        }
    
    def _calculate_backoff_from_summary(self, summary: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate backoff từ summary với multiple factors:
        1. App/Account usage percentage
        2. Business use case metrics (total_time, total_cputime)
        3. ETA (estimated time to regain access)
        
        Returns:
            {
                "should_backoff": bool,
                "backoff_seconds": int,
                "reason": str
            }
        """
        if not summary or "rate_limits" not in summary:
            return {"should_backoff": False, "backoff_seconds": 0, "reason": None}
        
        rate_limits = summary["rate_limits"]
        max_backoff_seconds = 0
        backoff_reason = None
        
        # 1. Check app-level usage
        app_usage = rate_limits.get("app_usage_pct", 0)
        if app_usage >= 95:
            max_backoff_seconds = max(max_backoff_seconds, 300)  # 5 minutes
            backoff_reason = f"App usage cao: {app_usage}%"
        elif app_usage >= 75:
            max_backoff_seconds = max(max_backoff_seconds, 60)  # 1 minute
            backoff_reason = f"App usage vừa phải: {app_usage}%"
        
        # 2. Check account-level limits
        account_details = rate_limits.get("account_details", [])
        for account in account_details:
            account_id = account.get("account_id", "unknown")
            
            # 2a. Insights usage
            insights_usage = account.get("insights_usage_pct", 0)
            if insights_usage >= 95:
                max_backoff_seconds = max(max_backoff_seconds, 300)
                backoff_reason = f"Account {account_id} insights usage cao: {insights_usage}%"
            elif insights_usage >= 75:
                max_backoff_seconds = max(max_backoff_seconds, 60)
                backoff_reason = f"Account {account_id} insights usage vừa: {insights_usage}%"
            
            # 2b. ETA from business use cases
            eta = account.get("eta_seconds", 0)
            if eta > max_backoff_seconds:
                max_backoff_seconds = eta
                backoff_reason = f"Account {account_id} yêu cầu chờ {eta}s"
            
            # 2c. Business use case metrics (NEW!)
            business_use_cases = account.get("business_use_cases", [])
            for use_case in business_use_cases:
                use_case_type = use_case.get("type", "unknown")
                total_time = use_case.get("total_time", 0)
                total_cputime = use_case.get("total_cputime", 0)
                call_count = use_case.get("call_count", 0)
                
                # Calculate backoff based on total_time and total_cputime
                time_based_backoff = self._calculate_time_based_backoff(
                    total_time, 
                    total_cputime, 
                    call_count,
                    use_case_type
                )
                
                if time_based_backoff["backoff_seconds"] > max_backoff_seconds:
                    max_backoff_seconds = time_based_backoff["backoff_seconds"]
                    backoff_reason = time_based_backoff["reason"]
        
        return {
            "should_backoff": max_backoff_seconds > 0,
            "backoff_seconds": max_backoff_seconds,
            "reason": backoff_reason
        }
    
    def _calculate_time_based_backoff(
        self,
        total_time: int,
        total_cputime: int,
        call_count: int,
        use_case_type: str
    ) -> Dict[str, Any]:
        """
        Calculate backoff based on API time metrics.
        
        Facebook rate limits are based on:
        - total_time: Total wall-clock time spent on API calls (seconds)
        - total_cputime: Total CPU time spent processing (seconds)
        - Thresholds vary by access tier
        
        Reference thresholds (for development_access):
        - ads_management: 5,000 CPU seconds per hour
        - ads_insights: 5,000 CPU seconds per hour
        
        Args:
            total_time: Wall-clock time in seconds
            total_cputime: CPU time in seconds
            call_count: Number of API calls made
            use_case_type: "ads_management", "ads_insights", etc.
            
        Returns:
            {"backoff_seconds": int, "reason": str}
        """
        backoff_seconds = 0
        reason = None
        
        THRESHOLDS = {
            "ads_management": {
                "critical_cputime": 90,  
                "warning_cputime": 70,   
                "critical_time": 90,
                "warning_time": 70
            },
            "ads_insights": {
                "critical_cputime": 90,  
                "warning_cputime": 70,   
                "critical_time": 90,
                "warning_time": 70
            },
            "default": {
                "critical_cputime": 90,
                "warning_cputime": 70,
                "critical_time": 90,
                "warning_time": 70
            }
        }
        
        thresholds = THRESHOLDS.get(use_case_type, THRESHOLDS["default"])
        
        if total_cputime >= thresholds["critical_cputime"]:
            backoff_seconds = 300  
            reason = f"{use_case_type}: CPU time cao ({total_cputime}s / ~100s limit)"
            
        elif total_cputime >= thresholds["warning_cputime"]:
            backoff_seconds = 120  
            reason = f"{use_case_type}: CPU time vừa ({total_cputime}s / ~100s limit)"
        
        if total_time >= thresholds["critical_time"]:
            backoff_seconds = max(backoff_seconds, 300)
            reason = f"{use_case_type}: Total time cao ({total_time}s)"
                
        elif total_time >= thresholds["warning_time"]:
            backoff_seconds = max(backoff_seconds, 120)
            if not reason:
                reason = f"{use_case_type}: Total time vừa ({total_time}s)"
        
        if call_count > 100 and total_time < 100:
            avg_time_per_call = total_time / call_count if call_count > 0 else 0
            if avg_time_per_call < 0.5:
                backoff_seconds = max(backoff_seconds, 60)
                if not reason:
                    reason = f"{use_case_type}: Request rate quá nhanh ({call_count} calls in {total_time}s)"
        
        return {
            "backoff_seconds": backoff_seconds,
            "reason": reason
        }

