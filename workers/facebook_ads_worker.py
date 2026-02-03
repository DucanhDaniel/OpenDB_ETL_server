"""
Facebook Ads Report Workers
"""

from typing import Dict, Any, List
from workers.base_report_worker import BaseReportWorker
from services.facebook.daily_processor import FacebookDailyReporter
from services.facebook.generic_processor import FacebookPerformanceReporter
from services.facebook.breakdown_processor import FacebookBreakdownReporter
import logging
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class FacebookAdsWorker(BaseReportWorker):
    """
    Abstract class for Facebook workers
    Every Facebook reporter must have get_report(...) to get data.
    """
    
    @abstractmethod
    def _create_reporter(self):
        """Create Facebook Ads reporter"""
        pass
    
    def _flatten_data(self, raw_data: List[Dict], context: Dict) -> List[Dict]:
        """
        Facebook data is already flattened by reporter.
        Just return as-is.
        """
        return raw_data
    
    def _get_cache_query(self, chunk: Dict[str, str]) -> Dict:
        """Build cache query for Facebook reports"""
        return {
            "user_email": self.context.get("user_email"),
            "template_name": self.context.get("template_name"),
            "accounts": self.context.get("accounts"),  # List of account IDs
            "start_date": chunk['start'],
            "end_date": chunk['end']
        }
        
    def _get_collection_name(self) -> str:
        """Get collection name based on template"""
        template_name = self.context.get("template_name", "default")
        # Sanitize template name for collection name
        safe_name = template_name.lower().replace(" ", "_").replace("-", "_")
        return f"facebook_{safe_name}_reports"
    
    def run(self) -> Dict[str, Any]:
        """
        Override run to handle Facebook-specific flow.
        Facebook reporter returns flattened data directly.
        """
        logger.info(f"[Job {self.job_id}] Starting Facebook Daily worker")
        
        try:
            # Initialize
            self._send_progress("RUNNING", "Initializing Facebook reporter...", 0)
            reporter = self._create_reporter()
            
            # Get accounts to process
            accounts = self.context.get("accounts", [])
            if not accounts:
                raise ValueError("No accounts specified")
            
            # Get template and fields
            template_name = self.context.get("template_name")
            selected_fields = self.context.get("selected_fields", [])
            
            if not template_name:
                raise ValueError("No template specified")
            
            logger.info(f"Processing {len(accounts)} accounts with template: {template_name}")
            
            # For now, skip cache and fetch directly
            # TODO: Implement proper caching later
            self._send_progress("RUNNING", "Fetching data from Facebook API...", 20)
            
            data = reporter.get_report(
                accounts_to_process=accounts,
                start_date=self.context["start_date"],
                end_date=self.context["end_date"],
                template_name=template_name,
                selected_fields=selected_fields
            )
            
            self.api_rows = len(data)
            
            # Check cancellation
            self._check_cancellation()
            
            # Write to sheet
            message = "No data to write"
            if data:
                self._send_progress("RUNNING", "Writing to sheet...", 95)
                message = self._write_to_sheet(data)
            
            logger.info(f"[Job {self.job_id}] Completed: {len(data)} total rows")
            
            return {
                "status": "COMPLETED",
                "message": message,
                "api_usage": {},  # Facebook doesn't track API usage same way
                "stats": {
                    "cached_rows": 0,
                    "api_rows": self.api_rows,
                    "total_rows": len(data)
                }
            }
            
        except Exception as e:
            logger.error(f"[Job {self.job_id}] Error: {e}", exc_info=True)
            raise
        
    
class FacebookDailyWorker(FacebookAdsWorker):
    """Worker for Facebook Daily reports"""
    
    def _create_reporter(self):
        """Create Facebook Daily reporter"""
        return FacebookDailyReporter(
            access_token=self.context["access_token"],
            email=self.context.get("user_email", "unknown@example.com"),
            progress_callback=self._send_progress,
            job_id=self.job_id
        )

class FacebookPerformanceWorker(FacebookAdsWorker):
    """Worker for Facebook Overview Performance reporter"""
    
    def _create_reporter(self):
        """Create Facebook Daily reporter"""
        return FacebookPerformanceReporter(
            access_token=self.context["access_token"],
            email=self.context.get("user_email", "unknown@example.com"),
            progress_callback=self._send_progress,
            job_id=self.job_id
        )
        
class FacebookBreakdownWorker(FacebookAdsWorker):
    """Worker for Facebook Breakdown reporter"""
    
    def _create_reporter(self):
        """Create Facebook Daily reporter"""
        return FacebookBreakdownReporter(
            access_token=self.context["access_token"],
            email=self.context.get("user_email", "unknown@example.com"),
            progress_callback=self._send_progress,
            job_id=self.job_id
        )
        
    