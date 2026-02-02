"""
TikTok GMV Report Workers
"""

from typing import Dict, Any, List
from workers.base_report_worker import BaseReportWorker
from services.gmv.campaign_creative_detail import (
    GMVCampaignCreativeDetailReporter, 
    _flatten_creative_report
)
from services.gmv.campaign_product_detail import (
    GMVCampaignProductDetailReporter,
    _flatten_product_report
)
import logging

logger = logging.getLogger(__name__)


class TikTokGMVCreativeWorker(BaseReportWorker):
    """Worker for TikTok GMV Creative reports"""
    
    def _create_reporter(self):
        """Create GMV Creative reporter"""
        return GMVCampaignCreativeDetailReporter(
            access_token=self.context["access_token"],
            advertiser_id=self.context["advertiser_id"],
            store_id=self.context["store_id"],
            progress_callback=self._send_progress,
            job_id=self.job_id,
            redis_client=self.redis_client
        )
    
    def _flatten_data(self, raw_data: List[Dict], context: Dict) -> List[Dict]:
        """Flatten creative data"""
        return _flatten_creative_report(raw_data, context)
    
    def _get_cache_query(self, chunk: Dict[str, str]) -> Dict:
        """Build cache query for creative reports"""
        return {
            "advertiser_id": self.context.get("advertiser_id"),
            "store_id": self.context.get("store_id"),
            "start_date": chunk['start'],
            "end_date": chunk['end']
        }
    
    def _get_collection_name(self) -> str:
        """Get collection name"""
        return "creative_reports"


class TikTokGMVProductWorker(BaseReportWorker):
    """Worker for TikTok GMV Product reports"""
    
    def _create_reporter(self):
        """Create GMV Product reporter"""
        return GMVCampaignProductDetailReporter(
            access_token=self.context["access_token"],
            advertiser_id=self.context["advertiser_id"],
            store_id=self.context["store_id"],
            progress_callback=self._send_progress,
            job_id=self.job_id,
            redis_client=self.redis_client
        )
    
    def _flatten_data(self, raw_data: List[Dict], context: Dict) -> List[Dict]:
        """Flatten product data"""
        return _flatten_product_report(raw_data, context)
    
    def _get_cache_query(self, chunk: Dict[str, str]) -> Dict:
        """Build cache query for product reports"""
        return {
            "advertiser_id": self.context.get("advertiser_id"),
            "store_id": self.context.get("store_id"),
            "start_date": chunk['start'],
            "end_date": chunk['end']
        }
    
    def _get_collection_name(self) -> str:
        """Get collection name"""
        return "product_reports"