"""
Worker Factory
Tạo worker instance dựa trên task_type
"""

from typing import Dict, Any, Callable
from typing import List

from .base_report_worker import BaseReportWorker
from .tiktok_gmv_worker import TikTokGMVCreativeWorker, TikTokGMVProductWorker
from .facebook_ads_worker import FacebookDailyWorker, FacebookPerformanceWorker, FacebookBreakdownWorker


class WorkerFactory:
    """Factory để tạo worker phù hợp dựa trên task_type"""
    
    # Map task_type → Worker class
    WORKER_REGISTRY = {
        # TikTok GMV
        "creative": TikTokGMVCreativeWorker,
        "product": TikTokGMVProductWorker,
        
        # Facebook Ads
        "facebook_daily": FacebookDailyWorker,
        "facebook_performance": FacebookPerformanceWorker,  
        "facebook_breakdown": FacebookBreakdownWorker
    }
    
    @classmethod
    def create_worker(
        cls,
        task_type: str,
        context: Dict[str, Any],
        db_client: Any,
        redis_client: Any,
        progress_callback: Callable
    ) -> BaseReportWorker:
        """
        Tạo worker instance dựa trên task_type.
        
        Args:
            task_type: Loại task ("creative", "product", "facebook_daily", etc.)
            context: Job context
            db_client: MongoDB client
            redis_client: Redis client
            progress_callback: Callback function để report progress
            
        Returns:
            Worker instance
            
        Raises:
            ValueError: Nếu task_type không hợp lệ
        """
        worker_class = cls.WORKER_REGISTRY.get(task_type)
        
        if not worker_class:
            raise ValueError(
                f"Unknown task_type: '{task_type}'. "
                f"Supported types: {list(cls.WORKER_REGISTRY.keys())}"
            )
        
        return worker_class(
            context=context,
            db_client=db_client,
            redis_client=redis_client,
            progress_callback=progress_callback
        )
    
    @classmethod
    def get_supported_types(cls) -> List[str]:
        """Lấy danh sách các task_type được hỗ trợ"""
        return list(cls.WORKER_REGISTRY.keys())
    
    @classmethod
    def register_worker(cls, task_type: str, worker_class: type):
        """
        Đăng ký worker mới vào registry (cho mở rộng sau này).
        
        Args:
            task_type: Tên task type
            worker_class: Worker class
        """
        if not issubclass(worker_class, BaseReportWorker):
            raise TypeError(f"{worker_class} must inherit from BaseReportWorker")
        
        cls.WORKER_REGISTRY[task_type] = worker_class

