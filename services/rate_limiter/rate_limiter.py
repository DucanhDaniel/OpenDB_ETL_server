# services/utils/rate_limiter.py
import time
from redis import Redis
from typing import List, Tuple

class RedisRateLimiter:
    """
    Một bộ giới hạn tần suất (rate limiter) sử dụng Redis,
    có khả năng xử lý nhiều quy tắc giới hạn (ví dụ: mỗi giây và mỗi phút).
    """
    def __init__(self, redis_client: Redis, rules: List[Tuple[int, int]]):
        """
        Khởi tạo limiter.
        Args:
            redis_client: Instance của redis.Redis.
            rules (List[Tuple[int, int]]): Một danh sách các quy tắc. 
                                           Mỗi quy tắc là một tuple (limit, period).
                                           Ví dụ: [(10, 1), (600, 60)]
        """
        if not rules:
            raise ValueError("Phải có ít nhất một quy tắc giới hạn.")
        self.redis = redis_client
        self.rules = sorted(rules, key=lambda x: x[1]) # Sắp xếp theo period để tối ưu

    def acquire(self, base_key: str) -> bool:
        """
        Cố gắng "chiếm" một slot request. Trả về True nếu được phép theo TẤT CẢ các quy tắc.
        """
        for limit, period in self.rules:
            # Tạo một key duy nhất trong Redis cho mỗi quy tắc
            key = f"{base_key}:{period}s"
            
            current_count = self.redis.incr(key)
            
            if current_count == 1:
                self.redis.expire(key, period)
            
            # Nếu vi phạm bất kỳ quy tắc nào, từ chối ngay lập tức
            if current_count > limit:
                return False
        
        # Nếu vượt qua tất cả các quy tắc, cho phép request
        return True