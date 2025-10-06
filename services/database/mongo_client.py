import os
from pymongo import MongoClient, UpdateOne
from typing import List, Dict, Any
from datetime import datetime
from pymongo import ASCENDING, DESCENDING

class MongoDbClient:
    def __init__(self):
        mongo_uri = os.getenv("MONGO_URI")
        db_name = os.getenv("MONGO_DATABASE")

        if not mongo_uri:
            raise ValueError("Biến môi trường MONGO_URI chưa được thiết lập.")
            
        print("Đang kết nối tới MongoDB...")
        self.client = MongoClient(mongo_uri)
        self.client.admin.command('ping')
        self.db = self.client[db_name]
        print(f"✅ Kết nối thành công tới database: '{db_name}'")
        
        self._ensure_indexes()

    def save_flattened_reports(self, collection_name: str, data: List[Dict[str, Any]], user_email: str):
        """
        Lưu dữ liệu đã được làm phẳng vào một collection được chỉ định.
        Mỗi `row` trong `data` đã là một dict hoàn chỉnh.
        """
        if not data:
            return 0
            
        collection = self.db[collection_name]
        operations = []

        for row in data:
            unique_id = (
                # f"{user_email}_"
                f"{row.get('advertiser_id')}_"
                f"{row.get('store_id')}_"
                f"{row.get('campaign_id')}_"
                f"{row.get('item_group_id', '')}_"
                f"{row.get('item_id', '')}_"
                f"{row.get('start_date')}_"
                f"{row.get('end_date')}_"
                f"{row.get('stat_time_day')}" 
            )

            # Tạo document cuối cùng bằng cách thêm user_email và timestamp vào row
            document_to_save = {
                **row,
                "user_email": user_email,
                "updated_at": datetime.utcnow()
            }
            
            operations.append(
                UpdateOne(
                    {"_id": unique_id},
                    {"$set": document_to_save},
                    upsert=True
                )
            )

        if not operations:
            return 0

        result = collection.bulk_write(operations)
        modified_count = result.modified_count or 0
        upserted_count = result.upserted_count or 0
        total_ops = upserted_count + modified_count
        
        print(f"Đã ghi/cập nhật {total_ops} bản ghi vào collection '{collection_name}'.")
        return total_ops
    
    # Bạn có thể thêm các hàm find, delete... tương tự với tham số collection_name
    def find(self, collection_name: str, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        return list(self.db[collection_name].find(query))
    
    def _ensure_indexes(self):
        """
        Tạo các index cần thiết cho việc truy vấn hiệu quả.
        Index được tối ưu cho việc lọc theo nhiều trường và một khoảng ngày.
        """
        print("Đang đảm bảo các index truy vấn tồn tại...")
        try:
            # Định nghĩa cấu trúc index chung
            index_definition = [
                ("user_email", ASCENDING),
                ("advertiser_id", ASCENDING),
                ("store_id", ASCENDING),
                ("task_type", ASCENDING),
                ("start_date", DESCENDING) # Dùng start_date cho cả lọc khoảng và sắp xếp
            ]
            
            # Áp dụng cho các collection bạn có
            self.db.product_reports.create_index(index_definition, name="primary_query_idx")
            self.db.creative_reports.create_index(index_definition, name="primary_query_idx")
            
            print("✅ Các index truy vấn đã sẵn sàng.")
        except Exception as e:
            print(f"⚠️ Lỗi khi tạo index: {e}")