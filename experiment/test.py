# test_mongo.py
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime

# --- LỚP MÔ PHỎNG ĐỂ TEST ---
# Lớp này là phiên bản rút gọn của MongoDbWriter thật để file test có thể chạy độc lập
class SimpleMongoWriter:
    def __init__(self):
        mongo_uri = os.getenv("MONGO_LOCAL_URI")
            
        print(f"Đang kết nối tới MongoDB tại: {mongo_uri.split('@')[-1]}...")
        self.client = MongoClient(mongo_uri)
        self.db = self.client.tiktok_data_warehouse
        self.collection = self.db.raw_reports
        
        # Kiểm tra kết nối
        self.client.admin.command('ping')
        print("✅ Kết nối tới MongoDB thành công!")

    def save_raw_data(self, raw_data: list, user_email: str, advertiser_id: str, task_type: str):
        if not raw_data:
            return 0

        operations = []
        for record in raw_data:
            dimensions = record.get("dimensions", {})
            unique_id = (
                f"{user_email}_{advertiser_id}_"
                f"{dimensions.get('campaign_id', '')}_{dimensions.get('item_group_id', '')}_"
                f"{dimensions.get('stat_time_day', '')}"
            )

            document = {
                "user_email": user_email,
                "advertiser_id": advertiser_id,
                "task_type": task_type,
                "report_date": dimensions.get('stat_time_day'),
                "raw_api_response": record,
                "updated_at": datetime.utcnow()
            }
            
            # Dùng update_one với upsert=True
            from pymongo import UpdateOne
            operations.append(
                UpdateOne(
                    {'_id': unique_id},
                    {'$set': document},
                    upsert=True
                )
            )

        result = self.collection.bulk_write(operations)
        print(f"Thao tác ghi hàng loạt hoàn tất. Đã chèn/cập nhật: {result.upserted_count + result.modified_count} bản ghi.")
        return result.upserted_count + result.modified_count

# --- HÀM CHÍNH ĐỂ CHẠY KỊCH BẢN TEST ---
def run_test():
    print("--- BẮT ĐẦU KỊCH BẢN TEST MONGODB ---")
    load_dotenv()
    
    # Dữ liệu mẫu
    test_user_email = "test.user.1@example.com"
    test_advertiser_id = "adv-test-98765"
    sample_raw_data = [
        {"dimensions": {"campaign_id": "C1", "item_group_id": "P1", "stat_time_day": "2025-10-04"}, "metrics": {"cost": 10}},
        {"dimensions": {"campaign_id": "C2", "item_group_id": "P2", "stat_time_day": "2025-10-04"}, "metrics": {"cost": 20}},
    ]

    try:
        # 1. Khởi tạo writer
        db_writer = SimpleMongoWriter()

        # 2. Ghi dữ liệu
        db_writer.save_raw_data(
            raw_data=sample_raw_data,
            user_email=test_user_email,
            advertiser_id=test_advertiser_id,
            task_type="product"
        )
        
        # 3. Đọc lại để xác nhận
        print("\nĐọc lại dữ liệu từ MongoDB để xác nhận:")
        results = db_writer.collection.find({"user_email": test_user_email})
        found_records = list(results)
        
        if not found_records:
            raise Exception("Không tìm thấy bản ghi nào sau khi ghi.")

        print(f"✅ Tìm thấy {len(found_records)} bản ghi:")
        for record in found_records:
            print(f"  - ID: {record['_id']}, Cost: {record['raw_api_response']['metrics'].get('cost')}")
            
        print("\n--- ✅ TEST THÀNH CÔNG! ---")

    except Exception as e:
        print(f"\n--- ❌ TEST THẤT BẠI ---")
        print(f"Lỗi: {e}")

if __name__ == "__main__":
    run_test()