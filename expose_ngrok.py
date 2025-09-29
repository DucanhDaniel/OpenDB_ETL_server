
import uvicorn
from pyngrok import ngrok
if __name__ == "__main__":
    port = 8011
    
    try:

        public_url = ngrok.connect(port, "http")
        print("="*50)
        print(f" * Ngrok tunnel đang chạy tại: {public_url}")
        print(f" * Uvicorn đang chạy trên http://127.0.0.1:{port}")
        print("="*50)
        
        # Chạy uvicorn. Lưu ý: truyền đối tượng 'app' trực tiếp
        
    except Exception as e:
        print(f"Lỗi: {e}")
    finally:
        # Ngắt kết nối ngrok khi ứng dụng dừng
        ngrok.disconnect(public_url.public_url)
        print("Đã đóng kết nối ngrok.")
        
# celery -A celery_worker.celery_app worker -P gevent --loglevel=info