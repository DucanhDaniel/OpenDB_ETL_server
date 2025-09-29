# # Dockerfile

# # Bước 1: Sử dụng một base image Python chính thức
# FROM python:3.13-slim

# # [THÊM MỚI] Cài đặt các gói hệ thống cần thiết, bao gồm curl cho healthcheck
# RUN apt-get update && apt-get install -y --no-install-recommends curl && \
#     apt-get clean && rm -rf /var/lib/apt/lists/*

# # Bước 2: Thiết lập thư mục làm việc bên trong container
# WORKDIR /app

# # Bước 3: Sao chép file requirements.txt và cài đặt thư viện
# COPY ./requirements.txt .
# RUN pip install --no-cache-dir --upgrade -r requirements.txt

# # Bước 4: Sao chép toàn bộ mã nguồn ứng dụng vào thư mục làm việc
# COPY . .

# # Bước 5: Mở cổng mà Uvicorn sẽ chạy
# EXPOSE 8011

# # Bước 6: Thiết lập lệnh mặc định để chạy ứng dụng
# CMD ["gunicorn", "-w", "4", "-k", "uvicorn.workers.UvicornWorker", "main:app", "--bind", "0.0.0.0:8011"]

# Dockerfile

# --- Giai đoạn 1: Builder ---
# Sử dụng một image Python đầy đủ để cài đặt các dependency
FROM python:3.13-slim AS builder

# Thiết lập thư mục làm việc
WORKDIR /app

# Cài đặt các thư viện cần thiết cho việc build
RUN pip install --upgrade pip

# Sao chép file requirements và cài đặt
# Việc này giúp tận dụng cache của Docker. Nếu file requirements không đổi, bước này sẽ không chạy lại.
COPY requirements.txt .
RUN pip wheel --no-cache-dir --wheel-dir /app/wheels -r requirements.txt


# --- Giai đoạn 2: Runtime ---
# Sử dụng một image Python "slim" gọn nhẹ hơn cho môi trường chạy chính thức
FROM python:3.13-slim

# Thiết lập thư mục làm việc
WORKDIR /app

# Sao chép các dependency đã được build từ giai đoạn trước
COPY --from=builder /app/wheels /wheels
COPY --from=builder /app/requirements.txt .

# Cài đặt các dependency từ wheels đã có sẵn
RUN pip install --no-cache /wheels/*

# Sao chép toàn bộ code của ứng dụng vào image
COPY . .


RUN groupadd --system appgroup

# [THÊM MỚI] Tạo một người dùng hệ thống tên là 'appuser' và thêm vào nhóm 'appgroup'
# --no-create-home: không cần tạo thư mục home cho user này
RUN useradd --system --no-create-home --gid appgroup appuser

# [THÊM MỚI] Gán quyền sở hữu thư mục /app cho người dùng và nhóm mới
RUN chown -R appuser:appgroup /app

# [THÊM MỚI] Chỉ định rằng các lệnh sau sẽ được chạy bởi 'appuser'
USER appuser

# Mở cổng 8001 để FastAPI server có thể nhận request từ bên ngoài
EXPOSE 8011

# Lệnh CMD sẽ được định nghĩa trong docker-compose.yml
# để chúng ta có thể dùng cùng một image cho cả API và Worker