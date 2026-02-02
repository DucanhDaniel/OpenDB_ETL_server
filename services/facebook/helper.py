import json

def write_to_file(output_filename, data): 

    # 3. Ghi dữ liệu ra file
    try:
        with open(output_filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"Đã ghi thành công dữ liệu vào file: {output_filename}")
    except Exception as e:
        print(f"Có lỗi khi ghi file: {e}")