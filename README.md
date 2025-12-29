# SQL Data Connector Project

Project này cung cấp module Python mạnh mẽ (`dbConnector`) để tương tác với Microsoft SQL Server, tối ưu hóa cho các tác vụ Data Engineering như: ETL, Insert dữ liệu lớn, và Đồng bộ hóa (Upsert) dữ liệu từ Pandas DataFrame.

## Tính năng nổi bật

* **Tự động hóa cao**: Tự động tạo bảng, phát hiện kiểu dữ liệu, và thêm cột mới nếu DataFrame thay đổi.
* **Hiệu năng cao**: Sử dụng `fast_executemany=True` của SQLAlchemy/pyodbc để tăng tốc độ Insert.
* **An toàn dữ liệu**: Hỗ trợ Transaction (Commit/Rollback) để đảm bảo tính toàn vẹn dữ liệu.
* **Sync thông minh**: Hàm `check_and_update_table` giúp so sánh và chỉ update những dòng thay đổi, insert những dòng mới.
* **Tiện ích**: Hỗ trợ làm sạch dữ liệu số (ví dụ: convert "1.5M" thành 1,500,000).

## Yêu cầu cài đặt

1.  **Hệ điều hành**: Windows, Linux, hoặc MacOS.
2.  **Driver**: Cần cài đặt **ODBC Driver 17 for SQL Server**.
    * [Tải về tại đây (Microsoft)](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)
3.  **Python Libraries**:
    ```bash
    pip install -r requirements.txt
    ```

## Cấu trúc Project

* `src/dbConnector.py`: Module chính chứa class `dbJob`.
* `config/db_config.yaml`: File cấu hình database (cần tự tạo dựa trên mẫu).
* `notebooks/demo_usage.ipynb`: Ví dụ cách sử dụng.

## Hướng dẫn sử dụng nhanh

### 1. Cấu hình kết nối
Tạo file `config/db_config.yaml`:
```yaml
db_info:
  server: "localhost"
  database: "MyDatabase"
  username: "sa"
  password: "mypassword"