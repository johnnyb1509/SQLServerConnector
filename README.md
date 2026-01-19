# SQL Server Connector

Thư viện kết nối SQL Server chuyên dụng cho các tác vụ ETL, được tối ưu hóa cho **Pandas**, hỗ trợ **Tiếng Việt (Unicode)** và **Upsert (Merge)** hiệu năng cao.

## 🚀 Tính năng nổi bật

* **High Performance:** Sử dụng `fast_executemany` giúp insert dữ liệu nhanh gấp 10-50 lần so với thông thường.
* **Smart Upsert:** Tự động chèn mới (Insert) hoặc cập nhật (Update) dựa trên Khóa chính (Primary Key).
* **Schema Evolution:** Tự động tạo bảng nếu chưa có, tự động thêm cột mới (Add Column) nếu DataFrame có thay đổi.
* **Unicode Support:** Xử lý triệt để lỗi font chữ Tiếng Việt khi làm việc với SQL Server & Pandas.
* **SQLAlchemy 2.0:** Tuân thủ chuẩn kết nối hiện đại, an toàn.

---

## 📦 Cài đặt

### Cách 1: Cài đặt trực tiếp từ Git (Khuyên dùng nội bộ)
Dành cho đồng nghiệp trong team, cài đặt không cần file whl.

```bash
# Cài phiên bản mới nhất từ nhánh main

pip install git+https://github.com/johnnyb1509/sqlServerConnector.git
```

### Cách 2: Cài đặt từ file .whl
Dành cho người dùng cuối, cài đặt từ file whl đã build sẵn.

```bash
pip install sqlServerConnector
```


## Cấu hình kết nối Database
File cấu hình `db_config.yaml`

```yaml
# Thông tin kết nối Database
# Lưu ý: Đảm bảo máy tính đã cài đặt ODBC Driver 17 for SQL Server
db_info:
    server: "localhost"  # Ví dụ: localhost hoặc  
    database: "YOUR_DATABASE_NAME"    # Ví dụ: TestDB
    username: "YOUR_USERNAME"         # Ví dụ: sa
    password: "YOUR_PASSWORD"         # Mật khẩu
```

## 📝 Hướng dẫn sử dụng nhanh

1. Khởi tạo kết nối
```python   
import yaml
from connector import SQLServerConnector
# Load config
with open('config/db_config.yaml', 'r') as f:
    conf = yaml.safe_load(f)['db_info']

# Khởi tạo
db = SQLServerConnector(
    server=conf['server'],
    database=conf['database'],
    username=conf['username'],
    password=conf['password']
)
```

2. Lấy dữ liệu (Read)
```python
# Cách 1: Lấy toàn bộ bảng
df = db.get_data("DM_KhachHang")

# Cách 2: Dùng câu lệnh SQL tùy ý
query = """
    SELECT TOP 100 * FROM Sales_Transaction 
    WHERE created_date >= '2023-01-01'
"""
df_sales = db.get_data(query)
print(df_sales.head())
```

3. Ghi dữ liệu (Upsert)
```python
import pandas as pd

# Giả lập dữ liệu
data = {
    'TransactionID': [101, 102],
    'Product': ['Laptop Dell', 'Chuột Logitech'], # Hỗ trợ tiếng Việt
    'Amount': [15000000, 250000]
}
df_new = pd.DataFrame(data)

# Đẩy vào DB
db.upsert_data(
    df=df_new,
    target_table="Fact_Sales",
    primary_key="TransactionID",  # Cột dùng để định danh (tránh trùng lặp)
    auto_evolve_schema=True       # Tự động thêm cột nếu thiếu
)
print("Dữ liệu đã được upsert thành công!")
```

4. Đóng kết nối
```python
# Luôn đóng kết nối khi hoàn tất để giải phóng tài nguyên
db.dispose()
```

## ⚠️ Lưu ý quan trọng
1. **Primary Key:** Khi dùng upsert_data, bắt buộc phải cung cấp primary_key. Nếu bảng chưa có Primary Key, thư viện sẽ tự set cột đó làm khóa chính khi tạo bảng mới.

2. **Date Time:** Các cột ngày tháng nên được convert sang datetime64[ns] trong Pandas trước khi đẩy vào để đảm bảo tính chính xác.
