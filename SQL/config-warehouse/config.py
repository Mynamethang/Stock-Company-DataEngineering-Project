import duckdb
import os

# Đường dẫn tới file DuckDB
database_path = '/home/ngocthang/Documents/Code/Stock-Company-Analysis/warehouse/test_datawarehouse.duckdb'

# Xóa file cơ sở dữ liệu nếu tồn tại
if os.path.exists(database_path):
    os.remove(database_path)
# Kết nối đến DuckDB, tạo hoặc mở cơ sở dữ liệu
conn = duckdb.connect(database=database_path)

# Đọc nội dung của file SQL
with open('/home/ngocthang/Documents/Code/Stock-Company-Analysis/SQL/config-warehouse/SQL_datawarehouse.sql', 'r') as file:
    sql_script = file.read()

# Chạy các câu lệnh SQL từ file
conn.execute(sql_script)

# Đóng kết nối
conn.close()

print(f"Database đã được tạo và lưu vào {database_path}")