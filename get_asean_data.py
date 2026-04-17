import pandas as pd
import os

url = "https://storage.googleapis.com/covid19-open-data/v3/epidemiology.csv"
local_file = "asean_data.csv"

# Danh sách mã quốc gia Đông Nam Á
# Việt Nam (VN), Thái Lan (TH), Malaysia (MY), Singapore (SG), Indonesia (ID), Philippines (PH), Brunei (BN), Campuchia (KH), Lào (LA), Myanmar (MM), Timor-Leste (TL).
# Tuple này dùng để quét cột location_key
sea_codes = ('VN', 'TH', 'MY', 'SG', 'ID', 'PH', 'BN', 'KH', 'LA', 'MM', 'TL')

print("Đang lấy data từ Google và lọc khu vực Đông Nam Á...")

first_chunk = True
total_rows = 0

# Đọc file lớn theo từng khúc 100.000 dòng
for chunk in pd.read_csv(url, chunksize=100000, dtype=str):
    
    # LỌC: Lấy các dòng có location_key bắt đầu bằng mã của 11 nước ĐNÁ
    # Nó sẽ lấy luôn cả các tỉnh thành (ví dụ: ID_AC cho tỉnh Aceh của Indonesia)
    asean_data = chunk[chunk['location_key'].str.startswith(sea_codes, na=False)]
    
    # Nếu khúc này có dữ liệu ĐNÁ thì ghi nối vào file
    if not asean_data.empty:
        asean_data.to_csv(local_file, mode='a', header=first_chunk, index=False)
        first_chunk = False
        total_rows += len(asean_data)

print("-" * 40)
print(f"File đã được lưu thành: {local_file}")
print(f"Tổng số dòng thu được: {total_rows} dòng.")