# Bước 0 – Thu thập dữ liệu

| File | Mô tả |
|---|---|
| `get_asean_data.py` | Tải `epidemiology.csv` (toàn cầu) từ Google Cloud Storage, đọc theo từng khối 100.000 dòng để không tràn RAM, giữ các dòng có `location_key` bắt đầu bằng mã 11 nước ĐNÁ (gồm cả cấp tỉnh/huyện) và ghi nối vào `data/raw/asean_data.csv`. |

- **Công cụ:** Python + Pandas, chạy trên máy (không cần Spark).
- **Đầu ra:** `data/raw/asean_data.csv`

```bash
pip install pandas
python work/0_thu_thap/get_asean_data.py
```

> Script ghi nối (`mode='a'`), nên hãy xóa file `asean_data.csv` cũ trước khi chạy lại.
