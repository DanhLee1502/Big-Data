# Bước 1 – Khám phá & làm sạch dữ liệu

Đầu vào: `hdfs://namenode:8020/data/raw/asean_data.csv`

| File | Mô tả | Đầu ra |
|---|---|---|
| `1a_doc_raw.py` | Đọc CSV thô, in tổng số dòng/cột, schema, 10 dòng đầu, thống kê mô tả (`describe`), ngày bắt đầu – kết thúc và danh sách mã quốc gia. | In ra màn hình |
| `1b_kiem_tra_null.py` | Đánh giá chất lượng dữ liệu: (1) đếm bản ghi theo cấp quốc gia / tỉnh / huyện, (2) số lượng và tỷ lệ NULL từng cột, (3) bản ghi trùng theo `(date, location_key)`, (4) thống kê tổng ca mắc, tử vong của từng nước. | In ra màn hình |
| `1c_lam_sach.py` | Làm sạch qua 7 bước rồi lưu Parquet (xem bên dưới). | `/data/processed/1_cleaned_data` |

## Các bước trong `1c_lam_sach.py`

1. Lọc cấp quốc gia (`location_key` dài 2 ký tự)
2. Xóa dòng NULL ở `date` hoặc `location_key`
3. Xóa trùng lặp theo `(date, location_key)`
4. Điền `0` cho NULL ở 8 cột số liệu
5. Chuyển `date` sang kiểu Date
6. Thêm `country_name` (tên tiếng Việt) từ `location_key`
7. Thêm cột `year`, `month`, `quarter` (Q1–Q4)

> `1a` và `1b` chỉ để khám phá, có thể bỏ qua. Chỉ `1c` là bắt buộc cho các bước sau.
