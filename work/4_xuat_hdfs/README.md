# Bước 4 – Xuất kết quả & kiểm tra HDFS

| File | Mô tả |
|---|---|
| `4_luu_hdfs_kiem_tra.py` | Bước cuối của pipeline, gồm 5 phần: (1) đọc 5 data mart Parquet và xuất mỗi bảng ra CSV trên HDFS, (2) làm tương tự với dữ liệu sạch và 3 bảng tổng hợp, (3) kiểm tra sự tồn tại và đếm số bản ghi của mọi thư mục trong `raw/`, `processed/`, `export/`, (4) in sơ đồ cây HDFS, (5) in bảng tổng kết số bản ghi qua từng giai đoạn. |

- **Đầu vào:** `/data/processed/*` (kết quả bước 1–3)
- **Đầu ra:** `/data/export/<tên_bảng>_csv/` – 9 thư mục CSV

> Chạy sau khi đã hoàn tất bước 1, 2 và 3. Bảng nào chưa có sẽ được báo `❌ LỖI` / `KHÔNG TÌM THẤY` thay vì dừng chương trình.
