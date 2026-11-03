# DM2 – Tốc độ tăng trưởng theo tháng (MoM)

Tạo bởi `work/3_datamart/3b_mom_growth.py` (Window `LAG`).

Cột thêm so với bảng tháng:
- `ca_mac_thang_truoc`: số ca mắc tháng liền trước
- `toc_do_tang_truong_pct`: (tháng này − tháng trước) / tháng trước × 100
- `xu_huong_dich`: Bùng phát cực mạnh (>100%) / Tăng nhanh (>20%) / Tăng nhẹ / Đi ngang / Giảm nhẹ / Suy giảm mạnh (<−20%)

> ⚠️ Thư mục chưa có file CSV. Chạy `3b_mom_growth.py` để tạo.
