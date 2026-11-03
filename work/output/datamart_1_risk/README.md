# DM1 – Mức độ nguy hiểm

Tạo bởi `work/3_datamart/3a_risk.py` từ bảng tổng hợp tháng.

Mỗi dòng = 1 quốc gia × 1 tháng, kèm 2 cột phân loại:
- `muc_do_lay_lan`: 1. Cực kỳ nghiêm trọng (≥ 500.000 ca) → 5. Thấp (< 1.000 ca)
- `muc_do_tu_vong`: theo CFR – ≥ 3% / 1–3% / 0,5–1% / < 0,5%

> ⚠️ File CSV hiện có trong thư mục này chứa cột MoM của 3b (`xu_huong_dich`…) thay vì 2 cột phân loại trên. Chạy lại `3a_risk.py` để cập nhật.
