# work/output – Kết quả CSV của các Data Mart

Đầu ra local của các script trong `work/3_datamart/` (trong container: `/home/jovyan/work/output`).
Mỗi thư mục con là kết quả Spark `coalesce(1)`, gồm một file `part-00000-*.csv` (có header) và file đánh dấu `_SUCCESS`.

| Thư mục | Script tạo ra | Nội dung | Số dòng |
|---|---|---|---|
| `datamart_1_risk/` | `3a_risk.py` | Quốc gia × tháng kèm nhãn mức độ nguy hiểm | 363 |
| `datamart_2_growth/` | `3b_mom_growth.py` | Quốc gia × tháng kèm tăng trưởng MoM | chưa có CSV |
| `datamart_3_top3/` | `3c_ranking.py` | Top 3 tâm dịch mỗi tháng | 99 |
| `datamart_4_yearly/` | `3d_yearly_summary.py` | Tổng kết quốc gia × năm | 33 |
| `datamart_5_peak/` | `3e_peak.py` | Tháng đỉnh dịch của 11 nước | 11 |

## Ý nghĩa các cột

| Cột | Ý nghĩa |
|---|---|
| `location_key`, `country_name` | Mã và tên quốc gia |
| `year`, `month`, `quarter` | Năm, tháng, quý |
| `tong_ca_mac_moi`, `tong_ca_tu_vong`, `tong_ca_phuc_hoi`, `tong_xet_nghiem` | Tổng ca mắc mới / tử vong / phục hồi / xét nghiệm trong kỳ |
| `luy_ke_ca_mac`, `luy_ke_tu_vong` | Giá trị lũy kế cao nhất tính đến cuối kỳ |
| `so_ngay_ghi_nhan` | Số ngày có dữ liệu trong tháng |
| `ty_le_tu_vong_pct` | CFR (%) = tử vong / ca mắc × 100 |
| `ca_mac_tb_ngay` | Ca mắc trung bình mỗi ngày |
| `ca_mac_thang_truoc`, `toc_do_tang_truong_pct`, `xu_huong_dich` | Số ca tháng trước, % tăng trưởng MoM, nhãn xu hướng (DM2) |
| `muc_do_lay_lan`, `muc_do_tu_vong` | Nhãn mức độ nguy hiểm (DM1) |
| `xep_hang` | Thứ hạng ca mắc trong tháng (DM3) |

> ⚠️ CSV trong `datamart_1_risk/` hiện chứa cột MoM của DM2 thay vì cột phân loại của DM1, và `datamart_2_growth/` chưa có CSV. Chạy lại `3a_risk.py` và `3b_mom_growth.py` để cập nhật.
