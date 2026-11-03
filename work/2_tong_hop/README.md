# Bước 2 – Tổng hợp theo thời gian

Đầu vào: `/data/processed/1_cleaned_data` (kết quả của `1c_lam_sach.py`)

| File | Mức gom nhóm | Mô tả | Đầu ra |
|---|---|---|---|
| `2a_theo_thang.py` | Quốc gia × Năm × Tháng | Tổng ca mắc mới, tử vong, phục hồi, xét nghiệm; lũy kế cao nhất; số ngày ghi nhận; tính CFR % và ca mắc trung bình/ngày. In diễn biến theo tháng của Việt Nam và Indonesia. | `/data/processed/2_monthly_aggregated` |
| `2b_theo_quy.py` | Quốc gia × Năm × Quý | Tổng ca mắc, tử vong, phục hồi, lũy kế, CFR %. In tổng toàn khu vực theo quý và các quý có nhiều ca nhất. | `/data/processed/2_quarterly_aggregated` |
| `2c_theo_nam.py` | Quốc gia × Năm | Tổng ca mắc, tử vong, phục hồi, lũy kế, CFR %. In tổng khu vực theo năm và tổng kết toàn giai đoạn từng nước. | `/data/processed/2_yearly_aggregated` |

## Chỉ số tính thêm

| Cột | Công thức |
|---|---|
| `ty_le_tu_vong_pct` (CFR %) | `tong_ca_tu_vong / tong_ca_mac_moi × 100` (bằng 0 nếu không có ca mắc) |
| `ca_mac_tb_ngay` | `tong_ca_mac_moi / so_ngay_ghi_nhan` (chỉ có ở `2a`) |

> Bảng tháng (`2a`) là đầu vào của DM1, DM2, DM3, DM5; bảng năm (`2c`) là đầu vào của DM4.
