# Bước 3 – Phân tích (Data Mart)

Mỗi script tạo một data mart, lưu **Parquet trên HDFS** và **CSV local** trong `work/output/`.

| File | Data mart | Câu hỏi phân tích | Kỹ thuật | Đầu vào |
|---|---|---|---|---|
| `3a_risk.py` | DM1 – Mức độ nguy hiểm | Mỗi tháng, mỗi nước ở mức lây lan và tử vong nào? | `when / otherwise` | `2_monthly_aggregated` |
| `3b_mom_growth.py` | DM2 – Tăng trưởng MoM | Dịch đang bùng phát hay suy giảm so với tháng trước? | Window `LAG` | `2_monthly_aggregated` |
| `3c_ranking.py` | DM3 – Xếp hạng tâm dịch | Nước nào là tâm dịch đứng đầu ĐNÁ mỗi tháng? | Window `RANK` | `2_monthly_aggregated` |
| `3d_yearly_summary.py` | DM4 – Tổng kết năm | Bức tranh từng năm 2020 – 2021 – 2022 ra sao? | `groupBy` | `2_yearly_aggregated` |
| `3e_peak.py` | DM5 – Đỉnh dịch | Đỉnh dịch của từng nước rơi vào tháng nào? | Window `ROW_NUMBER` | `2_monthly_aggregated` |

## Chi tiết

**`3a_risk.py`** – thêm 2 cột phân loại:
- `muc_do_lay_lan`: ≥ 500.000 ca → *Cực kỳ nghiêm trọng*; ≥ 100.000 → *Rất cao*; ≥ 10.000 → *Cao*; ≥ 1.000 → *Trung bình*; còn lại → *Thấp*
- `muc_do_tu_vong`: CFR ≥ 3% → *Nguy hiểm*; 1–3% → *Cao*; 0,5–1% → *Trung bình*; < 0,5% → *Thấp*

In thêm: phân phối các mức, các tháng > 500.000 ca, CFR trung bình từng nước, tháng nguy hiểm nhất từng nước.

**`3b_mom_growth.py`** – thêm `ca_mac_thang_truoc` (LAG 1 tháng), `toc_do_tang_truong_pct` và nhãn `xu_huong_dich`: *Bùng phát cực mạnh* (> 100%), *Tăng nhanh* (> 20%), *Tăng nhẹ*, *Đi ngang*, *Giảm nhẹ* (0 đến −20%), *Suy giảm mạnh* (< −20%). In top 10 tháng bùng phát mạnh nhất và diễn biến MoM của Việt Nam, Indonesia.

**`3c_ranking.py`** – xếp hạng các nước theo ca mắc trong từng tháng (`xep_hang`), lấy Top 3; đếm số tháng mỗi nước đứng hạng 1 và lọt Top 3; in bảng xếp hạng đầy đủ tháng 8/2021. HDFS lưu bảng xếp hạng đầy đủ, CSV chỉ lưu Top 3.

**`3d_yearly_summary.py`** – bảng quốc gia × năm, tổng toàn khu vực theo năm, nước dẫn đầu mỗi năm, tổng kết toàn giai đoạn và so sánh 2020 → 2022 của VN, ID, MY, TH, PH.

**`3e_peak.py`** – tháng đỉnh dịch của từng nước, Top 3 tháng cao điểm, các tháng vượt mốc báo động 100.000 ca, số tháng có dịch từng nước.

## Đầu ra

| Script | HDFS (Parquet) | CSV local |
|---|---|---|
| `3a_risk.py` | `/data/processed/datamart_1_risk` | `work/output/datamart_1_risk/` |
| `3b_mom_growth.py` | `/data/processed/datamart_2_mom_growth` | `work/output/datamart_2_growth/` |
| `3c_ranking.py` | `/data/processed/datamart_3_ranking` | `work/output/datamart_3_top3/` |
| `3d_yearly_summary.py` | `/data/processed/datamart_4_yearly_summary` | `work/output/datamart_4_yearly/` |
| `3e_peak.py` | `/data/processed/datamart_5_peak` | `work/output/datamart_5_peak/` |
