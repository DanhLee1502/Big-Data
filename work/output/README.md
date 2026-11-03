# Kết quả CSV của các Data Mart

Đầu ra local của các script trong `work/3_datamart/` (ghi vào `/home/jovyan/work/output` trong container).
Mỗi thư mục gồm một file `part-00000-*.csv` (có header) và file đánh dấu `_SUCCESS` do Spark tạo.

| Thư mục | Script tạo ra |
|---|---|
| `datamart_1_risk/` | `3_datamart/3a_risk.py` |
| `datamart_2_growth/` | `3_datamart/3b_mom_growth.py` |
| `datamart_3_top3/` | `3_datamart/3c_ranking.py` |
| `datamart_4_yearly/` | `3_datamart/3d_yearly_summary.py` |
| `datamart_5_peak/` | `3_datamart/3e_peak.py` |
