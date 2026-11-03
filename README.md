# Big Data – Phân tích diễn biến dịch tễ COVID-19 khu vực Đông Nam Á

Pipeline xử lý dữ liệu lớn bằng **PySpark + HDFS** trên dữ liệu
[COVID-19 Open Data](https://health.google.com/covid-19/open-data/) của Google,
lọc cho 11 nước ASEAN (VN, TH, MY, SG, ID, PH, BN, KH, LA, MM, TL).

## Cấu trúc thư mục

```
Big-Data/
├── README.md
├── data/
│   └── raw/
│       ├── asean_data.csv          # Dữ liệu dịch tễ thô đã lọc ASEAN
│       └── index.csv               # Bảng tra location_key → tên quốc gia/vùng
├── docs/
│   ├── bao_cao/
│   │   ├── final_report.pdf        # Báo cáo cuối kỳ
│   │   ├── pipeline.docx           # Mô tả pipeline
│   │   └── phan_tich_dich_te_covid19_dong_nam_a.xlsx
│   ├── so_do/
│   │   ├── data_architecture.png   # Kiến trúc dữ liệu
│   │   └── mind_map.png
│   └── trinh_bay/
│       └── covid_asean_slides.html # Slide thuyết trình
└── work/                           # Mount vào Jupyter: /home/jovyan/work
    ├── 0_thu_thap/
    │   └── get_asean_data.py       # Tải & lọc dữ liệu ASEAN → data/raw/
    ├── 1_lam_sach/
    │   ├── 1a_doc_raw.py           # Đọc, xem cấu trúc dữ liệu thô
    │   ├── 1b_kiem_tra_null.py     # Kiểm tra NULL, trùng lặp
    │   └── 1c_lam_sach.py          # Làm sạch → /data/processed/1_cleaned_data
    ├── 2_tong_hop/
    │   ├── 2a_theo_thang.py        # Quốc gia × Năm × Tháng
    │   ├── 2b_theo_quy.py          # Quốc gia × Năm × Quý
    │   └── 2c_theo_nam.py          # Quốc gia × Năm
    ├── 3_datamart/
    │   ├── 3a_risk.py              # DM1 – Mức độ nguy hiểm
    │   ├── 3b_mom_growth.py        # DM2 – Tăng trưởng MoM (LAG)
    │   ├── 3c_ranking.py           # DM3 – Xếp hạng tâm dịch (RANK)
    │   ├── 3d_yearly_summary.py    # DM4 – Tổng kết theo năm
    │   └── 3e_peak.py              # DM5 – Đỉnh dịch (ROW_NUMBER)
    ├── 4_xuat_hdfs/
    │   └── 4_luu_hdfs_kiem_tra.py  # Xuất CSV lên HDFS /data/export & kiểm tra
    └── output/                     # Kết quả CSV của các data mart
        ├── datamart_1_risk/
        ├── datamart_2_growth/
        ├── datamart_3_top3/
        ├── datamart_4_yearly/
        └── datamart_5_peak/
```

## Thứ tự chạy

| Bước | Script | Đầu vào | Đầu ra |
|---|---|---|---|
| 0 | `0_thu_thap/get_asean_data.py` | Google COVID-19 Open Data | `data/raw/asean_data.csv` |
| – | Đưa file lên HDFS | `data/raw/asean_data.csv` | `hdfs:///data/raw/asean_data.csv` |
| 1 | `1_lam_sach/1a → 1b → 1c` | `/data/raw` | `/data/processed/1_cleaned_data` |
| 2 | `2_tong_hop/2a, 2b, 2c` | `1_cleaned_data` | `2_monthly / 2_quarterly / 2_yearly_aggregated` |
| 3 | `3_datamart/3a → 3e` | `2_monthly`, `2_yearly` | `/data/processed/datamart_*` + `work/output/` |
| 4 | `4_xuat_hdfs/4_luu_hdfs_kiem_tra.py` | `/data/processed/*` | `/data/export/*_csv` |

Đưa dữ liệu thô lên HDFS (ví dụ):

```bash
hdfs dfs -mkdir -p /data/raw
hdfs dfs -put data/raw/asean_data.csv /data/raw/
```

Chạy một bước trong container Spark/Jupyter:

```bash
spark-submit /home/jovyan/work/1_lam_sach/1c_lam_sach.py
```

> Các script đọc/ghi HDFS qua `hdfs://namenode:8020` và ghi CSV ra đường dẫn tuyệt đối
> `/home/jovyan/work/output`, nên vị trí thư mục con trong `work/` không ảnh hưởng tới kết quả.
