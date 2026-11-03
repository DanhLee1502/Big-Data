# work – Mã nguồn pipeline PySpark

Thư mục này được mount vào container Spark tại `/home/jovyan/work` (xem `docker-compose.yml` ở thư mục gốc).

| Thư mục | Giai đoạn |
|---|---|
| [`0_thu_thap/`](0_thu_thap/) | Bước 0 – Tải và lọc dữ liệu ASEAN |
| [`1_lam_sach/`](1_lam_sach/) | Bước 1 – Khám phá và làm sạch |
| [`2_tong_hop/`](2_tong_hop/) | Bước 2 – Tổng hợp theo tháng / quý / năm |
| [`3_datamart/`](3_datamart/) | Bước 3 – 5 data mart phân tích |
| [`4_xuat_hdfs/`](4_xuat_hdfs/) | Bước 4 – Xuất CSV lên HDFS và kiểm tra |
| [`output/`](output/) | Kết quả CSV của các data mart |

## Hướng dẫn chạy pipeline

Các lệnh ở bước 1–3 chạy từ **thư mục gốc của repo**.

**1. Khởi động cụm**

```bash
docker-compose up -d
docker ps        # namenode, datanode, spark-jupyter
```

- HDFS Web UI: http://localhost:9870
- JupyterLab: http://localhost:8888 (lấy token bằng `docker logs spark-jupyter`)

**2. Thu thập dữ liệu** (bỏ qua nếu dùng sẵn `data/raw/asean_data.csv`)

```bash
pip install pandas
python work/0_thu_thap/get_asean_data.py
```

**3. Đưa dữ liệu lên HDFS**

```bash
docker cp data/raw/asean_data.csv namenode:/tmp/asean_data.csv
docker exec namenode hdfs dfs -mkdir -p /data/raw /data/processed /data/export
docker exec namenode hdfs dfs -put -f /tmp/asean_data.csv /data/raw/
docker exec namenode hdfs dfs -ls /data/raw/
```

**4. Chạy pipeline theo thứ tự** (trong container Spark)

```bash
docker exec -it spark-jupyter bash
cd /home/jovyan/work

python 1_lam_sach/1a_doc_raw.py
python 1_lam_sach/1b_kiem_tra_null.py
python 1_lam_sach/1c_lam_sach.py

python 2_tong_hop/2a_theo_thang.py
python 2_tong_hop/2b_theo_quy.py
python 2_tong_hop/2c_theo_nam.py

python 3_datamart/3a_risk.py
python 3_datamart/3b_mom_growth.py
python 3_datamart/3c_ranking.py
python 3_datamart/3d_yearly_summary.py
python 3_datamart/3e_peak.py

python 4_xuat_hdfs/4_luu_hdfs_kiem_tra.py
```

> Bước 1a, 1b chỉ để khám phá dữ liệu và có thể bỏ qua. Bước 3 cần `2a` (và `2c` cho `3d`) chạy trước.

## Cấu trúc dữ liệu trên HDFS sau khi chạy

```
hdfs://namenode:8020/data/
├── raw/asean_data.csv
├── processed/                       (Parquet)
│   ├── 1_cleaned_data/
│   ├── 2_monthly_aggregated/
│   ├── 2_quarterly_aggregated/
│   ├── 2_yearly_aggregated/
│   ├── datamart_1_risk/
│   ├── datamart_2_mom_growth/
│   ├── datamart_3_ranking/
│   ├── datamart_4_yearly_summary/
│   └── datamart_5_peak/
└── export/                          (CSV, hậu tố _csv)
```
