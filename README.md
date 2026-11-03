# 🦠 Xử lý và phân tích dữ liệu COVID-19 khu vực Đông Nam Á

Đồ án cuối kỳ học phần **Dữ liệu lớn (858017)** – Khoa Toán - Ứng dụng, Trường Đại học Sài Gòn.

| | |
|---|---|
| **Sinh viên** | Lê Thành Danh (3123580005) · Nguyễn Đăng Tiến (3123580050) |
| **Lớp** | DDU1231 |
| **Giảng viên hướng dẫn** | Vũ Ngọc Thanh Sang |
| **Công nghệ** | Docker Compose · Hadoop HDFS 3.2.1 · Apache Spark (PySpark) · Jupyter · Pandas |

## Giới thiệu

Dự án xây dựng một **pipeline dữ liệu lớn hoàn chỉnh** chạy trên cụm Hadoop/Spark được container hóa bằng Docker, nhằm theo dõi diễn biến dịch COVID-19 tại **11 quốc gia Đông Nam Á**:
Việt Nam, Thái Lan, Malaysia, Singapore, Indonesia, Philippines, Brunei, Campuchia, Lào, Myanmar và Timor-Leste.

- **Nguồn dữ liệu:** [Google COVID-19 Open Data](https://health.google.com/covid-19/open-data/) – file `epidemiology.csv` (toàn cầu).
- **Phạm vi:** 01/01/2020 → 16/09/2022, gồm **394.165 bản ghi** ở 3 cấp (quốc gia, tỉnh/bang, quận/huyện). Phân tích chỉ dùng **cấp quốc gia** (10.864 bản ghi).
- **Kết quả:** 5 data mart trả lời các câu hỏi:
  1. Mỗi tháng, mỗi nước đang ở **mức độ nguy hiểm** nào (lây lan và tử vong)?
  2. Dịch đang **bùng phát hay suy giảm** so với tháng trước (MoM)?
  3. Nước nào là **tâm dịch** đứng đầu Đông Nam Á mỗi tháng?
  4. Bức tranh **tổng kết theo năm** 2020 – 2021 – 2022?
  5. **Đỉnh dịch** của từng nước rơi vào tháng nào?

## Kiến trúc pipeline

![Kiến trúc dữ liệu](docs/so_do/data_architecture.png)

```
Google COVID-19 Open Data (epidemiology.csv)
        │  0_thu_thap/get_asean_data.py  (Pandas, đọc theo chunk, lọc 11 nước)
        ▼
data/raw/asean_data.csv ──(docker cp + hdfs dfs -put)──▶ HDFS /data/raw/
        │  1_lam_sach/1a → 1b → 1c
        ▼
HDFS /data/processed/1_cleaned_data            (Parquet)
        │  2_tong_hop/2a, 2b, 2c
        ▼
HDFS /data/processed/2_{monthly,quarterly,yearly}_aggregated
        │  3_datamart/3a → 3e
        ▼
HDFS /data/processed/datamart_*   +   work/output/datamart_*/  (CSV)
        │  4_xuat_hdfs/4_luu_hdfs_kiem_tra.py
        ▼
HDFS /data/export/*_csv
```

## Cấu trúc thư mục

```
Big-Data/
├── README.md
├── docker-compose.yml              # Cụm namenode + datanode + spark-jupyter
├── data/
│   └── raw/
│       ├── asean_data.csv
│       └── index.csv
├── docs/
│   ├── bao_cao/
│   │   ├── final_report.pdf
│   │   ├── pipeline.docx
│   │   └── phan_tich_dich_te_covid19_dong_nam_a.xlsx
│   ├── so_do/
│   │   ├── data_architecture.png
│   │   └── mind_map.png
│   └── trinh_bay/
│       └── covid_asean_slides.html
└── work/                           # Được mount vào container: /home/jovyan/work
    ├── 0_thu_thap/
    ├── 1_lam_sach/
    ├── 2_tong_hop/
    ├── 3_datamart/
    ├── 4_xuat_hdfs/
    └── output/
```

## Giải thích từng file

### Thư mục gốc

| File | Mô tả |
|---|---|
| `README.md` | Tài liệu giới thiệu dự án (file này). |
| `docker-compose.yml` | Khởi tạo 3 container: **namenode** (HDFS master, Web UI cổng `9870`), **datanode** (lưu block dữ liệu), **spark-jupyter** (PySpark + JupyterLab, cổng `8888`). Thư mục `work/` được bind-mount vào `/home/jovyan/work` để code sửa trên máy chạy ngay trong container. |

### `data/raw/` – Dữ liệu đầu vào

| File | Mô tả |
|---|---|
| `asean_data.csv` | Dữ liệu dịch tễ hằng ngày của 11 nước ĐNÁ (~16 MB, 394.165 dòng). 10 cột: `date`, `location_key`, `new_confirmed`, `new_deceased`, `new_recovered`, `new_tested`, `cumulative_confirmed`, `cumulative_deceased`, `cumulative_recovered`, `cumulative_tested`. `location_key` có 3 cấp: `VN` (quốc gia), `VN_44` (tỉnh), `VN_44_704` (huyện). |
| `index.csv` | Bảng tra cứu mã vùng toàn cầu của Google Open Data (22.963 dòng): ánh xạ `location_key` → tên quốc gia/tỉnh/huyện, mã ISO, cấp tổng hợp (`aggregation_level`). |

### `docs/` – Tài liệu

| File | Mô tả |
|---|---|
| `bao_cao/final_report.pdf` | Báo cáo cuối kỳ (66 trang): cơ sở lý thuyết Docker/Hadoop/Spark, cấu hình môi trường, quy trình xử lý từng bước, kết quả phân tích và kết luận. |
| `bao_cao/pipeline.docx` | Tóm tắt pipeline: nguồn dữ liệu, các bước 1–3, tên script và nơi lưu trữ trên HDFS. |
| `bao_cao/phan_tich_dich_te_covid19_dong_nam_a.xlsx` | Bảng tổng hợp theo quốc gia × năm: tổng ca mắc, tử vong, CFR %, ca mắc trung bình/ngày, mức độ rủi ro và biến thể chủ đạo (dự đoán). |
| `so_do/data_architecture.png` | Sơ đồ kiến trúc dữ liệu: nguồn → HDFS raw → processed → data mart → export. |
| `so_do/mind_map.png` | Sơ đồ tư duy tổng quan đồ án. |
| `trinh_bay/covid_asean_slides.html` | Slide thuyết trình dạng web (mở trực tiếp bằng trình duyệt). |

### `work/0_thu_thap/` – Thu thập dữ liệu

| File | Mô tả |
|---|---|
| `get_asean_data.py` | Đọc `epidemiology.csv` từ Google Cloud Storage theo từng khối 100.000 dòng (tránh tràn RAM), giữ các dòng có `location_key` bắt đầu bằng mã 11 nước ĐNÁ (kể cả cấp tỉnh/huyện), ghi nối vào `data/raw/asean_data.csv`. Chạy bằng Python + Pandas trên máy, **không** cần Spark. |

### `work/1_lam_sach/` – Khám phá & làm sạch

| File | Mô tả | Đầu ra |
|---|---|---|
| `1a_doc_raw.py` | Đọc CSV thô từ HDFS, in số dòng/cột, schema, 10 dòng đầu, thống kê mô tả, khoảng thời gian và danh sách mã quốc gia. | Chỉ in ra màn hình |
| `1b_kiem_tra_null.py` | Đánh giá chất lượng dữ liệu: đếm bản ghi theo 3 cấp (quốc gia/tỉnh/huyện), tỷ lệ NULL từng cột, bản ghi trùng `(date, location_key)`, thống kê tổng ca mắc/tử vong từng nước. | Chỉ in ra màn hình |
| `1c_lam_sach.py` | 7 bước làm sạch: (1) lọc cấp quốc gia, (2) bỏ NULL ở `date`/`location_key`, (3) bỏ trùng lặp, (4) điền 0 cho NULL ở cột số, (5) chuyển `date` sang kiểu Date, (6) thêm `country_name` tiếng Việt, (7) thêm `year`, `month`, `quarter`. | `/data/processed/1_cleaned_data` (Parquet) |

### `work/2_tong_hop/` – Tổng hợp theo thời gian

| File | Mô tả | Đầu ra |
|---|---|---|
| `2a_theo_thang.py` | Gom nhóm **quốc gia × năm × tháng**: tổng ca mắc mới, tử vong, phục hồi, xét nghiệm; lũy kế cao nhất; số ngày ghi nhận; tính **CFR %** (`tử vong / ca mắc × 100`) và **ca mắc trung bình/ngày**. Đây là bảng nền cho phần lớn data mart. | `/data/processed/2_monthly_aggregated` |
| `2b_theo_quy.py` | Gom nhóm **quốc gia × năm × quý**, tính CFR; in tổng toàn khu vực theo quý và các quý có nhiều ca nhất. | `/data/processed/2_quarterly_aggregated` |
| `2c_theo_nam.py` | Gom nhóm **quốc gia × năm**, tính CFR; in tổng khu vực theo năm và tổng kết toàn giai đoạn từng nước. | `/data/processed/2_yearly_aggregated` |

### `work/3_datamart/` – Phân tích (Data Mart)

| File | Câu hỏi phân tích | Kỹ thuật | Đầu ra |
|---|---|---|---|
| `3a_risk.py` | **DM1 – Mức độ nguy hiểm.** Phân loại mỗi tháng của mỗi nước theo *mức độ lây lan* (5 mức, từ < 1.000 đến ≥ 500.000 ca/tháng) và *mức độ tử vong* (4 mức theo CFR: < 0,5% / 0,5–1% / 1–3% / ≥ 3%). | `when/otherwise` | HDFS `datamart_1_risk`, CSV `output/datamart_1_risk/` |
| `3b_mom_growth.py` | **DM2 – Tăng trưởng MoM.** So sánh ca mắc với tháng trước, tính % tăng trưởng và gán xu hướng: *Bùng phát cực mạnh (>100%)*, *Tăng nhanh*, *Tăng nhẹ*, *Đi ngang*, *Giảm nhẹ*, *Suy giảm mạnh*. | Window `LAG` | HDFS `datamart_2_mom_growth`, CSV `output/datamart_2_growth/` |
| `3c_ranking.py` | **DM3 – Xếp hạng tâm dịch.** Xếp hạng các nước theo ca mắc trong từng tháng; lấy Top 3; đếm số tháng mỗi nước đứng hạng 1 / lọt Top 3. | Window `RANK` | HDFS `datamart_3_ranking`, CSV `output/datamart_3_top3/` (chỉ Top 3) |
| `3d_yearly_summary.py` | **DM4 – Tổng kết theo năm.** Bảng quốc gia × năm, tổng khu vực, nước dẫn đầu mỗi năm, so sánh 2020 → 2021 → 2022 của VN, ID, MY, TH, PH. | `groupBy` | HDFS `datamart_4_yearly_summary`, CSV `output/datamart_4_yearly/` |
| `3e_peak.py` | **DM5 – Đỉnh dịch.** Tìm tháng có nhiều ca nhất của mỗi nước, Top 3 tháng cao điểm, các tháng vượt mốc báo động 100.000 ca. | Window `ROW_NUMBER` | HDFS `datamart_5_peak`, CSV `output/datamart_5_peak/` |

### `work/4_xuat_hdfs/` – Xuất & kiểm tra

| File | Mô tả | Đầu ra |
|---|---|---|
| `4_luu_hdfs_kiem_tra.py` | Đọc lại 4 bảng tổng hợp và 5 data mart dạng Parquet, xuất từng bảng ra CSV trên HDFS, kiểm tra sự tồn tại của mọi thư mục `raw/processed/export`, in sơ đồ cây HDFS và bảng tổng kết số bản ghi từng giai đoạn. | `/data/export/*_csv` |

### `work/output/` – Kết quả CSV

Mỗi thư mục con là đầu ra Spark (`coalesce(1)`), gồm một file `part-00000-*.csv` và file đánh dấu `_SUCCESS`.

| Thư mục | Nội dung | Số dòng |
|---|---|---|
| `datamart_1_risk/` | Kết quả DM1 – xem lưu ý bên dưới | 363 |
| `datamart_2_growth/` | Kết quả DM2 – **chưa có file CSV** | – |
| `datamart_3_top3/` | Top 3 tâm dịch mỗi tháng, cột `xep_hang` | 99 |
| `datamart_4_yearly/` | Tổng kết quốc gia × năm | 33 |
| `datamart_5_peak/` | Tháng đỉnh dịch của 11 nước | 11 |

Các cột chính trong file kết quả:

| Cột | Ý nghĩa |
|---|---|
| `tong_ca_mac_moi` / `tong_ca_tu_vong` / `tong_ca_phuc_hoi` / `tong_xet_nghiem` | Tổng ca mắc mới / tử vong / phục hồi / xét nghiệm trong kỳ |
| `luy_ke_ca_mac` / `luy_ke_tu_vong` | Giá trị lũy kế cao nhất tính đến cuối kỳ |
| `ty_le_tu_vong_pct` | CFR (%) = tử vong / ca mắc × 100 |
| `ca_mac_tb_ngay` | Ca mắc trung bình mỗi ngày trong tháng |
| `ca_mac_thang_truoc`, `toc_do_tang_truong_pct`, `xu_huong_dich` | Số ca tháng trước, % tăng trưởng MoM, nhãn xu hướng |
| `muc_do_lay_lan`, `muc_do_tu_vong` | Nhãn phân loại rủi ro (DM1) |
| `xep_hang` | Thứ hạng ca mắc trong tháng (DM3) |

> ⚠️ **Lưu ý về `output/`:** file CSV trong `datamart_1_risk/` hiện chứa các cột MoM của 3b (`ca_mac_thang_truoc`, `toc_do_tang_truong_pct`, `xu_huong_dich`) thay vì cột phân loại `muc_do_lay_lan`, `muc_do_tu_vong` của 3a, còn `datamart_2_growth/` không có CSV. Chạy lại `3a_risk.py` và `3b_mom_growth.py` để có kết quả đúng.

## Hướng dẫn chạy

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

## Tài liệu tham khảo

- Google. *COVID-19 Open Data Repository.* https://health.google.com/covid-19/open-data/
- Apache Software Foundation. *Apache Spark 3.5 Programming Guide.*
- Chi tiết lý thuyết và phân tích: [`docs/bao_cao/final_report.pdf`](docs/bao_cao/final_report.pdf)
