# 🦠 Xử lý và phân tích dữ liệu COVID-19 khu vực Đông Nam Á

## Bài toán

Đại dịch COVID-19 tại Đông Nam Á diễn ra không đồng đều: mỗi quốc gia bùng phát vào những thời điểm khác nhau, đối mặt với những biến chủng khác nhau và có năng lực y tế rất khác nhau. Câu hỏi đặt ra là:

> Từ dữ liệu dịch tễ thực tế của **11 quốc gia Đông Nam Á** trong gần **3 năm (01/2020 – 09/2022)**, làm thế nào xây dựng một hệ thống xử lý dữ liệu lớn có thể tự động làm sạch, tổng hợp và phân tích đa chiều để rút ra những hiểu biết có giá trị về diễn biến đại dịch?

## Mục tiêu

- **Kỹ thuật:** xây dựng một pipeline ETL hoàn chỉnh trên **Hadoop HDFS** và **Apache Spark (PySpark)**, chạy trong môi trường **Docker**, áp dụng các kỹ thuật như Window Functions và lưu trữ dạng Parquet theo mô hình Data Lake.
- **Phân tích:** định lượng các xu hướng dịch tễ học của khu vực:
  - sự tiến triển của dịch qua 3 giai đoạn 2020 – 2021 – 2022;
  - khác biệt về mức độ nguy hiểm giữa biến chủng **Delta** và **Omicron**;
  - tác động của tiêm chủng thông qua sự thay đổi của tỷ lệ tử vong (CFR);
  - sự dịch chuyển vai trò "tâm dịch" giữa các quốc gia.

## Dữ liệu

Dữ liệu lấy từ [Google COVID-19 Open Data](https://health.google.com/covid-19/open-data/), nguồn tổng hợp và chuẩn hóa số liệu từ WHO, CDC và bộ y tế các nước. Từ file toàn cầu hơn 7 GB, dữ liệu được lọc còn **394.165 bản ghi** của Việt Nam, Thái Lan, Malaysia, Singapore, Indonesia, Philippines, Brunei, Campuchia, Lào, Myanmar và Timor-Leste, gồm số ca mắc, tử vong, phục hồi và xét nghiệm theo ngày.

## Phương pháp

Dữ liệu đi qua 4 giai đoạn: **làm sạch** (chuẩn hóa, loại trùng, xử lý giá trị thiếu) → **tổng hợp** theo tháng, quý, năm → **phân tích** qua 5 data mart → **lưu trữ và xuất** kết quả trên HDFS.

Năm data mart trả lời 5 câu hỏi:

1. **Mức độ nguy hiểm** – mỗi tháng, mỗi nước ở mức lây lan và tử vong nào?
2. **Tăng trưởng theo tháng** – dịch đang bùng phát hay suy giảm?
3. **Xếp hạng tâm dịch** – nước nào dẫn đầu khu vực mỗi tháng?
4. **Tổng kết theo năm** – bức tranh toàn cảnh từng năm.
5. **Đỉnh dịch** – tháng tồi tệ nhất của từng quốc gia.

![Kiến trúc dữ liệu](docs/so_do/data_architecture.png)

## Kết quả chính

| Năm | Giai đoạn | Tổng ca mắc | CFR |
|---|---|---|---|
| 2020 | Khởi phát | 1,53 triệu | 1,198% |
| 2021 | Khủng hoảng Delta | 13,4 triệu | 1,586% |
| 2022 | Omicron & tiêm chủng | 19,4 triệu | 0,348% |

- **Ba giai đoạn rõ rệt:** 2020 thăm dò, 2021 leo thang, 2022 chuyển sang trạng thái đặc hữu.
- **Vắc-xin có hiệu quả đo được:** năm 2022 số ca mắc cao kỷ lục nhưng CFR giảm hơn 4,5 lần so với 2021. Riêng Việt Nam, CFR giảm khoảng 93 lần từ 8/2021 đến 3/2022.
- **Delta nguy hiểm hơn Omicron nhiều lần:** CFR quý 3/2021 cao gấp khoảng 9 lần quý 1/2022.
- **Thời điểm bùng phát quyết định thiệt hại:** Việt Nam có nhiều ca mắc nhất (11,4 triệu) nhưng bùng phát vào thời kỳ Omicron. Indonesia bùng phát vào thời kỳ Delta, khi chưa có vắc-xin, nên có số ca tử vong cao nhất (157.849 ca, gần gấp 4 lần Việt Nam).
- **Tâm dịch dịch chuyển:** Indonesia giữ vai trò tâm dịch trong 2020 – 2021, Việt Nam trong 2022.
- **Năng lực y tế chênh lệch lớn:** CFR của Myanmar (3,15%) và Indonesia (2,47%) cao nhất khu vực. Singapore (0,09%) và Brunei (0,06%) gần như bằng 0.

## Hướng phát triển

- Tích hợp **Apache Kafka** và Spark Structured Streaming để xử lý dữ liệu thời gian thực.
- Dùng **Spark MLlib** (ARIMA, LSTM) để dự báo số ca mắc 3 – 6 tháng tới.
- Kết hợp dữ liệu tiêm chủng và kinh tế – xã hội để đánh giá tác động đa chiều của đại dịch.
- Triển khai trên cloud (AWS EMR, Google Dataproc) và xây dựng dashboard với Power BI hoặc Grafana.

## Tài liệu

- 📄 Báo cáo đầy đủ: [`docs/bao_cao/final_report.pdf`](docs/bao_cao/final_report.pdf)
- 🖥️ Slide thuyết trình: [`docs/trinh_bay/covid_asean_slides.html`](docs/trinh_bay/covid_asean_slides.html)
- ⚙️ Mã nguồn và hướng dẫn chạy: [`work/`](work/)
