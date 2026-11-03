# data/raw – Dữ liệu thô

Dữ liệu đầu vào của pipeline. File `asean_data.csv` được đưa lên HDFS tại `/data/raw/asean_data.csv` trước khi chạy các bước xử lý.

| File | Mô tả |
|---|---|
| `asean_data.csv` | Dữ liệu dịch tễ COVID-19 hằng ngày của 11 nước Đông Nam Á, lọc từ `epidemiology.csv` của [Google COVID-19 Open Data](https://health.google.com/covid-19/open-data/) bằng `work/0_thu_thap/get_asean_data.py`. ~16 MB, **394.165 dòng**, giai đoạn 01/01/2020 → 16/09/2022. |
| `index.csv` | Bảng tra cứu mã vùng toàn cầu của Google Open Data (22.963 dòng): ánh xạ `location_key` → tên quốc gia/tỉnh/huyện, mã ISO 3166, `aggregation_level` (0 = quốc gia, 1 = tỉnh, 2 = huyện). |

## Các cột của `asean_data.csv`

| Cột | Ý nghĩa |
|---|---|
| `date` | Ngày ghi nhận (`yyyy-MM-dd`) |
| `location_key` | Mã vùng: `VN` (quốc gia), `VN_44` (tỉnh/bang), `VN_44_704` (quận/huyện) |
| `new_confirmed` | Ca mắc mới trong ngày |
| `new_deceased` | Ca tử vong mới trong ngày |
| `new_recovered` | Ca phục hồi mới trong ngày |
| `new_tested` | Số xét nghiệm mới trong ngày |
| `cumulative_confirmed` | Lũy kế ca mắc |
| `cumulative_deceased` | Lũy kế tử vong |
| `cumulative_recovered` | Lũy kế phục hồi |
| `cumulative_tested` | Lũy kế xét nghiệm |

Mã 11 quốc gia: `BN` Brunei · `ID` Indonesia · `KH` Campuchia · `LA` Lào · `MM` Myanmar · `MY` Malaysia · `PH` Philippines · `SG` Singapore · `TH` Thái Lan · `TL` Timor-Leste · `VN` Việt Nam.

> Pipeline chỉ dùng các dòng cấp quốc gia (`location_key` dài 2 ký tự) – 10.864 dòng.
