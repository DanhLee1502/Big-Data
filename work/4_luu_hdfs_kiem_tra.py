from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

# ============================================================
# FILE CUỐI: LƯU TẤT CẢ KẾT QUẢ LÊN HDFS & KIỂM TRA
# Mục tiêu: Đảm bảo toàn bộ kết quả đã có trên HDFS
# ============================================================

spark = SparkSession.builder \
    .appName("Final_Upload_To_HDFS") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

HDFS_PROCESSED = "hdfs://namenode:8020/data/processed"
HDFS_EXPORT    = "hdfs://namenode:8020/data/export"
LOCAL_OUTPUT   = "/home/jovyan/work/output"

print("=" * 65)
print("FILE CUỐI: LƯU TOÀN BỘ KẾT QUẢ LÊN HDFS")
print("=" * 65)

# ── BƯỚC 1: ĐỌC LẠI VÀ LƯU TẤT CẢ DATA MARTS LÊN HDFS (CSV) ──
print("\n[1] Đọc từng Data Mart → Lưu CSV lên HDFS /data/export/...")
print("-" * 60)

datamarts = {
    "datamart_1_risk"        : "DM1 – Mức độ nguy hiểm",
    "datamart_2_mom_growth"  : "DM2 – Tốc độ tăng trưởng MoM",
    "datamart_3_ranking"     : "DM3 – Bảng xếp hạng tâm dịch",
    "datamart_4_yearly_summary": "DM4 – Tổng kết theo năm",
    "datamart_5_peak"        : "DM5 – Đỉnh dịch",
}

for folder, name in datamarts.items():
    try:
        df = spark.read.parquet(f"{HDFS_PROCESSED}/{folder}")
        count = df.count()
        # Lưu CSV lên HDFS
        df.coalesce(1).write.mode("overwrite") \
          .option("header", True) \
          .csv(f"{HDFS_EXPORT}/{folder}_csv")
        print(f"  ✅ {name:<35} ({count:,} bản ghi) → {HDFS_EXPORT}/{folder}_csv")
    except Exception as e:
        print(f"  ❌ {name}: LỖI - {e}")

# ── BƯỚC 2: LƯU DỮ LIỆU TỔNG HỢP LÊN HDFS (CSV) ────────────
print("\n[2] Lưu dữ liệu tổng hợp (monthly/quarterly/yearly) lên HDFS...")
print("-" * 60)

aggregated = {
    "1_cleaned_data"       : "Dữ liệu sạch",
    "2_monthly_aggregated" : "Tổng hợp theo tháng",
    "2_quarterly_aggregated": "Tổng hợp theo quý",
    "2_yearly_aggregated"  : "Tổng hợp theo năm",
}

for folder, name in aggregated.items():
    try:
        df = spark.read.parquet(f"{HDFS_PROCESSED}/{folder}")
        count = df.count()
        df.coalesce(1).write.mode("overwrite") \
          .option("header", True) \
          .csv(f"{HDFS_EXPORT}/{folder}_csv")
        print(f"  ✅ {name:<30} ({count:,} bản ghi) → {HDFS_EXPORT}/{folder}_csv")
    except Exception as e:
        print(f"  ❌ {name}: LỖI - {e}")

# ── BƯỚC 3: KIỂM TRA TẤT CẢ FILE TRÊN HDFS ─────────────────
print("\n[3] Kiểm tra toàn bộ cấu trúc HDFS...")
print("-" * 60)

# Kiểm tra /data/raw
try:
    df_raw = spark.read.csv(
        "hdfs://namenode:8020/data/raw/asean_data.csv",
        header=True, inferSchema=True
    )
    print(f"  ✅ /data/raw/asean_data.csv       ({df_raw.count():,} bản ghi thô)")
except:
    print(f"  ❌ /data/raw/asean_data.csv       KHÔNG TÌM THẤY!")

# Kiểm tra /data/processed
for folder, name in {**aggregated, **datamarts}.items():
    try:
        df = spark.read.parquet(f"{HDFS_PROCESSED}/{folder}")
        print(f"  ✅ /data/processed/{folder:<35} ({df.count():,} bản ghi)")
    except:
        print(f"  ❌ /data/processed/{folder:<35} KHÔNG TÌM THẤY!")

# Kiểm tra /data/export
for folder in list(aggregated.keys()) + list(datamarts.keys()):
    try:
        df = spark.read.csv(
            f"{HDFS_EXPORT}/{folder}_csv",
            header=True, inferSchema=True
        )
        print(f"  ✅ /data/export/{folder}_csv ({df.count():,} bản ghi CSV)")
    except:
        print(f"  ❌ /data/export/{folder}_csv KHÔNG TÌM THẤY!")

# ── BƯỚC 4: IN SƠ ĐỒ CẤU TRÚC HDFS CUỐI CÙNG ───────────────
print("\n[4] Cấu trúc HDFS hoàn chỉnh sau khi xử lý:")
print("-" * 60)
print("""
  hdfs://namenode:8020/
  ├── data/
  │   ├── raw/
  │   │   └── asean_data.csv              ← Dữ liệu thô gốc
  │   │
  │   ├── processed/                      ← Dữ liệu đã xử lý (Parquet)
  │   │   ├── 1_cleaned_data/             ← Bước 1: Dữ liệu sạch
  │   │   ├── 2_monthly_aggregated/       ← Bước 2a: Theo tháng
  │   │   ├── 2_quarterly_aggregated/     ← Bước 2b: Theo quý
  │   │   ├── 2_yearly_aggregated/        ← Bước 2c: Theo năm
  │   │   ├── datamart_1_risk/            ← Bước 3a: Mức độ nguy hiểm
  │   │   ├── datamart_2_mom_growth/      ← Bước 3b: Tăng trưởng MoM
  │   │   ├── datamart_3_ranking/         ← Bước 3c: Xếp hạng tâm dịch
  │   │   ├── datamart_4_yearly_summary/  ← Bước 3d: Tổng kết năm
  │   │   └── datamart_5_peak/            ← Bước 3e: Đỉnh dịch
  │   │
  │   └── export/                         ← Xuất CSV lên HDFS
  │       ├── 1_cleaned_data_csv/
  │       ├── 2_monthly_aggregated_csv/
  │       ├── 2_quarterly_aggregated_csv/
  │       ├── 2_yearly_aggregated_csv/
  │       ├── datamart_1_risk_csv/
  │       ├── datamart_2_mom_growth_csv/
  │       ├── datamart_3_ranking_csv/
  │       ├── datamart_4_yearly_summary_csv/
  │       └── datamart_5_peak_csv/
""")

# ── BƯỚC 5: TỔNG KẾT SỐ LIỆU ────────────────────────────────
print("[5] Tổng kết toàn bộ quá trình xử lý:")
print("-" * 60)

try:
    raw_count     = spark.read.csv("hdfs://namenode:8020/data/raw/asean_data.csv", header=True).count()
    cleaned_count = spark.read.parquet(f"{HDFS_PROCESSED}/1_cleaned_data").count()
    monthly_count = spark.read.parquet(f"{HDFS_PROCESSED}/2_monthly_aggregated").count()
    yearly_count  = spark.read.parquet(f"{HDFS_PROCESSED}/2_yearly_aggregated").count()
    peak_count    = spark.read.parquet(f"{HDFS_PROCESSED}/datamart_5_peak").count()

    print(f"""
  Giai đoạn          Số bản ghi     Ghi chú
  ─────────────────  ─────────────  ──────────────────────────
  Dữ liệu thô        {raw_count:>12,}  394.165 bản ghi (3 cấp)
  Sau làm sạch       {cleaned_count:>12,}  Chỉ cấp quốc gia
  Tổng hợp tháng     {monthly_count:>12,}  11 nước × nhiều tháng
  Tổng hợp năm       {yearly_count:>12,}  11 nước × 3 năm
  Đỉnh dịch          {peak_count:>12,}  1 đỉnh / quốc gia
    """)
except Exception as e:
    print(f"  Lỗi khi tổng kết: {e}")

print("=" * 65)
print("HOÀN TẤT! TOÀN BỘ KẾT QUẢ ĐÃ ĐƯỢC LƯU LÊN HDFS!")
print("=" * 65)

spark.stop()
