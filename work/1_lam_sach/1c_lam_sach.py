from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================================
# FILE 1c: LÀM SẠCH VÀ LƯU DỮ LIỆU
# Mục tiêu: Xử lý NULL, thêm cột tên nước, lưu Parquet lên HDFS
# ============================================================

spark = SparkSession.builder \
    .appName("1c_Lam_Sach") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("FILE 1c: LÀM SẠCH DỮ LIỆU")
print("=" * 60)

df_raw = spark.read.csv(
    "hdfs://namenode:8020/data/raw/asean_data.csv",
    header=True,
    inferSchema=True
)
total_raw = df_raw.count()
print(f"\n📥 Dữ liệu thô: {total_raw:,} bản ghi")

# ── BƯỚC 1: Lọc cấp quốc gia ─────────────────────────────────
df = df_raw.filter(F.length(F.col("location_key")) == 2)
b1 = df.count()
print(f"\n Bước 1 – Lọc cấp quốc gia   : {b1:>8,} bản ghi (loại {total_raw-b1:,})")

# ── BƯỚC 2: Xóa NULL date / location_key ─────────────────────
df = df.dropna(subset=["date", "location_key"])
b2 = df.count()
print(f" Bước 2 – Xóa NULL date/key   : {b2:>8,} bản ghi (loại {b1-b2:,})")

# ── BƯỚC 3: Xóa trùng lặp ────────────────────────────────────
df = df.dropDuplicates(["date", "location_key"])
b3 = df.count()
print(f" Bước 3 – Xóa trùng lặp       : {b3:>8,} bản ghi (loại {b2-b3:,})")

# ── BƯỚC 4: Điền 0 vào NULL cột số ───────────────────────────
numeric_cols = [
    "new_confirmed", "new_deceased", "new_recovered", "new_tested",
    "cumulative_confirmed", "cumulative_deceased",
    "cumulative_recovered", "cumulative_tested"
]
df = df.fillna(0, subset=numeric_cols)
print(f" Bước 4 – Điền 0 vào NULL số  : {df.count():>8,} bản ghi")

# ── BƯỚC 5: Chuẩn hóa kiểu dữ liệu ──────────────────────────
df = df.withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd"))
print(f" Bước 5 – Chuẩn hóa kiểu date: OK")

# ── BƯỚC 6: Thêm tên quốc gia ────────────────────────────────
country_map = {
    "BN": "Brunei",      "ID": "Indonesia",  "KH": "Campuchia",
    "LA": "Lào",         "MM": "Myanmar",    "MY": "Malaysia",
    "PH": "Philippines", "SG": "Singapore",  "TH": "Thái Lan",
    "TL": "Timor-Leste", "VN": "Việt Nam"
}
mapping_expr = F.create_map([F.lit(x) for pair in country_map.items() for x in pair])
df = df.withColumn("country_name", mapping_expr[F.col("location_key")])
print(f" Bước 6 – Thêm country_name   : OK")

# ── BƯỚC 7: Thêm cột thời gian ───────────────────────────────
df = df \
    .withColumn("year",  F.year("date")) \
    .withColumn("month", F.month("date")) \
    .withColumn("quarter",
        F.when(F.month("date") <= 3, "Q1")
         .when(F.month("date") <= 6, "Q2")
         .when(F.month("date") <= 9, "Q3")
         .otherwise("Q4")
    )
print(f" Bước 7 – Thêm year/month/quarter: OK")

# ── KẾT QUẢ ──────────────────────────────────────────────────
final = df.count()
print(f"\n{'='*50}")
print(f" Kết quả cuối : {final:,} bản ghi")
print(f"   Tổng loại bỏ: {total_raw - final:,} ({round((total_raw-final)/total_raw*100,1)}%)")

print("\n--- Schema sau làm sạch ---")
df.printSchema()

print("--- 10 dòng mẫu ---")
df.show(10, truncate=False)

print("--- Kiểm tra NULL sau làm sạch ---")
for c in ["date","location_key","new_confirmed","new_deceased"]:
    n = df.filter(F.col(c).isNull()).count()
    print(f"  {c:<25}: {n} NULL {'✅' if n==0 else '⚠️'}")

# ── LƯU LÊN HDFS ─────────────────────────────────────────────
print("\n Lưu dữ liệu sạch lên HDFS (Parquet)...")
df.write.mode("overwrite").parquet(
    "hdfs://namenode:8020/data/processed/1_cleaned_data"
)
print(" Đã lưu: hdfs://namenode:8020/data/processed/1_cleaned_data")

spark.stop()
print("\n HOÀN TẤT FILE 1c!")
