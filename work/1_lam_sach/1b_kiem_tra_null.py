from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================================
# FILE 1b: KIỂM TRA CHẤT LƯỢNG DỮ LIỆU
# Mục tiêu: Thống kê NULL, cấp độ data, bản ghi trùng lặp
# ============================================================

spark = SparkSession.builder \
    .appName("1b_Kiem_Tra_Chat_Luong") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("FILE 1b: KIỂM TRA CHẤT LƯỢNG DỮ LIỆU")
print("=" * 60)

df = spark.read.csv(
    "hdfs://namenode:8020/data/raw/asean_data.csv",
    header=True,
    inferSchema=True
)
total = df.count()

# ── 1. KIỂM TRA CẤP ĐỘ DATA ─────────────────────────────────
print("\n[1] CẤP ĐỘ DỮ LIỆU (location_key)")
print("-" * 50)
cap_quoc_gia = df.filter(F.length("location_key") == 2).count()
cap_tinh     = df.filter(F.size(F.split("location_key","_")) == 2).count()
cap_huyen    = df.filter(F.size(F.split("location_key","_")) == 3).count()

print(f"  Cấp quốc gia (VN, ID, ...)       : {cap_quoc_gia:>8,} bản ghi")
print(f"  Cấp tỉnh/bang (VN_44, ID_AC, ...) : {cap_tinh:>8,} bản ghi")
print(f"  Cấp huyện (VN_44_704, ...)        : {cap_huyen:>8,} bản ghi")
print(f"  TỔNG CỘNG                         : {total:>8,} bản ghi")
print("\n  => Nhóm sẽ chỉ xử lý cấp QUỐC GIA cho phân tích tổng thể")

df_country = df.filter(F.length("location_key") == 2)
print(f"\n  Danh sách 11 quốc gia Đông Nam Á:")
df_country.select("location_key").distinct() \
           .orderBy("location_key").show()

# ── 2. KIỂM TRA GIÁ TRỊ NULL ────────────────────────────────
print("\n[2] THỐNG KÊ GIÁ TRỊ NULL (cấp quốc gia)")
print("-" * 50)
total_c = df_country.count()
print(f"\n  {'Tên cột':<28} {'Số NULL':>8}  {'Tỉ lệ':>7}  Trạng thái")
print("  " + "-" * 55)
for c in df_country.columns:
    n = df_country.filter(F.col(c).isNull()).count()
    pct = round(n / total_c * 100, 1)
    flag = " CÓ NULL" if n > 0 else " OK"
    print(f"  {c:<28} {n:>8,}  {pct:>6.1f}%  {flag}")

# ── 3. KIỂM TRA TRÙNG LẶP ───────────────────────────────────
print("\n[3] KIỂM TRA BẢN GHI TRÙNG LẶP")
print("-" * 50)
dup = total_c - df_country.dropDuplicates(["date","location_key"]).count()
print(f"  Số bản ghi trùng (date + location_key): {dup:,}")
if dup == 0:
    print("  Không có trùng lặp!")
else:
    print("  Cần xử lý trùng lặp!")
    df_country.groupBy("date","location_key").count() \
              .filter(F.col("count") > 1).show(10)

# ── 4. THỐNG KÊ SỐ LIỆU TỪNG NƯỚC ──────────────────────────
print("\n[4] THỐNG KÊ TỔNG QUÁT TỪNG NƯỚC")
print("-" * 50)
df_country.fillna(0).groupBy("location_key").agg(
    F.count("date").alias("so_ngay"),
    F.sum("new_confirmed").alias("tong_ca_mac"),
    F.sum("new_deceased").alias("tong_tu_vong"),
    F.max("cumulative_confirmed").alias("luy_ke_cao_nhat")
).orderBy(F.desc("tong_ca_mac")).show(truncate=False)

spark.stop()
print("HOÀN TẤT FILE 1b!")
