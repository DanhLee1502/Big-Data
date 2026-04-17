from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================================
# FILE 1a: ĐỌC VÀ XEM DỮ LIỆU THÔ
# Mục tiêu: Hiểu cấu trúc file CSV đầu vào trên HDFS
# ============================================================

spark = SparkSession.builder \
    .appName("1a_Doc_Raw") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("FILE 1a: ĐỌC DỮ LIỆU THÔ TỪ HDFS")
print("=" * 60)

df = spark.read.csv(
    "hdfs://namenode:8020/data/raw/asean_data.csv",
    header=True,
    inferSchema=True
)

print(f"\n📊 Tổng số bản ghi : {df.count():,}")
print(f"📋 Số cột          : {len(df.columns)}")
print(f"📝 Tên các cột     : {df.columns}")

print("\n--- SCHEMA ---")
df.printSchema()

print("--- 10 DÒNG ĐẦU ---")
df.show(10, truncate=False)

print("--- THỐNG KÊ MÔ TẢ ---")
df.describe().show(truncate=False)

print("\n--- KHOẢNG THỜI GIAN ---")
df.agg(
    F.min("date").alias("Ngay_bat_dau"),
    F.max("date").alias("Ngay_ket_thuc")
).show()

print("\n--- TẤT CẢ MÃ QUỐC GIA (2 ký tự) ---")
df.filter(F.length(F.col("location_key")) == 2) \
  .select("location_key").distinct() \
  .orderBy("location_key").show()

spark.stop()
print("✅ HOÀN TẤT FILE 1a!")
