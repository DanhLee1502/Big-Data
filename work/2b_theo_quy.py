from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================================
# FILE 2b: TỔNG HỢP THEO QUÝ
# Mục tiêu: Gom nhóm số liệu theo Quốc gia × Năm × Quý
# ============================================================

spark = SparkSession.builder \
    .appName("2b_Tong_Hop_Quy") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("FILE 2b: TỔNG HỢP THEO QUÝ")
print("=" * 60)

df = spark.read.parquet("hdfs://namenode:8020/data/processed/1_cleaned_data")
print(f"\n Đọc dữ liệu sạch: {df.count():,} bản ghi")

# ── TỔNG HỢP ─────────────────────────────────────────────────
df_quarterly = df.groupBy(
    "location_key", "country_name", "year", "quarter"
).agg(
    F.sum("new_confirmed").alias("tong_ca_mac_moi"),
    F.sum("new_deceased").alias("tong_ca_tu_vong"),
    F.sum("new_recovered").alias("tong_ca_phuc_hoi"),
    F.max("cumulative_confirmed").alias("luy_ke_ca_mac"),
    F.count("date").alias("so_ngay")
).withColumn("ty_le_tu_vong_pct",
    F.when(F.col("tong_ca_mac_moi") > 0,
        F.round((F.col("tong_ca_tu_vong") / F.col("tong_ca_mac_moi")) * 100, 3)
    ).otherwise(F.lit(0.0))
).orderBy("location_key", "year", "quarter")

print(f"\nSố nhóm (quốc gia × quý): {df_quarterly.count():,}")

print("\n--- Tất cả quý của Việt Nam ---")
df_quarterly.filter(F.col("location_key") == "VN") \
    .select("year","quarter","tong_ca_mac_moi","tong_ca_tu_vong","ty_le_tu_vong_pct") \
    .orderBy("year","quarter").show(20, truncate=False)

print("\n--- Tổng hợp theo quý toàn Đông Nam Á ---")
df_quarterly.groupBy("year","quarter").agg(
    F.sum("tong_ca_mac_moi").alias("tong_ca_khu_vuc"),
    F.sum("tong_ca_tu_vong").alias("tong_tu_vong_khu_vuc")
).orderBy("year","quarter").show(20, truncate=False)

print("\n--- Quý có nhiều ca mắc nhất từng nước ---")
df_quarterly.orderBy(F.desc("tong_ca_mac_moi")) \
    .select("country_name","year","quarter","tong_ca_mac_moi","tong_ca_tu_vong") \
    .show(15, truncate=False)

# ── LƯU ──────────────────────────────────────────────────────
df_quarterly.write.mode("overwrite").parquet(
    "hdfs://namenode:8020/data/processed/2_quarterly_aggregated"
)
print("\n Đã lưu: hdfs://namenode:8020/data/processed/2_quarterly_aggregated")

spark.stop()
print(" HOÀN TẤT FILE 2b!")
