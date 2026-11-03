from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================================
# FILE 2c: TỔNG HỢP THEO NĂM
# Mục tiêu: Gom nhóm số liệu theo Quốc gia × Năm
# ============================================================

spark = SparkSession.builder \
    .appName("2c_Tong_Hop_Nam") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("FILE 2c: TỔNG HỢP THEO NĂM")
print("=" * 60)

df = spark.read.parquet("hdfs://namenode:8020/data/processed/1_cleaned_data")
print(f"\n📥 Đọc dữ liệu sạch: {df.count():,} bản ghi")

# ── TỔNG HỢP ─────────────────────────────────────────────────
df_yearly = df.groupBy(
    "location_key", "country_name", "year"
).agg(
    F.sum("new_confirmed").alias("tong_ca_mac_moi"),
    F.sum("new_deceased").alias("tong_ca_tu_vong"),
    F.sum("new_recovered").alias("tong_ca_phuc_hoi"),
    F.max("cumulative_confirmed").alias("luy_ke_ca_mac"),
    F.max("cumulative_deceased").alias("luy_ke_tu_vong")
).withColumn("ty_le_tu_vong_pct",
    F.when(F.col("tong_ca_mac_moi") > 0,
        F.round((F.col("tong_ca_tu_vong") / F.col("tong_ca_mac_moi")) * 100, 3)
    ).otherwise(F.lit(0.0))
).orderBy("year", F.desc("tong_ca_mac_moi"))

print(f"\n📊 Số nhóm (quốc gia × năm): {df_yearly.count():,}")

print("\n--- Bảng đầy đủ từng năm ---")
df_yearly.show(40, truncate=False)

print("\n--- Tổng toàn khu vực từng năm ---")
df_yearly.groupBy("year").agg(
    F.sum("tong_ca_mac_moi").alias("tong_ca_khu_vuc"),
    F.sum("tong_ca_tu_vong").alias("tong_tu_vong_khu_vuc"),
    F.round(F.avg("ty_le_tu_vong_pct"), 3).alias("cfr_tb")
).orderBy("year").show(truncate=False)

print("\n--- Tổng kết toàn giai đoạn từng nước ---")
df_yearly.groupBy("country_name").agg(
    F.sum("tong_ca_mac_moi").alias("tong_toan_ky"),
    F.sum("tong_ca_tu_vong").alias("tu_vong_toan_ky"),
    F.max("luy_ke_ca_mac").alias("luy_ke_cao_nhat")
).withColumn("cfr_toan_ky",
    F.round((F.col("tu_vong_toan_ky") / F.col("tong_toan_ky")) * 100, 3)
).orderBy(F.desc("tong_toan_ky")).show(truncate=False)

# ── LƯU ──────────────────────────────────────────────────────
df_yearly.write.mode("overwrite").parquet(
    "hdfs://namenode:8020/data/processed/2_yearly_aggregated"
)
print("\n✅ Đã lưu: hdfs://namenode:8020/data/processed/2_yearly_aggregated")

spark.stop()
print("✅ HOÀN TẤT FILE 2c!")
