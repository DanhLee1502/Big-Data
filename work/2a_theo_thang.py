from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================================
# FILE 2a: TỔNG HỢP THEO THÁNG
# Mục tiêu: Gom nhóm số liệu theo Quốc gia × Năm × Tháng
# ============================================================

spark = SparkSession.builder \
    .appName("2a_Tong_Hop_Thang") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("FILE 2a: TỔNG HỢP THEO THÁNG")
print("=" * 60)

df = spark.read.parquet("hdfs://namenode:8020/data/processed/1_cleaned_data")
print(f"\nĐọc dữ liệu sạch: {df.count():,} bản ghi")

# ── TỔNG HỢP ─────────────────────────────────────────────────
df_monthly = df.groupBy(
    "location_key", "country_name", "year", "month", "quarter"
).agg(
    F.sum("new_confirmed").alias("tong_ca_mac_moi"),
    F.sum("new_deceased").alias("tong_ca_tu_vong"),
    F.sum("new_recovered").alias("tong_ca_phuc_hoi"),
    F.sum("new_tested").alias("tong_xet_nghiem"),
    F.max("cumulative_confirmed").alias("luy_ke_ca_mac"),
    F.max("cumulative_deceased").alias("luy_ke_tu_vong"),
    F.count("date").alias("so_ngay_ghi_nhan")
).withColumn("ty_le_tu_vong_pct",
    F.when(F.col("tong_ca_mac_moi") > 0,
        F.round((F.col("tong_ca_tu_vong") / F.col("tong_ca_mac_moi")) * 100, 3)
    ).otherwise(F.lit(0.0))
).withColumn("ca_mac_tb_ngay",
    F.when(F.col("so_ngay_ghi_nhan") > 0,
        F.round(F.col("tong_ca_mac_moi") / F.col("so_ngay_ghi_nhan"), 0)
    ).otherwise(F.lit(0.0))
).orderBy("location_key", "year", "month")

print(f"\nSố nhóm (quốc gia × tháng): {df_monthly.count():,}")

print("\n--- Schema ---")
df_monthly.printSchema()

print("\n--- 15 dòng mẫu ---")
df_monthly.show(15, truncate=False)

print("\n--- Tổng ca mắc theo tháng của Việt Nam ---")
df_monthly.filter(F.col("location_key") == "VN") \
    .select("year","month","tong_ca_mac_moi","tong_ca_tu_vong","ty_le_tu_vong_pct","ca_mac_tb_ngay") \
    .orderBy("year","month").show(40, truncate=False)

print("\n--- Tổng ca mắc theo tháng của Indonesia ---")
df_monthly.filter(F.col("location_key") == "ID") \
    .select("year","month","tong_ca_mac_moi","tong_ca_tu_vong","ty_le_tu_vong_pct") \
    .orderBy("year","month").show(40, truncate=False)

# ── LƯU ──────────────────────────────────────────────────────
df_monthly.write.mode("overwrite").parquet(
    "hdfs://namenode:8020/data/processed/2_monthly_aggregated"
)
print("\nĐã lưu: hdfs://namenode:8020/data/processed/2_monthly_aggregated")

spark.stop()
print(" HOÀN TẤT FILE 2a!")
