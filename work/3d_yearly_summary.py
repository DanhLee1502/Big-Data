from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

# ============================================================
# FILE 3d: TỔNG KẾT VĨ MÔ THEO NĂM (YEARLY SUMMARY)
# Mục tiêu: Bức tranh toàn cảnh từng năm của Đông Nam Á
# ============================================================

spark = SparkSession.builder \
    .appName("3d_Yearly_Summary") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

os.makedirs("/home/jovyan/work/output", exist_ok=True)

print("=" * 65)
print("FILE 3d: TỔNG KẾT VĨ MÔ THEO NĂM")
print("=" * 65)

df_yearly = spark.read.parquet(
    "hdfs://namenode:8020/data/processed/2_yearly_aggregated"
)
print(f"\n📥 Đọc dữ liệu yearly: {df_yearly.count():,} nhóm")

print("\n--- [1] Bảng đầy đủ từng quốc gia × từng năm ---")
df_yearly.select(
    "year","country_name",
    "tong_ca_mac_moi","tong_ca_tu_vong",
    "ty_le_tu_vong_pct","luy_ke_ca_mac"
).orderBy("year", F.desc("tong_ca_mac_moi")).show(40, truncate=False)

print("--- [2] Tổng toàn khu vực Đông Nam Á theo năm ---")
df_yearly.groupBy("year").agg(
    F.sum("tong_ca_mac_moi").alias("tong_ca_khu_vuc"),
    F.sum("tong_ca_tu_vong").alias("tong_tu_vong_khu_vuc"),
    F.round(F.avg("ty_le_tu_vong_pct"), 3).alias("cfr_tb_khu_vuc_%")
).orderBy("year").show(truncate=False)

print("--- [3] Nước dẫn đầu số ca mắc từng năm ---")
df_yearly.orderBy("year", F.desc("tong_ca_mac_moi")) \
    .dropDuplicates(["year"]) \
    .select("year","country_name","tong_ca_mac_moi","ty_le_tu_vong_pct") \
    .show(truncate=False)

print("--- [4] Tổng kết toàn giai đoạn (2020-2022) từng nước ---")
df_yearly.groupBy("country_name").agg(
    F.sum("tong_ca_mac_moi").alias("tong_ca_toan_ky"),
    F.sum("tong_ca_tu_vong").alias("tu_vong_toan_ky"),
    F.max("luy_ke_ca_mac").alias("luy_ke_cao_nhat")
).withColumn("cfr_toan_ky",
    F.round((F.col("tu_vong_toan_ky") / F.col("tong_ca_toan_ky")) * 100, 3)
).orderBy(F.desc("tong_ca_toan_ky")).show(truncate=False)

print("--- [5] So sánh tốc độ tăng năm 2020 → 2021 → 2022 ---")
for country in ["VN","ID","MY","TH","PH"]:
    print(f"\n  {country}:")
    df_yearly.filter(F.col("location_key") == country) \
        .select("year","tong_ca_mac_moi","tong_ca_tu_vong","ty_le_tu_vong_pct") \
        .orderBy("year").show(truncate=False)

# ── LƯU ──────────────────────────────────────────────────────
df_yearly.write.mode("overwrite").parquet(
    "hdfs://namenode:8020/data/processed/datamart_4_yearly_summary"
)
df_yearly.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv("file:///home/jovyan/work/output/datamart_4_yearly")
print("\n✅ Đã lưu HDFS: datamart_4_yearly_summary")
print("✅ Đã lưu CSV : /home/jovyan/work/output/datamart_4_yearly/")

spark.stop()
print("✅ HOÀN TẤT FILE 3d!")
