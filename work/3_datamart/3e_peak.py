from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os

# ============================================================
# FILE 3e: PHÂN TÍCH ĐỈNH DỊCH (PEAK ANALYSIS)
# Mục tiêu: Tìm tháng đỉnh dịch + giai đoạn dịch của từng nước
# Kỹ thuật: Window ROW_NUMBER
# ============================================================

spark = SparkSession.builder \
    .appName("3e_Peak_Analysis") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

os.makedirs("/home/jovyan/work/output", exist_ok=True)

print("=" * 65)
print("FILE 3e: PHÂN TÍCH ĐỈNH DỊCH TỪNG QUỐC GIA")
print("=" * 65)

df = spark.read.parquet(
    "hdfs://namenode:8020/data/processed/2_monthly_aggregated"
)
print(f"\n📥 Đọc dữ liệu monthly: {df.count():,} nhóm")

# ── TOP 1 ĐỈNH DỊCH MỖI NƯỚC ─────────────────────────────────
window_peak = Window.partitionBy("location_key") \
                    .orderBy(F.desc("tong_ca_mac_moi"))

df_peak = df \
    .withColumn("row_num", F.row_number().over(window_peak)) \
    .filter(F.col("row_num") == 1) \
    .select("country_name","year","month",
            "tong_ca_mac_moi","tong_ca_tu_vong",
            "ty_le_tu_vong_pct","ca_mac_tb_ngay") \
    .orderBy(F.desc("tong_ca_mac_moi"))

print("\n--- [1] Tháng đỉnh dịch của từng quốc gia ---")
df_peak.show(truncate=False)

# ── TOP 3 ĐỈNH DỊCH MỖI NƯỚC ─────────────────────────────────
df_top3_peak = df \
    .withColumn("row_num", F.row_number().over(window_peak)) \
    .filter(F.col("row_num") <= 3) \
    .orderBy("location_key", "row_num")

print("--- [2] Top 3 tháng cao điểm của mỗi nước ---")
df_top3_peak.select(
    "country_name","row_num","year","month",
    "tong_ca_mac_moi","tong_ca_tu_vong","ty_le_tu_vong_pct"
).show(40, truncate=False)

# ── CÁC THÁNG CÓ CA MẮC VƯỢT NGƯỠNG 100K ─────────────────────
print("--- [3] Các tháng có ca mắc vượt 100.000 (mốc báo động) ---")
df.filter(F.col("tong_ca_mac_moi") >= 100000) \
    .select("country_name","year","month","tong_ca_mac_moi","tong_ca_tu_vong") \
    .orderBy("country_name","year","month").show(50, truncate=False)

# ── SO SÁNH ĐỈNH DỊCH CÁC NƯỚC ───────────────────────────────
print("--- [4] So sánh đỉnh dịch các nước ---")
df_peak.select(
    "country_name",
    F.concat(F.col("year"), F.lit("/"), F.col("month")).alias("thang_dinh"),
    "tong_ca_mac_moi",
    "ca_mac_tb_ngay",
    "ty_le_tu_vong_pct"
).show(truncate=False)

# ── THỐNG KÊ THÁNG CÓ DỊCH (CA MẮC > 0) ─────────────────────
print("--- [5] Số tháng có dịch (ca mắc > 0) từng nước ---")
df.filter(F.col("tong_ca_mac_moi") > 0) \
    .groupBy("country_name").agg(
        F.count("*").alias("so_thang_co_dich"),
        F.round(F.avg("tong_ca_mac_moi"), 0).alias("ca_mac_tb_thang"),
        F.round(F.avg("ty_le_tu_vong_pct"), 3).alias("cfr_tb")
    ).orderBy(F.desc("so_thang_co_dich")).show(truncate=False)

# ── LƯU ──────────────────────────────────────────────────────
df_peak.write.mode("overwrite").parquet(
    "hdfs://namenode:8020/data/processed/datamart_5_peak"
)
df_peak.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv("file:///home/jovyan/work/output/datamart_5_peak")
print("\n✅ Đã lưu HDFS: datamart_5_peak")
print("✅ Đã lưu CSV : /home/jovyan/work/output/datamart_5_peak/")

spark.stop()
print("✅ HOÀN TẤT FILE 3e!")
