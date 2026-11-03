from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os

# ============================================================
# FILE 3b: PHÂN TÍCH TỐC ĐỘ TĂNG TRƯỞNG THEO THÁNG (MoM)
# Mục tiêu: Dịch đang bùng phát hay suy giảm so với tháng trước?
# Kỹ thuật: Window Function LAG
# ============================================================

spark = SparkSession.builder \
    .appName("3b_MoM_Growth") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

os.makedirs("/home/jovyan/work/output", exist_ok=True)

print("=" * 65)
print("FILE 3b: TỐC ĐỘ TĂNG TRƯỞNG THEO THÁNG (MoM Growth)")
print("=" * 65)

df = spark.read.parquet(
    "hdfs://namenode:8020/data/processed/2_monthly_aggregated"
)
print(f"\n📥 Đọc dữ liệu monthly: {df.count():,} nhóm")

# ── WINDOW FUNCTION LAG ───────────────────────────────────────
window_mom = Window.partitionBy("location_key").orderBy("year", "month")

df_mom = df \
    .withColumn("ca_mac_thang_truoc",
        F.lag("tong_ca_mac_moi", 1).over(window_mom)
    ) \
    .withColumn("toc_do_tang_truong_pct",
        F.when(
            F.col("ca_mac_thang_truoc").isNull() | (F.col("ca_mac_thang_truoc") == 0),
            None
        ).otherwise(
            F.round(
                ((F.col("tong_ca_mac_moi") - F.col("ca_mac_thang_truoc"))
                 / F.col("ca_mac_thang_truoc")) * 100, 1
            )
        )
    ) \
    .withColumn("xu_huong_dich",
        F.when(F.col("toc_do_tang_truong_pct").isNull(),  "-- Thang dau --")
         .when(F.col("toc_do_tang_truong_pct") >  100,   "Bung phat cuc manh (>100%)")
         .when(F.col("toc_do_tang_truong_pct") >   20,   "Dang tang nhanh (>20%)")
         .when(F.col("toc_do_tang_truong_pct") >    0,   "Tang nhe (0-20%)")
         .when(F.col("toc_do_tang_truong_pct") ==   0,   "Di ngang (0%)")
         .when(F.col("toc_do_tang_truong_pct") >  -20,   "Giam nhe (0 den -20%)")
         .otherwise("Suy giam manh (<-20%)")
    )

print("\n--- [1] Phân phối xu hướng dịch bệnh ---")
df_mom.groupBy("xu_huong_dich").count() \
      .orderBy(F.desc("count")).show(truncate=False)

print("--- [2] Top 10 tháng BÙNG PHÁT MẠNH NHẤT ---")
df_mom.filter(F.col("toc_do_tang_truong_pct") > 100) \
    .select("country_name","year","month",
            "ca_mac_thang_truoc","tong_ca_mac_moi",
            "toc_do_tang_truong_pct","xu_huong_dich") \
    .orderBy(F.desc("toc_do_tang_truong_pct")) \
    .show(10, truncate=False)

print("--- [3] Tốc độ tăng trưởng trung bình từng nước ---")
df_mom.filter(F.col("toc_do_tang_truong_pct").isNotNull()) \
    .groupBy("country_name").agg(
        F.round(F.avg("toc_do_tang_truong_pct"), 1).alias("tang_truong_tb"),
        F.round(F.max("toc_do_tang_truong_pct"), 1).alias("tang_truong_max"),
        F.round(F.min("toc_do_tang_truong_pct"), 1).alias("tang_truong_min")
    ).orderBy(F.desc("tang_truong_max")).show(truncate=False)

print("--- [4] Diễn biến MoM của Việt Nam ---")
df_mom.filter(F.col("location_key") == "VN") \
    .select("year","month","ca_mac_thang_truoc",
            "tong_ca_mac_moi","toc_do_tang_truong_pct","xu_huong_dich") \
    .orderBy("year","month").show(40, truncate=False)

print("--- [5] Diễn biến MoM của Indonesia ---")
df_mom.filter(F.col("location_key") == "ID") \
    .select("year","month","ca_mac_thang_truoc",
            "tong_ca_mac_moi","toc_do_tang_truong_pct","xu_huong_dich") \
    .orderBy("year","month").show(40, truncate=False)

# ── LƯU ──────────────────────────────────────────────────────
df_mom.write.mode("overwrite").parquet(
    "hdfs://namenode:8020/data/processed/datamart_2_mom_growth"
)
df_mom.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv("file:///home/jovyan/work/output/datamart_2_growth")
print("\n✅ Đã lưu HDFS: datamart_2_mom_growth")
print("✅ Đã lưu CSV : /home/jovyan/work/output/datamart_2_growth/")

spark.stop()
print("✅ HOÀN TẤT FILE 3b!")
