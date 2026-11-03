from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os

# ============================================================
# FILE 3c: BẢNG XẾP HẠNG TÂM DỊCH (RANKING)
# Mục tiêu: Nước nào là tâm dịch đứng đầu Đông Nam Á mỗi tháng?
# Kỹ thuật: Window Function RANK
# ============================================================

spark = SparkSession.builder \
    .appName("3c_Ranking") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

os.makedirs("/home/jovyan/work/output", exist_ok=True)

print("=" * 65)
print("FILE 3c: BẢNG XẾP HẠNG TÂM DỊCH ĐÔNG NAM Á")
print("=" * 65)

df = spark.read.parquet(
    "hdfs://namenode:8020/data/processed/2_monthly_aggregated"
)
print(f"\n Đọc dữ liệu monthly: {df.count():,} nhóm")

# ── WINDOW RANK ───────────────────────────────────────────────
window_rank = Window.partitionBy("year","month") \
                    .orderBy(F.desc("tong_ca_mac_moi"))

df_ranking = df.withColumn("xep_hang", F.rank().over(window_rank))

df_top3 = df_ranking.filter(F.col("xep_hang") <= 3) \
                    .orderBy("year","month","xep_hang")

print("\n--- [1] Top 3 tâm dịch theo tháng (năm 2020) ---")
df_top3.filter(F.col("year") == 2020) \
    .select("year","month","xep_hang","country_name",
            "tong_ca_mac_moi","tong_ca_tu_vong","ty_le_tu_vong_pct") \
    .show(40, truncate=False)

print("--- [2] Top 3 tâm dịch theo tháng (năm 2021) ---")
df_top3.filter(F.col("year") == 2021) \
    .select("year","month","xep_hang","country_name",
            "tong_ca_mac_moi","tong_ca_tu_vong","ty_le_tu_vong_pct") \
    .show(40, truncate=False)

print("--- [3] Top 3 tâm dịch theo tháng (năm 2022) ---")
df_top3.filter(F.col("year") == 2022) \
    .select("year","month","xep_hang","country_name",
            "tong_ca_mac_moi","tong_ca_tu_vong","ty_le_tu_vong_pct") \
    .show(40, truncate=False)

print("--- [4] Số tháng đứng hạng #1 tâm dịch từng nước ---")
df_ranking.filter(F.col("xep_hang") == 1) \
    .groupBy("country_name").count() \
    .withColumnRenamed("count","so_thang_hang_1") \
    .orderBy(F.desc("so_thang_hang_1")).show(truncate=False)

print("--- [5] Số tháng lọt Top 3 từng nước ---")
df_top3.groupBy("country_name").count() \
    .withColumnRenamed("count","so_thang_top3") \
    .orderBy(F.desc("so_thang_top3")).show(truncate=False)

print("--- [6] Bảng xếp hạng đầy đủ tháng đỉnh dịch 8/2021 ---")
df_ranking.filter((F.col("year") == 2021) & (F.col("month") == 8)) \
    .select("xep_hang","country_name","tong_ca_mac_moi","tong_ca_tu_vong","ty_le_tu_vong_pct") \
    .orderBy("xep_hang").show(truncate=False)

# ── LƯU ──────────────────────────────────────────────────────
df_ranking.write.mode("overwrite").parquet(
    "hdfs://namenode:8020/data/processed/datamart_3_ranking"
)
df_top3.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv("file:///home/jovyan/work/output/datamart_3_top3")
print("\n Đã lưu HDFS: datamart_3_ranking")
print(" Đã lưu CSV : /home/jovyan/work/output/datamart_3_top3/")

spark.stop()
print(" HOÀN TẤT FILE 3c!")
