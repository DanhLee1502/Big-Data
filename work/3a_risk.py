from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

# ============================================================
# FILE 3a: PHÂN TÍCH MỨC ĐỘ NGUY HIỂM (RISK ASSESSMENT)
# Mục tiêu: Phân loại mức độ nguy hiểm từng tháng từng nước
# ============================================================

spark = SparkSession.builder \
    .appName("3a_Risk_Assessment") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

os.makedirs("/home/jovyan/work/output", exist_ok=True)

print("=" * 65)
print("FILE 3a: ĐÁNH GIÁ MỨC ĐỘ NGUY HIỂM DỊCH BỆNH")
print("=" * 65)

df = spark.read.parquet(
    "hdfs://namenode:8020/data/processed/2_monthly_aggregated"
)
print(f"\n Đọc dữ liệu monthly: {df.count():,} nhóm")

# ── PHÂN LOẠI ─────────────────────────────────────────────────
df_risk = df \
    .withColumn("muc_do_lay_lan",
        F.when(F.col("tong_ca_mac_moi") >= 500000, "1. Cuc ky nghiem trong")
         .when(F.col("tong_ca_mac_moi") >= 100000, "2. Rat cao (Bao dong)")
         .when(F.col("tong_ca_mac_moi") >=  10000, "3. Cao")
         .when(F.col("tong_ca_mac_moi") >=   1000, "4. Trung binh")
         .otherwise("5. Thap (Kiem soat)")
    ) \
    .withColumn("muc_do_tu_vong",
        F.when(F.col("ty_le_tu_vong_pct") >= 3.0, "1. Nguy hiem (>=3%)")
         .when(F.col("ty_le_tu_vong_pct") >= 1.0, "2. Cao (1-3%)")
         .when(F.col("ty_le_tu_vong_pct") >= 0.5, "3. Trung binh (0.5-1%)")
         .otherwise("4. Thap (<0.5%)")
    )

print("\n--- [1] Phân phối mức độ lây lan ---")
df_risk.groupBy("muc_do_lay_lan").count() \
       .orderBy("muc_do_lay_lan").show(truncate=False)

print("--- [2] Phân phối mức độ tử vong ---")
df_risk.groupBy("muc_do_tu_vong").count() \
       .orderBy("muc_do_tu_vong").show(truncate=False)

print("--- [3] Những tháng CỰC KỲ NGHIÊM TRỌNG (>500.000 ca/tháng) ---")
df_risk.filter(F.col("tong_ca_mac_moi") >= 500000) \
    .select("country_name","year","month",
            "tong_ca_mac_moi","tong_ca_tu_vong",
            "ty_le_tu_vong_pct","muc_do_lay_lan") \
    .orderBy(F.desc("tong_ca_mac_moi")).show(20, truncate=False)

print("--- [4] Tỉ lệ tử vong trung bình (CFR) từng nước ---")
df_risk.groupBy("country_name").agg(
    F.round(F.avg("ty_le_tu_vong_pct"), 3).alias("cfr_tb_%"),
    F.round(F.max("ty_le_tu_vong_pct"), 3).alias("cfr_max_%"),
    F.sum("tong_ca_mac_moi").alias("tong_ca_mac"),
    F.sum("tong_ca_tu_vong").alias("tong_tu_vong")
).orderBy(F.desc("cfr_tb_%")).show(truncate=False)

print("--- [5] Tháng nguy hiểm nhất từng nước ---")
df_risk.orderBy(F.desc("tong_ca_mac_moi")) \
    .select("country_name","year","month","tong_ca_mac_moi","muc_do_lay_lan") \
    .dropDuplicates(["country_name"]).show(15, truncate=False)

# ── LƯU ──────────────────────────────────────────────────────
df_risk.write.mode("overwrite").parquet(
    "hdfs://namenode:8020/data/processed/datamart_1_risk"
)
df_risk.coalesce(1).write.mode("overwrite").option("header", True) \
    .csv("file:///home/jovyan/work/output/datamart_1_risk")
print("\n Đã lưu HDFS: datamart_1_risk")
print("Đã lưu CSV : /home/jovyan/work/output/datamart_1_risk/")

spark.stop()
print("HOÀN TẤT FILE 3a!")
