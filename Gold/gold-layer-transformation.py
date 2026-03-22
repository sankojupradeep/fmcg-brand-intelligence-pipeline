import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta, timezone
import boto3
import json

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

S3_BUCKET = "fmcg-brand-intelligence"

# ── DATES ──
today          = datetime.now(timezone.utc)
last_week      = datetime.now(timezone.utc) - timedelta(days=7)
PROC_DATE      = today.strftime("%Y-%m-%d")
LAST_WEEK_DATE = last_week.strftime("%Y-%m-%d")

print(f"Gold job running for date:      {PROC_DATE}")
print(f"Week over week comparison date: {LAST_WEEK_DATE}")

# ─────────────────────────────────────────────
# FETCH SNOWFLAKE CREDENTIALS
# Pulled from Secrets Manager — never hardcoded
# ─────────────────────────────────────────────
print("\nFetching Snowflake credentials...")

secrets_client = boto3.client(
    "secretsmanager",
    region_name="us-east-1"
)

secret = secrets_client.get_secret_value(
    SecretId="fmcg/snowflake/credentials"
)
sf_creds = json.loads(secret["SecretString"])

print("Keys found in Secrets Manager:")
for key in sf_creds.keys():
    print(f"  '{key}'")

# Snowflake Spark connector options
# Used only for reading last week WoW data
SNOWFLAKE_OPTIONS = {
    "sfURL":       (
        f"{sf_creds['snowflake_account']}"
        f".snowflakecomputing.com"
    ),
    "sfUser":      sf_creds["snowflake_user"],
    "sfPassword":  sf_creds["snowflake_password"],
    "sfDatabase":  sf_creds["snowflake_database"],
    "sfSchema":    sf_creds["snowflake_schema"],
    "sfWarehouse": sf_creds["snowflake_warehouse"],
    "sfRole":      "ACCOUNTADMIN"
}

# JDBC URL for writing Gold to Snowflake
# More stable than Spark connector in Glue 4.0
JDBC_URL = (
    f"jdbc:snowflake://"
    f"{sf_creds['snowflake_account']}"
    f".snowflakecomputing.com"
    f"/?db={sf_creds['snowflake_database']}"
    f"&schema={sf_creds['snowflake_schema']}"
    f"&warehouse={sf_creds['snowflake_warehouse']}"
    f"&role=ACCOUNTADMIN"
)

JDBC_PROPERTIES = {
    "user":     sf_creds["snowflake_user"],
    "password": sf_creds["snowflake_password"],
    "driver":   "net.snowflake.client.jdbc.SnowflakeDriver"
}

print("Snowflake credentials loaded successfully")
print(f"Connecting to: {SNOWFLAKE_OPTIONS['sfURL']}")

# ─────────────────────────────────────────────
# 1. READ SILVER FROM GLUE CATALOG
# push_down_predicate filters by ingestion_date
# at read time — only loads today's partition
# ─────────────────────────────────────────────
print("\nReading Silver tables from Glue Catalog...")

news_predicate = "ingestion_date='" + PROC_DATE + "'"
news_df = glueContext.create_dynamic_frame.from_catalog(
    database="fmcg-silver-crawler",
    table_name="news",
    push_down_predicate=news_predicate
).toDF()

print("\nNews schema:")
news_df.printSchema()
print(f"News records for {PROC_DATE}: {news_df.count()}")

stocks_predicate = "ingestion_date='" + PROC_DATE + "'"
stocks_df = glueContext.create_dynamic_frame.from_catalog(
    database="fmcg-silver-crawler",
    table_name="stocks",
    push_down_predicate=stocks_predicate
).toDF()

print("\nStocks schema:")
stocks_df.printSchema()
print(f"Stock records for {PROC_DATE}: {stocks_df.count()}")

trends_predicate = "ingestion_date='" + PROC_DATE + "'"
trends_df = glueContext.create_dynamic_frame.from_catalog(
    database="fmcg-silver-crawler",
    table_name="trends",
    push_down_predicate=trends_predicate
).toDF()

print("\nTrends schema:")
trends_df.printSchema()
print(f"Trends records for {PROC_DATE}: {trends_df.count()}")

# ─────────────────────────────────────────────
# 2. SELECT REQUIRED COLUMNS
# stock_available only in stocks — not trends
# prevents AMBIGUOUS_REFERENCE on join
# ingestion_date excluded — processed_date
# captures the pipeline run date in Gold
# ─────────────────────────────────────────────
print("\nSelecting required columns...")

news_daily = news_df.select(
    "brand_name",
    "brand_type",
    "category",
    "avg_daily_sentiment",
    "share_of_voice_pct",
    "news_velocity",
    "coverage_trend",
    "daily_article_count",
    "positive_count",
    "negative_count",
    "rolling_7d_avg"
)

stocks_daily = stocks_df.select(
    "brand_name",
    "brand_type",
    "category",
    "ticker",
    "close_price",
    "daily_change_pct",
    "volatility_7d",
    "price_signal",
    "stock_available"    # only here — not in trends
)

trends_daily = trends_df.select(
    "brand_name",
    "brand_type",
    "category",
    "trend_score",
    "trend_momentum",
    "trend_zscore",
    "is_anomaly"
    # stock_available intentionally excluded
    # comes from stocks table only
)

print(f"News daily:   {news_daily.count()}")
print(f"Stocks daily: {stocks_daily.count()}")
print(f"Trends daily: {trends_daily.count()}")

# ─────────────────────────────────────────────
# 3. JOIN ALL THREE SOURCES
# News + Trends — all 12 brands
# Stocks left join — null for private brands
# ─────────────────────────────────────────────
print("\nJoining Silver tables...")

# News + Trends
news_trends = news_daily.join(
    trends_daily,
    on=["brand_name", "category", "brand_type"],
    how="left"
)

# Join with Stocks
# Private brands get null stock fields
brand_gold = news_trends.join(
    stocks_daily,
    on=["brand_name", "category", "brand_type"],
    how="left"
)

# stock_available from stocks only
# private brands get null → coalesce sets False
brand_gold = brand_gold.withColumn(
    "stock_available",
    F.coalesce(
        F.col("stock_available"),
        F.lit(False)
    )
)

print(f"Joined records: {brand_gold.count()}")

# ─────────────────────────────────────────────
# 4. NORMALIZE ALL SIGNALS TO 0-100
# All signals on different scales
# Must normalize before combining into index
# ─────────────────────────────────────────────
print("\nNormalizing signals to 0-100 scale...")

# Sentiment: -1/+1 → 0-100
# -1.0 → 0   (worst negative)
#  0.0 → 50  (neutral)
# +1.0 → 100 (best positive)
brand_gold = brand_gold.withColumn(
    "sentiment_normalized",
    F.round(
        ((F.col("avg_daily_sentiment") + 1) / 2) * 100, 2
    )
)

# Trend: already 0-100 from Google
# Clamp edges to be safe
brand_gold = brand_gold.withColumn(
    "trend_normalized",
    F.round(
        F.when(F.col("trend_score") > 100, 100)
         .when(F.col("trend_score") < 0,     0)
         .otherwise(F.col("trend_score")), 2
    )
)

# Stock: ±5% daily change → 0-100
# -5% → 0,  0% → 50,  +5% → 100
# null for private brands
brand_gold = brand_gold.withColumn(
    "stock_normalized",
    F.round(
        F.when(
            F.col("stock_available") == True,
            F.when(
                F.col("daily_change_pct").isNotNull(),
                F.least(
                    F.lit(100.0),
                    F.greatest(
                        F.lit(0.0),
                        (F.col("daily_change_pct") + 5) * 10
                    )
                )
            ).otherwise(F.lit(50.0))
        ).otherwise(
            F.lit(None).cast(DoubleType())
        ), 2
    )
)

# ─────────────────────────────────────────────
# 5. BRAND HEALTH INDEX
# Listed  → sentiment 30% + trend 30%
#           + stock 30% + sov 10%
# Private → sentiment 40% + trend 40%
#           + sov 20%
# ─────────────────────────────────────────────
print("\nComputing Brand Health Index...")

brand_gold = brand_gold.withColumn(
    "brand_health_index",
    F.round(
        F.when(
            F.col("stock_available") == True,
            (F.col("sentiment_normalized") * 0.30) +
            (F.col("trend_normalized")     * 0.30) +
            (F.col("stock_normalized")     * 0.30) +
            (F.col("share_of_voice_pct")   * 0.10)
        ).otherwise(
            (F.col("sentiment_normalized") * 0.40) +
            (F.col("trend_normalized")     * 0.40) +
            (F.col("share_of_voice_pct")   * 0.20)
        ), 2
    )
)

# Health label
brand_gold = brand_gold.withColumn(
    "health_label",
    F.when(
        F.col("brand_health_index") >= 75, "STRONG"
    ).when(
        F.col("brand_health_index") >= 55, "HEALTHY"
    ).when(
        F.col("brand_health_index") >= 35, "WEAK"
    ).otherwise("AT_RISK")
)

# Alert flag
brand_gold = brand_gold.withColumn(
    "alert_flag",
    F.when(
        (F.col("is_anomaly") == True) |
        (F.col("avg_daily_sentiment") < -0.5) |
        (
            (F.col("coverage_trend") == "ACCELERATING") &
            (F.col("avg_daily_sentiment") < 0)
        ),
        True
    ).otherwise(False)
)

# Alert reason
brand_gold = brand_gold.withColumn(
    "alert_reason",
    F.when(
        F.col("avg_daily_sentiment") < -0.5,
        "High negative sentiment"
    ).when(
        F.col("is_anomaly") == True,
        "Abnormal trend spike — zscore > 2"
    ).when(
        (F.col("coverage_trend") == "ACCELERATING") &
        (F.col("avg_daily_sentiment") < 0),
        "Accelerating negative coverage"
    ).otherwise(
        F.lit(None).cast(StringType())
    )
)

brand_gold = brand_gold.withColumn(
    "processed_date",
    F.lit(PROC_DATE).cast("date")
)

# ─────────────────────────────────────────────
# 6. WEEK OVER WEEK FROM SNOWFLAKE
# Read last week directly from Snowflake
# First run → WoW columns null (expected)
# After 7 days → auto populates
# ─────────────────────────────────────────────
print(f"\nFetching WoW data from Snowflake ({LAST_WEEK_DATE})...")

try:
    last_week_df = spark.read \
        .format("net.snowflake.spark.snowflake") \
        .options(**SNOWFLAKE_OPTIONS) \
        .option(
            "query",
            f"""
            SELECT
                BRAND_NAME,
                BRAND_HEALTH_INDEX   AS last_week_index,
                HEALTH_LABEL         AS last_week_label,
                SENTIMENT_NORMALIZED AS last_week_sentiment,
                TREND_NORMALIZED     AS last_week_trend
            FROM BRAND_HEALTH
            WHERE PROCESSED_DATE = '{LAST_WEEK_DATE}'
            """
        ).load()

    # Lowercase columns to match PySpark convention
    last_week_df = last_week_df.toDF(
        *[c.lower() for c in last_week_df.columns]
    )

    wow_count = last_week_df.count()
    print(f"Last week Snowflake records: {wow_count}")

    if wow_count > 0:
        brand_gold = brand_gold.join(
            last_week_df,
            on="brand_name",
            how="left"
        ).withColumn(
            "index_change_wow",
            F.round(
                F.col("brand_health_index") -
                F.col("last_week_index"), 2
            )
        ).withColumn(
            "wow_direction",
            F.when(
                F.col("index_change_wow") > 5,  "IMPROVING"
            ).when(
                F.col("index_change_wow") < -5, "DECLINING"
            ).otherwise("STABLE")
        ).withColumn(
            "label_changed",
            F.when(
                F.col("health_label") !=
                F.col("last_week_label"), True
            ).otherwise(False)
        ).withColumn(
            "sentiment_shift",
            F.round(
                F.col("sentiment_normalized") -
                F.col("last_week_sentiment"), 2
            )
        ).withColumn(
            "trend_shift",
            F.round(
                F.col("trend_normalized") -
                F.col("last_week_trend"), 2
            )
        )
        print("WoW comparison added successfully")

    else:
        raise Exception("No last week records found")

except Exception as e:
    print(f"First run or no history: {str(e)}")
    brand_gold = brand_gold \
        .withColumn("last_week_index",
            F.lit(None).cast(DoubleType())) \
        .withColumn("last_week_label",
            F.lit(None).cast(StringType())) \
        .withColumn("index_change_wow",
            F.lit(None).cast(DoubleType())) \
        .withColumn("wow_direction",
            F.lit(None).cast(StringType())) \
        .withColumn("label_changed",
            F.lit(None).cast(BooleanType())) \
        .withColumn("sentiment_shift",
            F.lit(None).cast(DoubleType())) \
        .withColumn("trend_shift",
            F.lit(None).cast(DoubleType()))

# ─────────────────────────────────────────────
# 7. FINAL GOLD TABLE
# One row per brand per day
# ─────────────────────────────────────────────
gold_final = brand_gold.select(
    # Identity
    F.col("brand_name"),
    F.col("brand_type"),
    F.col("category"),
    F.col("processed_date"),

    # Raw signals
    F.col("avg_daily_sentiment"),
    F.col("trend_score"),
    F.col("trend_momentum"),
    F.col("share_of_voice_pct"),
    F.col("daily_article_count"),
    F.col("news_velocity"),
    F.col("coverage_trend"),

    # Stock signals — null for private brands
    F.col("close_price"),
    F.col("daily_change_pct"),
    F.col("volatility_7d"),
    F.col("price_signal"),
    F.col("stock_available"),

    # Normalized scores
    F.col("sentiment_normalized"),
    F.col("trend_normalized"),
    F.col("stock_normalized"),

    # Brand Health Index
    F.col("brand_health_index"),
    F.col("health_label"),

    # Alerts
    F.col("is_anomaly"),
    F.col("alert_flag"),
    F.col("alert_reason"),

    # Week over week
    F.col("last_week_index"),
    F.col("last_week_label"),
    F.col("index_change_wow"),
    F.col("wow_direction"),
    F.col("label_changed"),
    F.col("sentiment_shift"),
    F.col("trend_shift")
)

print(f"\nFinal Gold records: {gold_final.count()}")
print(f"Final Gold columns: {len(gold_final.columns)}")

# Preview in CloudWatch logs
print("\nGold preview:")
gold_final.select(
    "brand_name",
    "brand_health_index",
    "health_label",
    "alert_flag",
    "index_change_wow",
    "wow_direction"
).show(12, truncate=False)

print("\nWriting Gold to Snowflake via JDBC...")

JDBC_URL = (
    f"jdbc:snowflake://"
    f"{sf_creds['snowflake_account']}"
    f".snowflakecomputing.com"
    f"/?db={sf_creds['snowflake_database']}"
    f"&schema={sf_creds['snowflake_schema']}"
    f"&warehouse={sf_creds['snowflake_warehouse']}"
    f"&role=ACCOUNTADMIN"
)

JDBC_PROPERTIES = {
    "user":     sf_creds["snowflake_user"],
    "password": sf_creds["snowflake_password"],
    "driver":   "net.snowflake.client.jdbc.SnowflakeDriver"
}

gold_final.write \
    .jdbc(
        url=JDBC_URL,
        table="BRAND_HEALTH",
        mode="append",
        properties=JDBC_PROPERTIES
    )

print("Snowflake write complete")
print(f"\nGold job complete for {PROC_DATE}")

job.commit()
