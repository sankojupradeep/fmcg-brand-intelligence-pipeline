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

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

S3_BUCKET   = "fmcg-brand-intelligence"
BRONZE_PATH = f"s3://{S3_BUCKET}/bronze/"
SILVER_PATH = f"s3://{S3_BUCKET}/silver/"

today     = datetime.now(timezone.utc)
YEAR      = today.strftime("%Y")
MONTH     = today.strftime("%m")
DAY       = today.strftime("%d")
PROC_DATE = today.strftime("%Y-%m-%d")

print(f"Silver incremental run for: {PROC_DATE}")
print(f"Reading Bronze partition:   year={YEAR}/month={MONTH}/day={DAY}")

# ─────────────────────────────────────────────
# 1. NEWS
# ─────────────────────────────────────────────
print("\n── Processing News ──")

news_path = (
    f"{BRONZE_PATH}news/"
    f"*/*/year={YEAR}/month={MONTH}/day={DAY}/"
)

print(f"Bronze news path: {news_path}")

try:
    news_raw   = spark.read.json(news_path)
    news_count = news_raw.count()
    print(f"Bronze news records found: {news_count}")
except Exception as e:
    print(f"No Bronze news for {PROC_DATE}: {str(e)}")
    news_raw   = None
    news_count = 0

if news_count > 0:

    # ── Basic: Clean and select ──
    # Explicit select drops duplicate "source" column
    # conflict between raw NewsAPI "source" object
    # and our pipeline_source metadata field
    news_clean = news_raw.select(
        F.col("brand_name"),
        F.col("brand_type"),
        F.col("category"),
        F.col("ticker"),
        F.col("headline"),
        F.col("source_name"),
        F.col("published_at"),
        F.col("url"),
        F.col("ingested_at"),
        F.to_date(
            F.col("published_at")
        ).alias("news_date"),
        F.lit("newsapi").alias("data_source")
    ).filter(
        # Basic: Null filtering
        F.col("headline").isNotNull() &
        F.col("brand_name").isNotNull() &
        F.col("url").isNotNull()
    ).dropDuplicates(
        # Basic: Deduplication
        ["url", "brand_name"]
    )

    print(f"After cleaning: {news_clean.count()} records")

    # ── Advanced: Sentiment Scoring ──
    # VADER — rule based sentiment for short text
    # compound score: -1 (negative) to +1 (positive)
    def score_sentiment(headline):
        try:
            from vaderSentiment.vaderSentiment import (
                SentimentIntensityAnalyzer
            )
            analyzer = SentimentIntensityAnalyzer()
            return float(
                analyzer.polarity_scores(headline)["compound"]
            )
        except:
            return 0.0

    sentiment_udf = F.udf(score_sentiment, DoubleType())

    news_clean = news_clean.withColumn(
        "sentiment_score",
        sentiment_udf(F.col("headline"))
    ).withColumn(
        "sentiment_label",
        F.when(
            F.col("sentiment_score") >= 0.05,  "POSITIVE"
        ).when(
            F.col("sentiment_score") <= -0.05, "NEGATIVE"
        ).otherwise("NEUTRAL")
    ).withColumn(
        "week_number",
        F.weekofyear(F.col("news_date"))
    )

    # ── Advanced: Share of Voice ──
    # % of category coverage per brand per week
    # Coca-Cola = 42% of Beverages coverage this week
    category_window = Window.partitionBy(
        "category", "week_number"
    )
    brand_window = Window.partitionBy(
        "category", "brand_name", "week_number"
    )

    news_clean = news_clean.withColumn(
        "category_total_mentions",
        F.count("headline").over(category_window)
    ).withColumn(
        "brand_mentions",
        F.count("headline").over(brand_window)
    ).withColumn(
        "share_of_voice_pct",
        F.round(
            (F.col("brand_mentions") /
             F.col("category_total_mentions")) * 100, 2
        )
    )

    # ── Advanced: Daily Aggregation + News Velocity ──
    # One row per brand per day
    daily_news = news_clean.groupBy(
        "brand_name", "category",
        "brand_type", "news_date"
    ).agg(
        F.count("headline").alias("daily_article_count"),
        F.avg("sentiment_score").alias("avg_daily_sentiment"),
        F.first(
            "share_of_voice_pct"
        ).alias("share_of_voice_pct"),
        F.sum(
            F.when(
                F.col("sentiment_label") == "POSITIVE", 1
            ).otherwise(0)
        ).alias("positive_count"),
        F.sum(
            F.when(
                F.col("sentiment_label") == "NEGATIVE", 1
            ).otherwise(0)
        ).alias("negative_count")
    )

    # 7-day rolling average for velocity computation
    rolling_window = Window.partitionBy(
        "brand_name"
    ).orderBy("news_date").rowsBetween(-6, 0)

    news_silver = daily_news.withColumn(
        "rolling_7d_avg",
        F.round(
            F.avg("daily_article_count").over(
                rolling_window
            ), 2
        )
    ).withColumn(
        # Advanced: News velocity
        # Is coverage accelerating or decelerating?
        "news_velocity",
        F.when(
            F.col("rolling_7d_avg") > 0,
            F.round(
                ((F.col("daily_article_count") -
                  F.col("rolling_7d_avg")) /
                  F.col("rolling_7d_avg")) * 100, 2
            )
        ).otherwise(F.lit(0.0))
    ).withColumn(
        "coverage_trend",
        F.when(
            F.col("news_velocity") > 20,  "ACCELERATING"
        ).when(
            F.col("news_velocity") < -20, "DECELERATING"
        ).otherwise("STABLE")
    ).withColumn(
        # ingestion_date — when pipeline ran
        # used for partitioning instead of news_date
        # ensures Gold can filter by today's run
        "ingestion_date",
        F.lit(PROC_DATE)
    )

    silver_news_count = news_silver.count()
    print(f"Silver news records: {silver_news_count}")

    # ── Incremental Write ──
    # Partition by ingestion_date not news_date
    # ingestion_date = today's pipeline run date
    # Gold filters by ingestion_date to get today's batch
    news_silver.repartition(1).write \
        .mode("append") \
        .partitionBy(
            "category",
            "brand_name",
            "ingestion_date"
        ) \
        .parquet(f"{SILVER_PATH}news/")

    print(f"Silver news written → {SILVER_PATH}news/")
    print(f"Partition: ingestion_date={PROC_DATE}")

else:
    print(f"No news data for {PROC_DATE} — skipping")

# ─────────────────────────────────────────────
# 2. STOCKS
# ─────────────────────────────────────────────
print("\n── Processing Stocks ──")

stocks_path = (
    f"{BRONZE_PATH}stocks/"
    f"*/*/year={YEAR}/month={MONTH}/day={DAY}/"
)

print(f"Bronze stocks path: {stocks_path}")

try:
    stocks_raw   = spark.read.json(stocks_path)
    stocks_count = stocks_raw.count()
    print(f"Bronze stocks records found: {stocks_count}")
except Exception as e:
    print(f"No Bronze stocks for {PROC_DATE}: {str(e)}")
    stocks_raw   = None
    stocks_count = 0

if stocks_count > 0:

    stock_window = Window.partitionBy(
        "ticker"
    ).orderBy("stock_date")

    volatility_window = Window.partitionBy(
        "ticker"
    ).orderBy("stock_date").rowsBetween(-6, 0)

    stocks_silver = stocks_raw.filter(
        # Basic: Null filtering
        F.col("close_price") > 0
    ).dropDuplicates(
        # Basic: Deduplication
        ["ticker", "date"]
    ).withColumn(
        # Basic: Schema standardization
        "stock_date", F.col("date")
    ).withColumn(
        "prev_close",
        F.lag("close_price", 1).over(stock_window)
    ).withColumn(
        # Advanced: Day over day price change %
        "daily_change_pct",
        F.when(
            F.col("prev_close").isNotNull() &
            (F.col("prev_close") > 0),
            F.round(
                ((F.col("close_price") -
                  F.col("prev_close")) /
                  F.col("prev_close")) * 100, 4
            )
        ).otherwise(F.lit(0.0))
    ).withColumn(
        # Advanced: 7-day rolling volatility
        # std dev of daily returns over 7 days
        "volatility_7d",
        F.round(
            F.stddev("daily_change_pct").over(
                volatility_window
            ), 4
        )
    ).withColumn(
        # Advanced: Price signal label
        "price_signal",
        F.when(
            F.col("daily_change_pct") > 2.0,  "STRONG_UP"
        ).when(
            F.col("daily_change_pct") > 0.5,  "UP"
        ).when(
            F.col("daily_change_pct") < -2.0, "STRONG_DOWN"
        ).when(
            F.col("daily_change_pct") < -0.5, "DOWN"
        ).otherwise("FLAT")
    ).select(
        F.col("brand_name"),
        F.col("brand_type"),
        F.col("category"),
        F.col("ticker"),
        F.col("exchange"),
        F.col("stock_date"),
        F.col("open_price"),
        F.col("close_price"),
        F.col("high_price"),
        F.col("low_price"),
        F.col("volume"),
        F.col("daily_change_pct"),
        F.col("volatility_7d"),
        F.col("price_signal"),
        F.col("ingested_at"),
        F.lit(True).alias("stock_available"),
        F.lit("yfinance").alias("data_source"),
        # ingestion_date for partitioning
        F.lit(PROC_DATE).alias("ingestion_date")
    )

    silver_stocks_count = stocks_silver.count()
    print(f"Silver stocks records: {silver_stocks_count}")

    stocks_silver.repartition(1).write \
        .mode("append") \
        .partitionBy(
            "category",
            "brand_name",
            "ingestion_date"
        ) \
        .parquet(f"{SILVER_PATH}stocks/")

    print(f"Silver stocks written → {SILVER_PATH}stocks/")
    print(f"Partition: ingestion_date={PROC_DATE}")

else:
    print(f"No stocks data for {PROC_DATE} — skipping")

# ─────────────────────────────────────────────
# 3. TRENDS
# ─────────────────────────────────────────────
print("\n── Processing Trends ──")

trends_path = (
    f"{BRONZE_PATH}trends/"
    f"*/year={YEAR}/month={MONTH}/day={DAY}/"
)

print(f"Bronze trends path: {trends_path}")

try:
    trends_raw   = spark.read.json(trends_path)
    trends_count = trends_raw.count()
    print(f"Bronze trends records found: {trends_count}")
except Exception as e:
    print(f"No Bronze trends for {PROC_DATE}: {str(e)}")
    trends_raw   = None
    trends_count = 0

if trends_count > 0:

    brand_window = Window.partitionBy(
        "brand_name"
    ).orderBy("date")

    brand_stats_window = Window.partitionBy(
        "brand_name"
    )

    trends_silver = trends_raw.filter(
        # Basic: Null filtering
        F.col("trend_score").isNotNull() &
        F.col("brand_name").isNotNull()
    ).dropDuplicates(
        # Basic: Deduplication
        ["brand_name", "date"]
    ).withColumn(
        # Basic: Schema standardization
        "trend_date", F.col("date")
    ).withColumn(
        "prev_score",
        F.lag("trend_score", 1).over(brand_window)
    ).withColumn(
        # Advanced: Week over week momentum
        # How much did search interest change?
        "trend_momentum",
        F.when(
            F.col("prev_score").isNotNull() &
            (F.col("prev_score") > 0),
            F.round(
                ((F.col("trend_score") -
                  F.col("prev_score")) /
                  F.col("prev_score")) * 100, 2
            )
        ).otherwise(F.lit(0.0))
    ).withColumn(
        "trend_mean",
        F.avg("trend_score").over(brand_stats_window)
    ).withColumn(
        "trend_std",
        F.stddev("trend_score").over(brand_stats_window)
    ).withColumn(
        # Advanced: Z-Score anomaly detection
        # zscore > 2 = statistically unusual spike
        "trend_zscore",
        F.when(
            F.col("trend_std") > 0,
            F.round(
                (F.col("trend_score") -
                 F.col("trend_mean")) /
                 F.col("trend_std"), 4
            )
        ).otherwise(F.lit(0.0))
    ).withColumn(
        "is_anomaly",
        F.when(
            F.abs(F.col("trend_zscore")) > 2.0, True
        ).otherwise(False)
    ).select(
        F.col("brand_name"),
        F.col("brand_type"),
        F.col("category"),
        F.col("trend_date"),
        F.col("trend_score"),
        F.col("trend_momentum"),
        F.col("trend_zscore"),
        F.col("is_anomaly"),
        F.col("geo"),
        F.col("ingested_at"),
        F.col("stock_available"),
        F.lit("google_trends").alias("data_source"),
        # ingestion_date for partitioning
        F.lit(PROC_DATE).alias("ingestion_date")
    )

    silver_trends_count = trends_silver.count()
    print(f"Silver trends records: {silver_trends_count}")

    trends_silver.repartition(1).write \
        .mode("append") \
        .partitionBy(
            "category",
            "brand_name",
            "ingestion_date"
        ) \
        .parquet(f"{SILVER_PATH}trends/")

    print(f"Silver trends written → {SILVER_PATH}trends/")
    print(f"Partition: ingestion_date={PROC_DATE}")

else:
    print(f"No trends data for {PROC_DATE} — skipping")


job.commit()