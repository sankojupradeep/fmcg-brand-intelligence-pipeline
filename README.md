# FMCG Brand Intelligence Pipeline

> A real-time brand monitoring and competitive intelligence pipeline that ingests live data from Google Trends, News API, and Yahoo Finance for 12 major FMCG brands — then surfaces category-level market insights using AWS and Snowflake.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Brands Tracked](#brands-tracked)
- [Tech Stack](#tech-stack)
- [Pipeline Layers](#pipeline-layers)
- [Transformations](#transformations)
- [Brand Health Index](#brand-health-index)

---

## Project Overview

Most FMCG brand managers track performance by looking at sales reports — data that arrives weeks after decisions needed to be made. This pipeline solves that by aggregating real-time public signals (search trends, news sentiment, stock movement) into a single **Brand Health Index** per brand per day.

**The key engineering challenge:** 6 of the 12 brands are publicly listed (HUL, ITC, Dabur, Coca-Cola, PepsiCo) and 6 are private (Himalaya, Patanjali, Haldirams, Thums Up, Limca, Kurkure). The pipeline handles both with a unified Silver schema — listed brands use a 4-signal scoring formula including stock data, private brands use a 3-signal formula with redistributed weights.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     INGESTION LAYER                          │
│                                                              │
│  NewsAPI          yfinance         pytrends                  │
│  (headlines)      (stock prices)   (search trends)           │
│      │                │                 │                    │
│  Lambda           Lambda           Lambda                    │
│  (scheduled       (scheduled       (scheduled                │
│   every 3h)        every 1h)        every 6h)                │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER (S3)                         │
│                                                              │
│  s3://fmcg-brand-intelligence/bronze/                        │
│  ├── news/category={}/brand={}/year={}/month={}/day={}/      │
│  ├── stocks/category={}/brand={}/year={}/month={}/day={}/    │
│  └── trends/category={}/year={}/month={}/day={}/             │
│                                                              │
│  Format: Newline-delimited JSON                              │
│  Purpose: Immutable raw data archive                         │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│              SILVER LAYER (S3 + Glue Catalog)                │
│                                                              │
│  AWS Glue PySpark Job — Incremental daily transformation     │
│                                                              │
│  Transformations:                                            │
│  ├── Null filtering + deduplication                          │
│  ├── Schema standardization (JSON → Parquet)                 │
│  ├── VADER sentiment scoring (-1 to +1)                      │
│  ├── Share of voice % per category per week                  │
│  ├── 7-day rolling news velocity                             │
│  ├── Week-over-week trend momentum                           │
│  ├── Z-score anomaly detection (threshold: 2.0)              │
│  └── 7-day stock volatility (rolling stddev)                 │
│                                                              │
│  Partition key: ingestion_date                               │
│  Format: Parquet                                             │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    GOLD LAYER (Snowflake)                     │
│                                                              │
│  AWS Glue PySpark Job — Brand Health Index computation       │
│                                                              │
│  ├── Joins news + stocks + trends into 1 row per brand       │
│  ├── Normalizes all signals to 0-100 scale                   │
│  ├── Computes weighted Brand Health Index                     │
│  ├── Generates health labels (STRONG/HEALTHY/WEAK/AT_RISK)   │
│  ├── Auto-generates alert flags with reasons                 │
│  └── Week-over-week Brand Health Index comparison            │
│                                                              │
│  Destination: Snowflake FMCG_INTELLIGENCE.GOLD.BRAND_HEALTH  │
│  Write mode: Append (incremental daily)                      │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                   ORCHESTRATION                              │
│                                                              │
│  EventBridge Scheduler — 5 rules                            │
│  00:00 UTC → News Lambda                                     │
│  00:15 UTC → Stocks Lambda                                   │
│  00:30 UTC → Trends Lambda                                   │
│  01:30 UTC → Silver Glue Job                                 │
│  02:30 UTC → Gold Glue Job → Snowflake                       │
└─────────────────────────────────────────────────────────────┘
```

---

## Brands Tracked

| Category | Brand | Type | Data Sources |
|---|---|---|---|
| Beverages | Coca-Cola (KO) | Listed (NYSE) | News + Trends + Stock |
| Beverages | PepsiCo (PEP) | Listed (NYSE) | News + Trends + Stock |
| Beverages | Thums Up | Private | News + Trends |
| Beverages | Limca | Private | News + Trends |
| Health | HUL (HINDUNILVR) | Listed (NSE) | News + Trends + Stock |
| Health | Dabur (DABUR) | Listed (NSE) | News + Trends + Stock |
| Health | Himalaya | Private | News + Trends |
| Health | Patanjali | Private | News + Trends |
| Snacks | ITC (ITC) | Listed (NSE) | News + Trends + Stock |
| Snacks | Haldirams | Private | News + Trends |
| Snacks | Kurkure | Private | News + Trends |
| Snacks | Lays | Private | News + Trends |

---

## Tech Stack

```
Ingestion        AWS Lambda (Python 3.11)
Orchestration    AWS EventBridge Scheduler
Storage          AWS S3 (Bronze + Silver)
Processing       AWS Glue 4.0 (PySpark)
Catalog          AWS Glue Data Catalog
Serving          Snowflake (Gold layer)
Secrets          AWS Secrets Manager
Sentiment        VADER (vaderSentiment)
Stock Data       yfinance
Search Trends    pytrends (Google Trends)
News             NewsAPI
```

---

## Pipeline Layers

### Bronze Layer

Raw data exactly as APIs returned it. No transformations. Three Lambda functions ingest independently on different schedules.

**Idempotency:** Each Lambda checks if today's S3 partition already exists before calling the API. If yes — skips entirely. Safe to trigger multiple times per day without duplication.

**Partition structure:**
```
bronze/news/category=Beverages/brand=Coca-Cola/year=2026/month=03/day=22/
bronze/stocks/category=Health/brand=HUL/year=2026/month=03/day=22/
bronze/trends/category=Snacks/year=2026/month=03/day=22/
```

### Silver Layer

Cleaned, enriched, and transformed data in Parquet format. One Glue PySpark job reads yesterday's Bronze partition and appends to Silver.

**Incremental load:** Reads only `year={Y}/month={M}/day={D}` partition from Bronze. Appends to Silver partitioned by `ingestion_date`. Old Silver partitions never touched.

### Gold Layer

Business-ready Brand Health Index written directly to Snowflake via JDBC. One row per brand per day. Reads last week's Snowflake data for week-over-week comparison.

---

## Transformations

### Basic (Data Quality)

| Transformation | Description |
|---|---|
| Null filtering | Drops records with null headline, brand_name, close_price |
| Deduplication | News by URL, stocks by ticker+date, trends by brand+date |
| Schema standardization | published_at → news_date, date → stock_date |
| Source column fix | Drops conflicting NewsAPI "source" object |
| JSON → Parquet | 10x faster Athena queries, lower storage cost |

### Advanced (Signal Extraction)

| Transformation | Description |
|---|---|
| VADER Sentiment | Scores each headline -1 to +1, labels POSITIVE/NEGATIVE/NEUTRAL |
| Share of Voice | % of category news coverage per brand per week |
| News Velocity | 7-day rolling average → acceleration/deceleration signal |
| Trend Momentum | Week-over-week % change in Google search interest |
| Z-Score Anomaly | Flags brands with trend_zscore > 2.0 as statistically unusual |
| Stock Volatility | 7-day rolling stddev of daily returns |
| Price Signal | STRONG_UP / UP / FLAT / DOWN / STRONG_DOWN labels |

---

## Brand Health Index

Single composite score (0-100) per brand per day.

### Listed Brands (stock data available)

```
Brand Health Index =
  sentiment_normalized × 30% +
  trend_normalized     × 30% +
  stock_normalized     × 30% +
  share_of_voice_pct   × 10%
```

### Private Brands (no stock data)

```
Brand Health Index =
  sentiment_normalized × 40% +
  trend_normalized     × 40% +
  share_of_voice_pct   × 20%
```

### Normalization

| Signal | Raw Scale | Normalized |
|---|---|---|
| Sentiment | -1.0 to +1.0 | (x + 1) / 2 × 100 |
| Trend score | 0 to 100 | Clamped 0-100 |
| Stock change | -5% to +5% | (x + 5) × 10 |

### Health Labels

| Score | Label |
|---|---|
| ≥ 75 | STRONG |
| ≥ 55 | HEALTHY |
| ≥ 35 | WEAK |
| < 35 | AT_RISK |

### Alert Triggers

| Condition | Alert Reason |
|---|---|
| avg_daily_sentiment < -0.5 | High negative sentiment |
| trend_zscore > 2.0 | Abnormal trend spike |
| coverage_trend = ACCELERATING + negative sentiment | Accelerating negative coverage |

---


## Project Structure

```
fmcg-brand-intelligence/
│
├── README.md
│
├── bronze/
│   ├── lambda_news/
│   │   └── lambda_function.py       # NewsAPI ingestion
│   ├── lambda_stocks/
│   │   └── lambda_function.py       # yfinance ingestion
│   └── lambda_trends/
│       └── lambda_function.py       # pytrends ingestion
│
├── silver/
│   └── glue_silver_transformation.py # PySpark Silver job
│
├── gold/
│   └── glue_gold_transformation.py   # PySpark Gold + Snowflake
│
├── snowflake/
│   └── setup.sql                     # Snowflake DDL
│
├── docs/
│   └── architecture.png
│
└── requirements.txt
```


**Why Secrets Manager over environment variables?**
Snowflake credentials rotated via Secrets Manager without redeploying Lambda or Glue. Environment variables require redeployment on every credential change.
