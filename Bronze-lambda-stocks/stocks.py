import json
import boto3
import os
import yfinance as yf
from datetime import datetime, timezone

LISTED_BRANDS = {
    "Beverages": {
        "Coca-Cola": {"ticker": "KO",             "exchange": "NYSE"},
        "PepsiCo":   {"ticker": "PEP",            "exchange": "NYSE"}
    },
    "Health": {
        "HUL":   {"ticker": "HINDUNILVR.NS", "exchange": "NSE"},
        "Dabur": {"ticker": "DABUR.NS",      "exchange": "NSE"}
    },
    "Snacks": {
        "ITC":   {"ticker": "ITC.NS",        "exchange": "NSE"}
    }
}


def already_ingested(s3_client, bucket, brand_name, category, today):
    """
    Check if today's stock partition already exists.
    Stock prices don't change after market close —
    no point re-fetching same day data.
    """
    prefix = (
        f"bronze/stocks/"
        f"category={category}/"
        f"brand={brand_name}/"
        f"year={today.year}/"
        f"month={today.month:02d}/"
        f"day={today.day:02d}/"
    )
    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
        MaxKeys=1
    )
    return response.get("KeyCount", 0) > 0


def fetch_stock_data(brand_name, ticker, exchange, category):
    run_timestamp = datetime.now(timezone.utc).isoformat()

    try:
        stock = yf.Ticker(ticker)

        # Fetch only last 2 days — incremental keeps this minimal
        hist = stock.history(period="2d", interval="1d")

        if hist.empty:
            print(f"  No data for {ticker}")
            return []

        records = []

        for date, row in hist.iterrows():
            daily_change_pct = (
                ((row["Close"] - row["Open"]) / row["Open"]) * 100
                if row["Open"] > 0 else 0.0
            )

            records.append({
                # Identity
                "brand_name":       brand_name,
                "brand_type":       "listed",
                "category":         category,
                "ticker":           ticker,
                "exchange":         exchange,

                # Stock fields
                "date":             date.strftime("%Y-%m-%d"),
                "open_price":       round(float(row["Open"]), 4),
                "high_price":       round(float(row["High"]), 4),
                "low_price":        round(float(row["Low"]), 4),
                "close_price":      round(float(row["Close"]), 4),
                "volume":           int(row["Volume"]),
                "daily_change_pct": round(daily_change_pct, 4),
                "price_range":      round(float(row["High"] - row["Low"]), 4),

                # Pipeline metadata
                "ingested_at":      run_timestamp,
                "pipeline_source":  "yfinance",
                "data_layer":       "bronze",
                "stock_available":  True
            })

        print(f"  {ticker} → {len(records)} records")
        return records

    except Exception as e:
        print(f"  Error fetching {ticker}: {str(e)}")
        return []


def write_to_s3(s3_client, data, brand_name, category, today):
    if not data:
        return 0

    s3_key = (
        f"bronze/stocks/"
        f"category={category}/"
        f"brand={brand_name}/"
        f"year={today.year}/"
        f"month={today.month:02d}/"
        f"day={today.day:02d}/"
        f"stocks_{brand_name}_{today.strftime('%Y%m%d_%H%M%S')}.json"
    )

    body = "\n".join(json.dumps(r) for r in data)

    s3_client.put_object(
        Bucket=os.environ["S3_BUCKET_NAME"],
        Key=s3_key,
        Body=body.encode("utf-8"),
        ContentType="application/json"
    )

    print(f"  Written {len(data)} → {s3_key}")
    return len(data)


def lambda_handler(event, context):
    s3_client = boto3.client("s3")
    bucket    = os.environ["S3_BUCKET_NAME"]
    today     = datetime.now(timezone.utc)
    total     = 0
    summary   = []
    skipped   = []

    for category, brands in LISTED_BRANDS.items():
        print(f"\nCategory: {category}")

        for brand_name, brand_meta in brands.items():

            # ── INCREMENTAL CHECK ──
            if already_ingested(s3_client, bucket, brand_name, category, today):
                print(f"  {brand_name} already ingested today — skipping")
                skipped.append(brand_name)
                continue

            print(f"  Fetching stock for {brand_name}...")
            records = fetch_stock_data(
                brand_name,
                brand_meta["ticker"],
                brand_meta["exchange"],
                category
            )

            count = write_to_s3(
                s3_client, records,
                brand_name, category, today
            )

            total += count
            summary.append({
                "brand":    brand_name,
                "ticker":   brand_meta["ticker"],
                "records":  count,
                "date":     today.strftime("%Y-%m-%d")
            })

    print(f"\nRun complete — {total} records written")
    print(f"Skipped: {skipped}")

    return {
        "statusCode":    200,
        "total_records": total,
        "skipped":       skipped,
        "summary":       summary
    }