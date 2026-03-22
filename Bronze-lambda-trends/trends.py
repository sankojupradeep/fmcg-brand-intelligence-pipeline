import json
import boto3
import os
import time
from pytrends.request import TrendReq
from datetime import datetime, timezone

BRAND_BATCHES = [
    {
        "batch_id": 1,
        "category": "Beverages",
        "brands": [
            {"name": "Coca-Cola",  "type": "listed",  "term": "Coca Cola India"},
            {"name": "PepsiCo",    "type": "listed",  "term": "Pepsi India"},
            {"name": "Thums Up",   "type": "private", "term": "Thums Up"},
            {"name": "Limca",      "type": "private", "term": "Limca drink"}
        ]
    },
    {
        "batch_id": 2,
        "category": "Health",
        "brands": [
            {"name": "HUL",        "type": "listed",  "term": "Hindustan Unilever"},
            {"name": "Dabur",      "type": "listed",  "term": "Dabur India"},
            {"name": "Himalaya",   "type": "private", "term": "Himalaya herbals"},
            {"name": "Patanjali",  "type": "private", "term": "Patanjali products"}
        ]
    },
    {
        "batch_id": 3,
        "category": "Snacks",
        "brands": [
            {"name": "ITC",        "type": "listed",  "term": "ITC Bingo"},
            {"name": "Haldirams",  "type": "private", "term": "Haldiram India"},
            {"name": "Kurkure",    "type": "private", "term": "Kurkure chips"},
            {"name": "Lays",       "type": "private", "term": "Lays India"}
        ]
    }
]


def already_ingested(s3_client, bucket, category, batch_id, today):
    """
    Check if today's trends batch already exists.
    Trends data updates weekly — running twice same day
    gives identical data, no point re-fetching.
    """
    prefix = (
        f"bronze/trends/"
        f"category={category}/"
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


def init_pytrends():
    return TrendReq(
        hl="en-IN",
        tz=330,
        timeout=(10, 25)
    )


def fetch_batch_trends(pytrends, batch):
    run_timestamp = datetime.now(timezone.utc).isoformat()
    category      = batch["category"]
    brands        = batch["brands"]
    kw_list       = [b["term"] for b in brands]
    term_to_brand = {b["term"]: b for b in brands}

    try:
        pytrends.build_payload(
            kw_list=kw_list,
            timeframe="today 1-m",
            geo="IN",
            cat=0
        )

        df = pytrends.interest_over_time()

        if df.empty:
            print(f"  No trends data for batch {batch['batch_id']}")
            return []

        if "isPartial" in df.columns:
            df = df.drop(columns=["isPartial"])

        records = []

        for date, row in df.iterrows():
            for term in kw_list:
                if term not in row:
                    continue

                brand_meta  = term_to_brand[term]
                trend_score = int(row[term])

                records.append({
                    
                    "brand_name":      brand_meta["name"],
                    "brand_type":      brand_meta["type"],
                    "category":        category,
                    "search_term":     term,

                    
                    "date":            date.strftime("%Y-%m-%d"),
                    "trend_score":     trend_score,

                   
                    "timeframe":       "today 1-m",
                    "geo":             "IN",
                    "ingested_at":     run_timestamp,
                    "pipeline_source": "google_trends",
                    "data_layer":      "bronze",
                    "stock_available": brand_meta["type"] == "listed"
                })

        print(f"  Batch {batch['batch_id']} → {len(records)} records")
        return records

    except Exception as e:
        print(f"  Error batch {batch['batch_id']}: {str(e)}")
        return []


def write_to_s3(s3_client, data, category, batch_id, today):
    if not data:
        print(f"  No data for batch {batch_id} — skipping")
        return 0

    s3_key = (
        f"bronze/trends/"
        f"category={category}/"
        f"year={today.year}/"
        f"month={today.month:02d}/"
        f"day={today.day:02d}/"
        f"trends_batch{batch_id}_{today.strftime('%Y%m%d_%H%M%S')}.json"
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
    pytrends  = init_pytrends()
    total     = 0
    summary   = []
    skipped   = []

    for batch in BRAND_BATCHES:
        print(f"\nBatch {batch['batch_id']} — {batch['category']}")

        if already_ingested(
            s3_client, bucket,
            batch["category"],
            batch["batch_id"],
            today
        ):
            print(f"  Batch {batch['batch_id']} already ingested today — skipping")
            skipped.append(batch["category"])
            continue

        records = fetch_batch_trends(pytrends, batch)

        count = write_to_s3(
            s3_client, records,
            batch["category"],
            batch["batch_id"],
            today
        )

        total += count
        summary.append({
            "batch_id": batch["batch_id"],
            "category": batch["category"],
            "records":  count,
            "date":     today.strftime("%Y-%m-%d")
        })

        if batch != BRAND_BATCHES[-1]:
            print("  Waiting 10s before next batch...")
            time.sleep(10)

    print(f"\nRun complete — {total} records written")
    print(f"Skipped: {skipped}")

    return {
        "statusCode":    200,
        "total_records": total,
        "skipped":       skipped,
        "summary":       summary
    }