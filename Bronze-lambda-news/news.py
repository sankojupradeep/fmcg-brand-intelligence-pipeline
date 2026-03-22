import json
import boto3
import os
import urllib.request
import urllib.parse
from datetime import datetime, timezone

BRANDS = {
    "Beverages": {
        "Coca-Cola": {
            "type": "listed",
            "ticker": "KO",
            "search_terms": ["Coca Cola India", "Coke India"]
        },
        "PepsiCo": {
            "type": "listed",
            "ticker": "PEP",
            "search_terms": ["Pepsi India", "PepsiCo India"]
        },
        "Thums Up": {
            "type": "private",
            "ticker": None,
            "search_terms": ["Thums Up"]
        },
        "Limca": {
            "type": "private",
            "ticker": None,
            "search_terms": ["Limca drink India"]
        }
    },
    "Health": {
        "HUL": {
            "type": "listed",
            "ticker": "HINDUNILVR",
            "search_terms": ["Hindustan Unilever", "HUL India"]
        },
        "Dabur": {
            "type": "listed",
            "ticker": "DABUR",
            "search_terms": ["Dabur India"]
        },
        "Himalaya": {
            "type": "private",
            "ticker": None,
            "search_terms": ["Himalaya Drug Company", "Himalaya herbals"]
        },
        "Patanjali": {
            "type": "private",
            "ticker": None,
            "search_terms": ["Patanjali products", "Patanjali India"]
        }
    },
    "Snacks": {
        "ITC": {
            "type": "listed",
            "ticker": "ITC",
            "search_terms": ["ITC snacks India", "Bingo chips"]
        },
        "Haldirams": {
            "type": "private",
            "ticker": None,
            "search_terms": ["Haldiram India", "Haldirams snacks"]
        },
        "Kurkure": {
            "type": "private",
            "ticker": None,
            "search_terms": ["Kurkure chips India"]
        },
        "Lays": {
            "type": "private",
            "ticker": None,
            "search_terms": ["Lays India", "Lays chips India"]
        }
    }
}


def already_ingested(s3_client, bucket, brand_name, category, today):
    """
    Check if today's Bronze partition already exists.
    If yes — skip API call entirely.
    Makes Lambda idempotent — safe to trigger multiple times per day.
    """
    prefix = (
        f"bronze/news/"
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


def fetch_news(brand_name, search_terms, brand_meta):
    api_key       = os.environ["NEWSAPIKEY"]   
    run_timestamp = datetime.now(timezone.utc).isoformat()
    all_articles  = []

    for term in search_terms:
        try:
            params = urllib.parse.urlencode({
                "q":        term,
                "language": "en",
                "pageSize": 20,
                "apiKey":   api_key
            })

            url = f"https://newsapi.org/v2/top-headlines?{params}"
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "fmcg-brand-monitor/1.0"}
            )

            with urllib.request.urlopen(req, timeout=10) as response:
                data = json.loads(response.read().decode("utf-8"))

            # Fallback — everything endpoint without date filter
            if data.get("status") != "ok" or len(data.get("articles", [])) == 0:
                params2 = urllib.parse.urlencode({
                    "q":        term,
                    "language": "en",
                    "sortBy":   "publishedAt",
                    "pageSize": 20,
                    "apiKey":   api_key
                })
                url2 = f"https://newsapi.org/v2/everything?{params2}"
                req2 = urllib.request.Request(
                    url2,
                    headers={"User-Agent": "fmcg-brand-monitor/1.0"}
                )
                with urllib.request.urlopen(req2, timeout=10) as r2:
                    data = json.loads(r2.read().decode("utf-8"))

            articles = data.get("articles", [])
            print(f"  {term} → {len(articles)} articles")

            for article in articles:
                if article.get("title") == "[Removed]":
                    continue
                if not article.get("title"):
                    continue

                all_articles.append({
                    "brand_name":      brand_name,
                    "brand_type":      brand_meta["type"],
                    "ticker":          brand_meta["ticker"],
                    "category":        brand_meta.get("category"),
                    "search_term":     term,


                    "article_id":      hash(
                                           article.get("url", "") +
                                           article.get("publishedAt", "")
                                       ),
                    "headline":        article.get("title"),
                    "description":     article.get("description"),
                    "source_name":     article.get("source", {}).get("name"),
                    "author":          article.get("author"),
                    "url":             article.get("url"),
                    "published_at":    article.get("publishedAt"),

                    "ingested_at":     run_timestamp,
                    "pipeline_source": "newsapi",
                    "data_layer":      "bronze"
                })

        except Exception as e:
            print(f"  Error fetching {term}: {str(e)}")
            continue
    seen_urls = set()
    unique    = []
    for a in all_articles:
        url = a.get("url")
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique.append(a)

    return unique


def write_to_s3(s3_client, data, brand_name, category, today):
    if not data:
        print(f"  No articles for {brand_name} — skipping")
        return 0

    s3_key = (
        f"bronze/news/"
        f"category={category}/"
        f"brand={brand_name}/"
        f"year={today.year}/"
        f"month={today.month:02d}/"
        f"day={today.day:02d}/"
        f"news_{brand_name}_{today.strftime('%Y%m%d_%H%M%S')}.json"
    )

    body = "\n".join(json.dumps(r) for r in data)

    s3_client.put_object(
        Bucket=os.environ["S3BUCKETNAME"],        
        Key=s3_key,
        Body=body.encode("utf-8"),
        ContentType="application/json"
    )

    print(f"  Written {len(data)} → {s3_key}")
    return len(data)


def lambda_handler(event, context):
    s3_client = boto3.client("s3")
    bucket    = os.environ["S3BUCKETNAME"]        
    today     = datetime.now(timezone.utc)
    total     = 0
    summary   = []
    skipped   = []

    for category, brands in BRANDS.items():
        print(f"\nCategory: {category}")

        for brand_name, brand_meta in brands.items():
            brand_meta["category"] = category
            if already_ingested(
                s3_client, bucket,
                brand_name, category, today
            ):
                print(f"  {brand_name} already ingested today — skipping")
                skipped.append(brand_name)
                continue

            print(f"  Fetching news for {brand_name}...")
            articles = fetch_news(
                brand_name,
                brand_meta["search_terms"],
                brand_meta
            )

            count = write_to_s3(
                s3_client, articles,
                brand_name, category, today
            )

            total += count
            summary.append({
                "brand":    brand_name,
                "category": category,
                "articles": count,
                "date":     today.strftime("%Y-%m-%d")
            })

    return {
        "statusCode":     200,
        "total_articles": total,
        "skipped":        skipped,
        "summary":        summary
    }
