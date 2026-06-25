import hashlib
import json
import os
import typing
from decimal import Decimal
from urllib.parse import unquote_plus

import boto3

if typing.TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBServiceResource
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_ssm import SSMClient


endpoint_url = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")

s3: "S3Client" = boto3.client("s3", endpoint_url=endpoint_url)
ssm: "SSMClient" = boto3.client("ssm", endpoint_url=endpoint_url)
dynamodb: "DynamoDBServiceResource" = boto3.resource("dynamodb", endpoint_url=endpoint_url)

# This Lambda is triggered by new objects in the profanity-checked review bucket.
# It classifies the review sentiment using preprocessed tokens plus the overall
# rating, stores compact metadata in DynamoDB, and writes the final analyzed
# review JSON to the analyzed review bucket.
POSITIVE_WORDS = {
    "amazing", "awesome", "best", "delight", "easy", "excellent", "fun",
    "good", "great", "happy", "like", "liked", "love", "loved", "nice",
    "perfect", "recommend", "useful", "wonderful",
}

NEGATIVE_WORDS = {
    "awful", "bad", "broken", "boring", "cheap", "disappointing", "hate",
    "hated", "poor", "problem", "terrible", "useless", "waste", "worst",
    "wrong",
}


def get_parameter(name: str) -> str:
    """Read a bucket or table name from SSM Parameter Store."""
    parameter = ssm.get_parameter(Name=name)
    return parameter["Parameter"]["Value"]


def iter_s3_records(event):
    """Normalize MiniStack/AWS S3 event shapes to a list of S3 records."""
    if isinstance(event, dict):
        if event.get("Event") == "s3:TestEvent":
            return []
        if isinstance(event.get("Records"), list):
            return event["Records"]
        if "s3" in event:
            return [event]
    if isinstance(event, list):
        return event
    raise ValueError(f"Unsupported S3 event payload: {event!r}")


def read_json_from_s3(bucket: str, key: str) -> dict:
    """Load one profanity-checked review JSON object from S3."""
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read().decode("utf-8")
    return json.loads(body)


def classify_sentiment(review: dict) -> str:
    """Classify sentiment as positive, neutral, or negative."""
    tokens = review.get("tokens", [])
    lexical_score = 0
    for token in tokens:
        if token in POSITIVE_WORDS:
            lexical_score += 1
        if token in NEGATIVE_WORDS:
            lexical_score -= 1

    try:
        overall = float(review.get("overall", 0))
    except (TypeError, ValueError):
        overall = 0

    if overall >= 4:
        rating_score = 1
    elif overall <= 2 and overall > 0:
        rating_score = -1
    else:
        rating_score = 0

    total_score = lexical_score + rating_score
    if total_score > 0:
        return "positive"
    if total_score < 0:
        return "negative"
    return "neutral"


def make_review_id(key: str, review: dict) -> str:
    """Create a stable DynamoDB partition key for the result item."""
    stable_text = json.dumps(
        {
            "key": key,
            "reviewerID": review.get("reviewerID"),
            "asin": review.get("asin"),
            "summary": review.get("summary"),
        },
        sort_keys=True,
    )
    return hashlib.sha1(stable_text.encode("utf-8")).hexdigest()


def store_result(key: str, review: dict, sentiment: str) -> None:
    """Persist compact result metadata in the results DynamoDB table."""
    results_table = dynamodb.Table(get_parameter("/assignment3/tables/results"))
    item = {
        "reviewID": make_review_id(key, review),
        "sourceKey": key,
        "reviewerID": str(review.get("reviewerID", "UNKNOWN")),
        "asin": str(review.get("asin", "")),
        "overall": Decimal(str(review.get("overall", 0))),
        "sentiment": sentiment,
        "profanityPassed": bool(review.get("profanityPassed", True)),
        "impolite": bool(review.get("impolite", False)),
        "bannedAtProcessingTime": bool(review.get("userStatus", {}).get("banned", False)),
    }
    results_table.put_item(Item=item)


def handler(event, context):
    """S3-triggered Lambda entry point for sentiment analysis and result storage."""
    final_bucket = get_parameter("/assignment3/buckets/analyzed")

    for record in iter_s3_records(event):
        source_bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])
        review = read_json_from_s3(source_bucket, key)
        sentiment = classify_sentiment(review)

        review["sentiment"] = sentiment
        store_result(key, review, sentiment)

        s3.put_object(
            Bucket=final_bucket,
            Key=key,
            Body=json.dumps(review).encode("utf-8"),
            ContentType="application/json",
        )

    return {"statusCode": 200}
