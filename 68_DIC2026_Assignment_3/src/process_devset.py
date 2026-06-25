import argparse
import json
import os
import time
import typing
from decimal import Decimal

import boto3

if typing.TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBServiceResource
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_ssm import SSMClient


os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

ENDPOINT_URL = "http://localhost:4566"

s3: "S3Client" = boto3.client("s3", endpoint_url=ENDPOINT_URL)
ssm: "SSMClient" = boto3.client("ssm", endpoint_url=ENDPOINT_URL)
dynamodb: "DynamoDBServiceResource" = boto3.resource("dynamodb", endpoint_url=ENDPOINT_URL)


def get_parameter(name: str) -> str:
    parameter = ssm.get_parameter(Name=name)
    return parameter["Parameter"]["Value"]


def list_object_keys(bucket: str, prefix: str = "") -> list[str]:
    keys = []
    continuation_token = None

    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token
        response = s3.list_objects_v2(**kwargs)

        for item in response.get("Contents", []):
            keys.append(item["Key"])

        if not response.get("IsTruncated"):
            return keys
        continuation_token = response.get("NextContinuationToken")


def clear_bucket(bucket: str) -> None:
    keys = list_object_keys(bucket)
    if not keys:
        return

    s3.delete_objects(
        Bucket=bucket,
        Delete={"Objects": [{"Key": key} for key in keys]},
    )


def clear_table(table_name: str, key_name: str) -> None:
    table = dynamodb.Table(table_name)
    response = table.scan()

    with table.batch_writer() as batch:
        for item in response.get("Items", []):
            batch.delete_item(Key={key_name: item[key_name]})

        while "LastEvaluatedKey" in response:
            response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            for item in response.get("Items", []):
                batch.delete_item(Key={key_name: item[key_name]})


def decimal_to_builtin(value):
    if isinstance(value, Decimal):
        if value % 1 == 0:
            return int(value)
        return float(value)
    if isinstance(value, list):
        return [decimal_to_builtin(item) for item in value]
    if isinstance(value, dict):
        return {key: decimal_to_builtin(item) for key, item in value.items()}
    return value


def read_reviews(path: str, limit: int | None) -> list[dict]:
    reviews = []

    with open(path, "r", encoding="utf-8") as file:
        first_non_empty = ""
        while not first_non_empty:
            first_non_empty = file.readline()
            if first_non_empty == "":
                return []
            first_non_empty = first_non_empty.strip()

        file.seek(0)
        if first_non_empty.startswith("["):
            data = json.load(file)
            reviews = data if isinstance(data, list) else [data]
        else:
            for line in file:
                line = line.strip()
                if not line:
                    continue
                reviews.append(json.loads(line))
                if limit and len(reviews) >= limit:
                    break

    if limit:
        return reviews[:limit]
    return reviews


def wait_for_analyzed_objects(bucket: str, expected_count: int, prefix: str, timeout_seconds: int) -> None:
    end_time = time.time() + timeout_seconds

    while time.time() < end_time:
        current_count = len(list_object_keys(bucket, prefix))
        print(f"Analyzed reviews: {current_count}/{expected_count}", end="\r")
        if current_count >= expected_count:
            print()
            return
        time.sleep(2)

    current_count = len(list_object_keys(bucket, prefix))
    raise TimeoutError(
        f"Only {current_count}/{expected_count} analyzed reviews appeared in time."
    )


def scan_table(table_name: str) -> list[dict]:
    table = dynamodb.Table(table_name)
    items = []
    response = table.scan()
    items.extend(response.get("Items", []))

    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items.extend(response.get("Items", []))

    return [decimal_to_builtin(item) for item in items]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("reviews_path", help="Path to reviews_devset.json")
    parser.add_argument("--limit", type=int, default=None, help="Optional limit for a quick test run")
    parser.add_argument("--timeout", type=int, default=900, help="Maximum wait time in seconds")
    args = parser.parse_args()

    raw_bucket = get_parameter("/assignment3/buckets/raw")
    preprocessed_bucket = get_parameter("/assignment3/buckets/preprocessed")
    profanity_bucket = get_parameter("/assignment3/buckets/profanity_checked")
    analyzed_bucket = get_parameter("/assignment3/buckets/analyzed")
    users_table = get_parameter("/assignment3/tables/users")
    results_table = get_parameter("/assignment3/tables/results")

    print("Clearing previous local MiniStack state for a clean devset run...")
    for bucket in [raw_bucket, preprocessed_bucket, profanity_bucket, analyzed_bucket]:
        clear_bucket(bucket)
    clear_table(users_table, "reviewerID")
    clear_table(results_table, "reviewID")

    reviews = read_reviews(args.reviews_path, args.limit)
    if not reviews:
        raise ValueError("No reviews found.")

    prefix = "devset/"
    reviewer_ids = {str(review.get("reviewerID", "UNKNOWN")) for review in reviews}

    print(f"Uploading {len(reviews)} reviews...")
    for index, review in enumerate(reviews, start=1):
        key = f"{prefix}review_{index:06d}.json"
        s3.put_object(
            Bucket=raw_bucket,
            Key=key,
            Body=json.dumps(review).encode("utf-8"),
            ContentType="application/json",
        )
        if index % 100 == 0:
            print(f"Uploaded {index}/{len(reviews)}")

    print("Waiting for the Lambda chain to finish...")
    wait_for_analyzed_objects(analyzed_bucket, len(reviews), prefix, args.timeout)

    result_items = [
        item for item in scan_table(results_table)
        if str(item.get("sourceKey", "")).startswith(prefix)
    ]
    user_items = [
        item for item in scan_table(users_table)
        if str(item.get("reviewerID", "UNKNOWN")) in reviewer_ids
    ]

    sentiment_counts = {"positive": 0, "neutral": 0, "negative": 0}
    failed_profanity_count = 0

    for item in result_items:
        sentiment = item.get("sentiment", "neutral")
        sentiment_counts[sentiment] = sentiment_counts.get(sentiment, 0) + 1
        if not item.get("profanityPassed", True):
            failed_profanity_count += 1

    banned_users = sorted(
        str(item["reviewerID"])
        for item in user_items
        if item.get("banned", False)
    )

    output = {
        "processed_reviews": len(result_items),
        "positive_reviews": sentiment_counts.get("positive", 0),
        "neutral_reviews": sentiment_counts.get("neutral", 0),
        "negative_reviews": sentiment_counts.get("negative", 0),
        "failed_profanity_reviews": failed_profanity_count,
        "banned_users": banned_users,
    }

    with open("devset_results.json", "w", encoding="utf-8") as file:
        json.dump(output, file, indent=2)

    print("\nResults for report:")
    print(json.dumps(output, indent=2))
    print("\nSaved to devset_results.json")


if __name__ == "__main__":
    main()
