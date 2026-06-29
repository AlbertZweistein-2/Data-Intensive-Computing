import json
import os
import re
import typing
from decimal import Decimal
from functools import lru_cache
from urllib.parse import unquote_plus

import boto3

try:
    from profanityfilter import ProfanityFilter
except Exception:
    ProfanityFilter = None

if typing.TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBServiceResource
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_ssm import SSMClient


endpoint_url = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")

s3: "S3Client" = boto3.client("s3", endpoint_url=endpoint_url)
ssm: "SSMClient" = boto3.client("ssm", endpoint_url=endpoint_url)
dynamodb: "DynamoDBServiceResource" = boto3.resource("dynamodb", endpoint_url=endpoint_url)

# This Lambda is triggered by new objects in the preprocessed review bucket.
# It checks summary/reviewText/cleanText for profanity, updates the per-user
# impolite review counter in DynamoDB, marks users as banned after more than
# three impolite reviews, and forwards the review to the next S3 bucket.
EXTRA_BAD_WORDS = {
    "asshole",
    "bastard",
    "bitch",
    "crap",
    "damn",
    "fuck",
    "fucking",
    "idiot",
    "moron",
    "shit",
    "stupid",
}
PROFANITY_FILTER = None
WORD_RE = re.compile(r"[a-zA-Z]+")


@lru_cache(maxsize=None)
def get_parameter(name: str) -> str:
    """Read a bucket or table name from SSM Parameter Store."""
    parameter = ssm.get_parameter(Name=name)
    return parameter["Parameter"]["Value"]


@lru_cache(maxsize=1)
def get_users_table():
    return dynamodb.Table(get_parameter("/assignment3/tables/users"))


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
    """Load one preprocessed review JSON object from S3."""
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read().decode("utf-8")
    return json.loads(body)


def contains_profanity(text: str) -> bool:
    """Return True when profanityfilter or the fallback dictionary finds profanity."""
    global PROFANITY_FILTER

    tokens = WORD_RE.findall(text.lower())
    if any(token in EXTRA_BAD_WORDS for token in tokens):
        return True

    if ProfanityFilter:
        try:
            if PROFANITY_FILTER is None:
                PROFANITY_FILTER = ProfanityFilter(
                    extra_censor_list=list(EXTRA_BAD_WORDS)
                )
            return PROFANITY_FILTER.is_profane(text)
        except Exception:
            pass

    return False


def update_user_status(reviewer_id: str, is_impolite: bool) -> dict:
    """Update the user-level impolite review counter and ban flag."""
    users_table = get_users_table()

    if not reviewer_id:
        reviewer_id = "UNKNOWN"

    if is_impolite:
        response = users_table.update_item(
            Key={"reviewerID": reviewer_id},
            UpdateExpression=(
                "ADD impoliteReviewCount :one "
                "SET banned = if_not_exists(banned, :false)"
            ),
            ExpressionAttributeValues={
                ":one": Decimal(1),
                ":false": False,
            },
            ReturnValues="ALL_NEW",
        )
        item = response["Attributes"]
    else:
        response = users_table.update_item(
            Key={"reviewerID": reviewer_id},
            UpdateExpression=(
                "SET impoliteReviewCount = if_not_exists(impoliteReviewCount, :zero), "
                "banned = if_not_exists(banned, :false)"
            ),
            ExpressionAttributeValues={
                ":zero": Decimal(0),
                ":false": False,
            },
            ReturnValues="ALL_NEW",
        )
        item = response["Attributes"]

    count = int(item.get("impoliteReviewCount", 0))
    if count > 3 and not item.get("banned", False):
        response = users_table.update_item(
            Key={"reviewerID": reviewer_id},
            UpdateExpression="SET banned = :true",
            ExpressionAttributeValues={":true": True},
            ReturnValues="ALL_NEW",
        )
        item = response["Attributes"]

    return {
        "reviewerID": reviewer_id,
        "impoliteReviewCount": int(item.get("impoliteReviewCount", 0)),
        "banned": bool(item.get("banned", False)),
    }


def handler(event, context):
    """S3-triggered Lambda entry point for profanity checking and ban logic."""
    output_bucket = get_parameter("/assignment3/buckets/profanity_checked")

    for record in iter_s3_records(event):
        source_bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])
        review = read_json_from_s3(source_bucket, key)

        text_for_check = " ".join(
            [
                str(review.get("summary", "")),
                str(review.get("reviewText", "")),
                str(review.get("cleanText", "")),
            ]
        )
        is_impolite = contains_profanity(text_for_check)
        user_status = update_user_status(str(review.get("reviewerID", "UNKNOWN")), is_impolite)

        review["profanityPassed"] = not is_impolite
        review["impolite"] = is_impolite
        review["userStatus"] = user_status

        s3.put_object(
            Bucket=output_bucket,
            Key=key,
            Body=json.dumps(review).encode("utf-8"),
            ContentType="application/json",
        )

    return {"statusCode": 200}
