import json
import os
import time
import typing

import boto3
import pytest

if typing.TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBServiceResource
    from mypy_boto3_lambda import LambdaClient
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_ssm import SSMClient


os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "test"
os.environ["AWS_SECRET_ACCESS_KEY"] = "test"

s3: "S3Client" = boto3.client("s3", endpoint_url="http://localhost:4566")
ssm: "SSMClient" = boto3.client("ssm", endpoint_url="http://localhost:4566")
awslambda: "LambdaClient" = boto3.client("lambda", endpoint_url="http://localhost:4566")
dynamodb: "DynamoDBServiceResource" = boto3.resource(
    "dynamodb", endpoint_url="http://localhost:4566"
)


@pytest.fixture(autouse=True)
def _wait_for_lambdas():
    awslambda.get_waiter("function_active").wait(FunctionName="preprocess")
    awslambda.get_waiter("function_active").wait(FunctionName="profanity_check")
    awslambda.get_waiter("function_active").wait(FunctionName="sentiment_analysis")


def get_parameter(name: str) -> str:
    parameter = ssm.get_parameter(Name=name)
    return parameter["Parameter"]["Value"]


def wait_for_s3_json(bucket: str, key: str, timeout_seconds: int = 20) -> dict:
    end_time = time.time() + timeout_seconds
    while time.time() < end_time:
        try:
            response = s3.get_object(Bucket=bucket, Key=key)
            return json.loads(response["Body"].read().decode("utf-8"))
        except Exception:
            time.sleep(0.5)
    raise AssertionError(f"Object {bucket}/{key} was not created in time")


def test_review_pipeline_preprocess_profanity_sentiment_and_ban():
    raw_bucket = get_parameter("/assignment3/buckets/raw")
    analyzed_bucket = get_parameter("/assignment3/buckets/analyzed")
    users_table_name = get_parameter("/assignment3/tables/users")
    results_table_name = get_parameter("/assignment3/tables/results")

    users_table = dynamodb.Table(users_table_name)
    results_table = dynamodb.Table(results_table_name)

    reviewer_id = "TEST_USER_BANNED"

    for number in range(1, 5):
        key = f"test-review-{number}.json"
        review = {
            "reviewerID": reviewer_id,
            "asin": f"TEST_ASIN_{number}",
            "reviewerName": "Integration Test",
            "helpful": [0, 0],
            "reviewText": "This product is awful and stupid shit.",
            "overall": 1.0,
            "summary": "Worst product",
            "unixReviewTime": 1700000000,
            "reviewTime": "01 1, 2026",
            "category": "Test",
        }

        s3.put_object(
            Bucket=raw_bucket,
            Key=key,
            Body=json.dumps(review).encode("utf-8"),
            ContentType="application/json",
        )

        analyzed = wait_for_s3_json(analyzed_bucket, key)

        assert "tokens" in analyzed
        assert "cleanText" in analyzed
        assert "the" not in analyzed["tokens"]
        assert analyzed["reviewerID"] == reviewer_id
        assert analyzed["profanityPassed"] is False
        assert analyzed["impolite"] is True
        assert analyzed["sentiment"] == "negative"

    user_response = users_table.get_item(Key={"reviewerID": reviewer_id})
    assert user_response["Item"]["impoliteReviewCount"] == 4
    assert user_response["Item"]["banned"] is True

    scan_response = results_table.scan()
    matching_items = [
        item for item in scan_response["Items"] if item["reviewerID"] == reviewer_id
    ]
    assert len(matching_items) >= 4
