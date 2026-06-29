import json
import os
import re
import typing
from functools import lru_cache
from urllib.parse import unquote_plus

import boto3

try:
    from nltk.corpus import stopwords
    from nltk.stem import SnowballStemmer, WordNetLemmatizer
    from nltk.tokenize import RegexpTokenizer
except Exception:
    stopwords = None
    SnowballStemmer = None
    WordNetLemmatizer = None
    RegexpTokenizer = None

if typing.TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_ssm import SSMClient


endpoint_url = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")

s3: "S3Client" = boto3.client("s3", endpoint_url=endpoint_url)
ssm: "SSMClient" = boto3.client("ssm", endpoint_url=endpoint_url)

FALLBACK_STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "be", "been", "but", "by",
    "for", "from", "had", "has", "have", "he", "her", "his", "i",
    "in", "is", "it", "its", "me", "my", "not", "of", "on", "or",
    "our", "she", "so", "that", "the", "their", "them", "there",
    "they", "this", "to", "too", "was", "we", "were", "with", "you",
    "your",
}

TOKENIZER = RegexpTokenizer(r"[A-Za-z]+") if RegexpTokenizer else None
STEMMER = SnowballStemmer("english") if SnowballStemmer else None
LEMMATIZER = WordNetLemmatizer() if WordNetLemmatizer else None
FALLBACK_TOKEN_SPLITTER = re.compile(r"[\s\d()\[\]{}.!?,;:+=\-_'\"`~#@&*%€$§\\/]+")


@lru_cache(maxsize=None)
def get_parameter(name: str) -> str:
    """Read a configuration value from SSM Parameter Store."""
    parameter = ssm.get_parameter(Name=name)
    return parameter["Parameter"]["Value"]


def get_output_bucket_name() -> str:
    return get_parameter("/assignment3/buckets/preprocessed")


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


def simple_lemma(token: str) -> str:
    """Use NLTK lemmatization when available; otherwise use a stable fallback."""
    if LEMMATIZER:
        try:
            return LEMMATIZER.lemmatize(token)
        except LookupError:
            pass

    if STEMMER:
        return STEMMER.stem(token)

    if len(token) > 4 and token.endswith("ies"):
        return token[:-3] + "y"
    if len(token) > 5 and token.endswith("ing"):
        return token[:-3]
    if len(token) > 4 and token.endswith("ed"):
        return token[:-2]
    if len(token) > 3 and token.endswith("s"):
        return token[:-1]
    return token


@lru_cache(maxsize=1)
def get_stopwords() -> frozenset[str]:
    """Use NLTK stopwords when available; otherwise use the local fallback list."""
    if stopwords:
        try:
            return frozenset(stopwords.words("english"))
        except LookupError:
            pass
    return frozenset(FALLBACK_STOPWORDS)


def preprocess_text(text: str) -> tuple[list[str], str]:
    """Tokenize, case-fold, remove stopwords, and apply NLTK-based normalization."""
    text = text.lower()
    if TOKENIZER:
        tokens = TOKENIZER.tokenize(text)
    else:
        tokens = FALLBACK_TOKEN_SPLITTER.split(text)

    stopword_set = get_stopwords()
    cleaned_tokens = []

    for token in tokens:
        token = token.strip()
        if len(token) <= 1 or token in stopword_set:
            continue
        cleaned_tokens.append(simple_lemma(token))

    return cleaned_tokens, " ".join(cleaned_tokens)


def read_review(bucket: str, key: str) -> dict:
    """Load one raw review JSON object from S3."""
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read().decode("utf-8")
    return json.loads(body)


def build_output_key(source_key: str) -> str:
    if source_key.endswith(".json"):
        return source_key
    return f"{source_key}.json"


def preprocess_review(review: dict) -> dict:
    """Build the downstream JSON payload for the next Lambda stage."""
    summary = str(review.get("summary", ""))
    review_text = str(review.get("reviewText", ""))
    combined_text = f"{summary} {review_text}"
    tokens, clean_text = preprocess_text(combined_text)

    return {
        "reviewerID": review.get("reviewerID"),
        "asin": review.get("asin"),
        "overall": review.get("overall"),
        "summary": summary,
        "reviewText": review_text,
        "tokens": tokens,
        "cleanText": clean_text,
    }


def handler(event, context):
    """S3-triggered Lambda entry point for preprocessing one or more reviews."""
    output_bucket = get_output_bucket_name()

    for record in iter_s3_records(event):
        source_bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])
        review = read_review(source_bucket, key)
        processed_review = preprocess_review(review)

        s3.put_object(
            Bucket=output_bucket,
            Key=build_output_key(key),
            Body=json.dumps(processed_review).encode("utf-8"),
            ContentType="application/json",
        )

    return {"statusCode": 200}
