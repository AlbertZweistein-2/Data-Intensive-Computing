import argparse
import importlib.util
import json
import os
import re
from collections import defaultdict
from importlib import resources
from pathlib import Path


os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module {name} from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def read_reviews(path: Path, limit: int | None) -> list[dict]:
    reviews = []

    with path.open("r", encoding="utf-8") as file:
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


def load_profanity_words(extra_words: set[str]) -> set[str]:
    words = set(extra_words)

    try:
        badwords_path = resources.files("profanityfilter").joinpath("data/badwords.txt")
        with badwords_path.open("r", encoding="utf-8") as file:
            words.update(
                line.strip().lower()
                for line in file
                if line.strip() and not line.startswith("#")
            )
    except Exception:
        pass

    return words


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("reviews_path", help="Path to reviews_devset.json")
    parser.add_argument("--limit", type=int, default=None, help="Optional limit for a quick test run")
    parser.add_argument("--output", default="devset_results_offline.json", help="Output JSON path")
    parser.add_argument(
        "--use-profanityfilter",
        action="store_true",
        help="Use the slower profanityfilter library path instead of the direct bad-word dictionary",
    )
    parser.add_argument(
        "--use-nltk",
        action="store_true",
        help="Use locally installed NLTK preprocessing instead of the Lambda package fallback",
    )
    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parent
    preprocess = load_module(
        "offline_preprocess",
        base_dir / "lambdas" / "preprocess" / "handler.py",
    )
    if not args.use_nltk:
        preprocess.stopwords = None
        preprocess.TOKENIZER = None
        preprocess.STEMMER = None
        preprocess.LEMMATIZER = None
        preprocess.get_stopwords.cache_clear()

    profanity = load_module(
        "offline_profanity",
        base_dir / "lambdas" / "profanity_check" / "handler.py",
    )
    sentiment = load_module(
        "offline_sentiment",
        base_dir / "lambdas" / "sentiment_analysis" / "handler.py",
    )

    reviews = read_reviews(Path(args.reviews_path), args.limit)
    if not reviews:
        raise ValueError("No reviews found.")

    sentiment_counts = {"positive": 0, "neutral": 0, "negative": 0}
    failed_profanity_reviews = 0
    impolite_counts: defaultdict[str, int] = defaultdict(int)
    banned_users = set()
    word_re = re.compile(r"[a-zA-Z]+")
    profanity_words = load_profanity_words(profanity.EXTRA_BAD_WORDS)

    for index, review in enumerate(reviews, start=1):
        processed = preprocess.preprocess_review(review)

        text_for_check = " ".join(
            [
                str(processed.get("summary", "")),
                str(processed.get("reviewText", "")),
                str(processed.get("cleanText", "")),
            ]
        )
        if args.use_profanityfilter:
            is_impolite = profanity.contains_profanity(text_for_check)
        else:
            tokens = word_re.findall(text_for_check.lower())
            is_impolite = any(token in profanity_words for token in tokens)
        reviewer_id = str(processed.get("reviewerID", "UNKNOWN"))

        if is_impolite:
            failed_profanity_reviews += 1
            impolite_counts[reviewer_id] += 1
            if impolite_counts[reviewer_id] > 3:
                banned_users.add(reviewer_id)

        processed["profanityPassed"] = not is_impolite
        processed["impolite"] = is_impolite
        processed["userStatus"] = {
            "reviewerID": reviewer_id,
            "impoliteReviewCount": impolite_counts[reviewer_id],
            "banned": reviewer_id in banned_users,
        }

        label = sentiment.classify_sentiment(processed)
        sentiment_counts[label] = sentiment_counts.get(label, 0) + 1

        if index % 10000 == 0:
            print(f"Processed {index}/{len(reviews)} reviews")

    output = {
        "processed_reviews": len(reviews),
        "positive_reviews": sentiment_counts.get("positive", 0),
        "neutral_reviews": sentiment_counts.get("neutral", 0),
        "negative_reviews": sentiment_counts.get("negative", 0),
        "failed_profanity_reviews": failed_profanity_reviews,
        "banned_users": sorted(banned_users),
    }

    with open(args.output, "w", encoding="utf-8") as file:
        json.dump(output, file, indent=2)

    print(json.dumps(output, indent=2))
    print(f"Saved to {args.output}")


if __name__ == "__main__":
    main()
