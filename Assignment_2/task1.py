import json
import re
import sys
from operator import add
from pyspark import SparkConf, SparkContext

# Example usage locally: 
# spark-submit task1.py hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json output_rdd.txt stopwords.txt

# Example usage on a cluster:
# spark-submit --master yarn --deploy-mode cluster \
#   --files stopwords.txt \
#   task1.py \
#   hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json \
#   hdfs:///user/e01520348/output_rdd \
#   stopwords.txt

DEFAULT_INPUT_PATH = "hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json"
DEFAULT_OUTPUT_PATH = "output_rdd.txt"
DEFAULT_STOPWORDS_PATH = "stopwords.txt"


def parse_args(args):
    # Read CLI arguments or fall back to defaults.
    input_path = args[1] if len(args) > 1 else DEFAULT_INPUT_PATH
    output_path = args[2] if len(args) > 2 else DEFAULT_OUTPUT_PATH
    stopwords_path = args[3] if len(args) > 3 else DEFAULT_STOPWORDS_PATH
    return input_path, output_path, stopwords_path


 # Parse input/output configuration.
input_path, output_path, stopwords_path = parse_args(sys.argv)

# Create Spark context.
conf = SparkConf().setAppName("Assignment2")
sc = SparkContext.getOrCreate(conf=conf)

# Load stopwords and broadcast to workers.
with open(stopwords_path, "r", encoding="utf-8") as f:
    stopwords = set(line.strip().lower() for line in f if line.strip())

stopwords_bc = sc.broadcast(stopwords)

# Compile regex for tokenization.
# word_split_re = re.compile(r"[^a-zA-Z<>^|]+")
word_split_re = re.compile(r'[\s\d()\[\]{}.!?,;:+=\-_"\'`~#@&*%€$§\\/]+')


def parse_review(line):
    # Parse a JSON line to (category, review text).
    data = json.loads(line)
    return data["category"], data.get("reviewText", "")


def tokenize(text):
    # Tokenize text and remove stopwords.
    stopwords = stopwords_bc.value
    words = word_split_re.split(text.lower())

    return {
        w for w in words
        if len(w) > 1 and w not in stopwords
    }


def make_count_events(record):
    # Emit count events for totals, categories, words, and category-word pairs.
    category, text = record
    unique_words = tokenize(text)

    yield (("_n_",), 1)
    yield (("_cat_", category), 1)

    for word in unique_words:
        yield (("_w_", word), 1)
        yield (("_A_", category, word), 1)


# Load input data.
lines = sc.textFile(input_path)

# Aggregate all count events.
counts = (
    lines
    .map(parse_review)
    .flatMap(make_count_events)
    .reduceByKey(add)
    .cache()
)

# Total number of documents.
n = (
    counts
    .filter(lambda kv: kv[0][0] == "_n_")
    .map(lambda kv: kv[1])
    .first()
)

# Document counts per category.
cat_counts = (
    counts
    .filter(lambda kv: kv[0][0] == "_cat_")
    .map(lambda kv: (kv[0][1], kv[1]))
    .collectAsMap()
)

# Document counts per word.
word_counts = (
    counts
    .filter(lambda kv: kv[0][0] == "_w_")
    .map(lambda kv: (kv[0][1], kv[1]))
    .collectAsMap()
)

# Broadcast counts to workers for chi-square calculation.
n_bc = sc.broadcast(n)
cat_counts_bc = sc.broadcast(cat_counts)
word_counts_bc = sc.broadcast(word_counts)


def compute_chi2(kv):
    # Compute chi-square for a category-word pair.
    key, A = kv
    _, category, word = key

    n = n_bc.value
    n_c = cat_counts_bc.value[category]
    n_w = word_counts_bc.value[word]

    B = n_c - A
    C = n_w - A
    D = n - A - B - C

    denominator = (A + B) * (A + C) * (B + D) * (C + D)

    if denominator == 0:
        chi2 = 0.0
    else:
        chi2 = n * ((A * D - B * C) ** 2) / denominator

    return category, (word, chi2)


# Compute chi-square scores for category-word pairs.
chi2_scores = (
    counts
    .filter(lambda kv: kv[0][0] == "_A_")
    .map(compute_chi2)
)

# Select top terms per category by chi-square.
top_terms_per_category = (
    chi2_scores
    .groupByKey()
    .mapValues(lambda terms: sorted(terms, key=lambda x: (-x[1], x[0]))[:75])
    .sortByKey()
    .cache()
)

# Build the global dictionary from top terms.
dictionary_terms = (
    top_terms_per_category
    .flatMap(lambda kv: [word for word, score in kv[1]])
    .distinct()
    .sortBy(lambda w: w)
)

def format_category_line(kv):
    # Format a category line with term scores.
    category, terms = kv
    return category + " " + " ".join(
        f"{word}:{score}" for word, score in terms
    )


# Collect output lines.
category_lines = top_terms_per_category.map(format_category_line).collect()
dictionary_line = " ".join(dictionary_terms.collect())

# Combine lines for output.
output_lines = category_lines + [dictionary_line]

# Write output to HDFS or local file.
if output_path.startswith("hdfs://"):
    (
        sc.parallelize(output_lines, 1)
        .saveAsTextFile(output_path)
    )
else:
    with open(output_path, "w", encoding="utf-8") as f:
        for line in output_lines:
            f.write(line + "\n")

sc.stop()
