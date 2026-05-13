#!/usr/bin/env python3

import json
import time
import re
import heapq

from collections import defaultdict

from pyspark import SparkContext, SparkConf,StorageLevel



# -----------------------------------------------------
# CONSTANTS
# -----------------------------------------------------

DEV_PATH = "hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json"

FULL_PATH = "hdfs:///dic_shared/amazon-reviews/full/reviewscombined.json"

STOPWORDS_FILE = "stopwords.txt"

TOP_K = 75

TOKEN_PATTERN = re.compile(
    r"[ \t\d\(\)\[\]\{\}\.\!\?,;:\+=\-_'\"`~#@&\*%€\$\§\\\/<>^|]+"
)


# -----------------------------------------------------
# LOAD STOPWORDS
# -----------------------------------------------------

def load_stopwords(path):

    sw = set()

    with open(path, "r") as f:

        for line in f:

            w = line.strip().lower()

            if w:
                sw.add(w)

    return sw


# -----------------------------------------------------
# TOKENIZE
# -----------------------------------------------------

def tokenize(text, stopwords):

    if text is None:
        return []

    tokens = TOKEN_PATTERN.split(text.lower())

    cleaned = [
        token
        for token in tokens
        if (
            len(token) > 2
            and token.isalpha()
            and token not in stopwords
        )
    ]

    return cleaned




def local_term_category_count(iterator, stopwords):

    local_counts = defaultdict(int)

    for line in iterator:

        try:
            data = json.loads(line)

            category = data["category"]

            words = tokenize(
                data["reviewText"],
                stopwords
            )

            for word in words:

                local_counts[(word, category)] += 1

        except Exception:
            continue

    for key, count in local_counts.items():
        yield (key, count)


# -----------------------------------------------------
# MAIN
# -----------------------------------------------------

def main(input_path, output_path, stopword_path):

    t0 = time.time()

    print("====================================")
    print("ASSIGNMENT 2 PART 1 - SPARK RDD")
    print("====================================")

    print("INPUT:", input_path)
    print("OUTPUT:", output_path)

    stopwords = load_stopwords(stopword_path)

    conf = SparkConf().setAppName("A2-Part1-RDD")

    sc = SparkContext(conf=conf)

    bc_stop = sc.broadcast(stopwords)

    # -------------------------------------------------
    # LOAD DATA
    # -------------------------------------------------

    print("Loading dataset...")

    data = sc.textFile(input_path)

    print("Dataset loaded")



    print("Running local partition aggregation...")

    term_cat_count = (
        data.mapPartitions(
            lambda part: local_term_category_count(
                part,
                bc_stop.value
            )
        )
        .reduceByKey(lambda a, b: a + b)
    )

    # -------------------------------------------------
    # RELOAD JSON DATA
    # (needed for category totals)
    # -------------------------------------------------

    parsed_data = data.map(lambda x: json.loads(x))

    # -------------------------------------------------
    # CATEGORY TOTALS
    # -------------------------------------------------

    print("Computing category totals...")

    cat_totals = (
        parsed_data.map(
            lambda x: (x["category"], 1)
        )
        .reduceByKey(
            lambda a, b: a + b
        )
        .collectAsMap()
    )

    N = sum(cat_totals.values())

    print("TOTAL REVIEWS:", N)



    print("Persisting term-category counts...")

    term_cat_count.persist(StorageLevel.DISK_ONLY)

    # -------------------------------------------------
    # TERM TOTALS
    # -------------------------------------------------

    print("Computing term totals...")

    term_totals = (
        term_cat_count.map(
            lambda x: (x[0][0], x[1])
        )
        .reduceByKey(
            lambda a, b: a + b
        )
        .collectAsMap()
    )

    print("TOTAL DISTINCT TERMS:", len(term_totals))

    # -------------------------------------------------
    # CHI-SQUARE
    # -------------------------------------------------

    print("Computing chi-square values...")

    def compute_chi(record):

        (word, category), A = record

        total_t = term_totals[word]

        total_c = cat_totals[category]

        B = total_c - A

        C = total_t - A

        D = N - A - B - C

        denom = (
            (A + B)
            * (C + D)
            * (A + C)
            * (B + D)
        )

        if denom == 0:
            chi2 = 0.0
        else:
            chi2 = (
                N * ((A * D - B * C) ** 2)
            ) / denom

        return (category, (word, chi2))

    chi_entries = term_cat_count.map(compute_chi)



    print("Computing top 75 terms per category...")

    def seq_op(heap, value):

        score = value[1]

        if len(heap) < TOP_K:

            heapq.heappush(heap, (score, value))

        else:

            if score > heap[0][0]:
                heapq.heapreplace(heap, (score, value))

        return heap

    def comb_op(heap1, heap2):

        for score, value in heap2:

            if len(heap1) < TOP_K:

                heapq.heappush(heap1, (score, value))

            else:

                if score > heap1[0][0]:
                    heapq.heapreplace(heap1, (score, value))

        return heap1

    top75 = (
        chi_entries.aggregateByKey(
            [],
            seq_op,
            comb_op
        )
        .mapValues(
            lambda heap: sorted(
                [x[1] for x in heap],
                key=lambda x: -x[1]
            )
        )
        .collect()
    )

    # -------------------------------------------------
    # MERGED DICTIONARY
    # -------------------------------------------------

    print("Building merged dictionary...")

    merged_dict = sorted(term_totals.keys())

    # -------------------------------------------------
    # WRITE OUTPUT
    # -------------------------------------------------

    print("Formatting output...")

    result_lines = []

    for cat, items in sorted(top75, key=lambda x: x[0]):

        line = cat

        for term, score in items:

            line += f" {term}:{score:.4f}"

        result_lines.append(line)

    # dictionary line
    result_lines.append(" ".join(merged_dict))

    print("Saving output to HDFS...")

    sc.parallelize(result_lines).saveAsTextFile(
        output_path
    )

    runtime = time.time() - t0

    print("====================================")
    print("RUNTIME (sec):", runtime)
    print("====================================")

    sc.stop()


# -----------------------------------------------------
# ENTRYPOINT
# -----------------------------------------------------

if __name__ == "__main__":

    import sys

    if len(sys.argv) < 3:

        print("Usage:")
        print(
            "spark-submit script.py "
            "<input_hdfs_path> <output_hdfs_path>"
        )

        sys.exit(1)

    # Spark/YARN-safe argument handling
    input_path = sys.argv[-2]

    output_path = sys.argv[-1]

    print("INPUT ARG:", input_path)
    print("OUTPUT ARG:", output_path)

    main(
        input_path,
        output_path,
        STOPWORDS_FILE
    )