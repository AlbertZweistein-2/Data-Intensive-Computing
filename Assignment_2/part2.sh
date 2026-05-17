#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ]; then
    echo "Usage: $0 <local|cluster> [dev|full] [output_path] [stopwords_path]"
    exit 1
fi

MODE="$1"
shift

DATASET="dev"
if [ "$#" -gt 0 ] && { [ "$1" = "dev" ] || [ "$1" = "full" ]; }; then
    DATASET="$1"
    shift
fi

OUTPUT="${1:-}"
if [ "$#" -gt 0 ]; then
    shift
fi

STOPWORDS="${1:-stopwords.txt}"

if [ "$DATASET" = "full" ]; then
    INPUT="hdfs:///dic_shared/amazon-reviews/full/reviewscombined.json"
else
    INPUT="hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json"
fi

if [ "$MODE" = "local" ]; then
    OUTPUT="${OUTPUT:-output_ds.txt}"
    CMD=(spark-submit task2.py "$INPUT" "$OUTPUT" "$STOPWORDS")
elif [ "$MODE" = "cluster" ]; then
    OUTPUT="${OUTPUT:-hdfs:///user/$USER/output_ds}"
    CMD=(spark-submit --master yarn --deploy-mode cluster --files "$STOPWORDS" task2.py "$INPUT" "$OUTPUT" "$(basename "$STOPWORDS")")
else
    echo "First argument must be 'local' or 'cluster'."
    exit 1
fi

echo "Running Part 2 with dataset=$DATASET output=$OUTPUT"
SECONDS=0
"${CMD[@]}"
echo "Part 2 finished in $SECONDS seconds."
