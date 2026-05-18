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
    OUTPUT="${OUTPUT:-output_svm.txt}"
    RESULT_FILE="$OUTPUT"
    CMD=(spark-submit task3.py "$INPUT" "$OUTPUT" "$STOPWORDS")
elif [ "$MODE" = "cluster" ]; then
    OUTPUT="${OUTPUT:-hdfs:///user/$USER/output_svm}"
    RESULT_FILE="$(basename "$OUTPUT")"
    if [[ "$RESULT_FILE" != *.txt ]]; then
        RESULT_FILE="${RESULT_FILE}.txt"
    fi
    CMD=(spark-submit --master yarn --deploy-mode cluster --files "$STOPWORDS" task3.py "$INPUT" "$OUTPUT" "$(basename "$STOPWORDS")")
else
    echo "First argument must be 'local' or 'cluster'."
    exit 1
fi

echo "Running Part 3 with dataset=$DATASET output=$OUTPUT"
if [ "$MODE" = "cluster" ]; then
    if hdfs dfs -test -e "$OUTPUT"; then
        echo "Removing old HDFS output: $OUTPUT"
        hdfs dfs -rm -r -skipTrash "$OUTPUT"
    fi
fi

SECONDS=0
"${CMD[@]}"
ELAPSED="$SECONDS"

if [ "$MODE" = "cluster" ]; then
    hdfs dfs -get -f "$OUTPUT/part-00000" "$RESULT_FILE"
fi

{
    echo "---"
    echo "Part 3"
    echo "mode=$MODE"
    echo "dataset=$DATASET"
    echo "execution_time_seconds=$ELAPSED"
    echo "result_file=$RESULT_FILE"
    echo ""
    echo "SVM and grid search results:"
    if [ -f "$RESULT_FILE" ]; then
        cat "$RESULT_FILE"
    else
        echo "Result file was not found locally."
    fi
} >> part3.log

echo "Part 3 finished in $ELAPSED seconds."
echo "Result file: $RESULT_FILE"
echo "Log file: part3.log"
