#!/usr/bin/env bash
set -euo pipefail

START_SECONDS=$SECONDS

if [ "$#" -gt 2 ]; then
    echo "Usage: $0 [input_hdfs_path] [stopwords.txt]"
    echo "Example: $0 hdfs:///dic_shared/amazon-reviews/full/reviewscombined.json ../Assets/stopwords.txt"
    exit 1
fi

INPUT_HDFS="${1:-hdfs:///dic_shared/amazon-reviews/full/reviewscombined.json}"
STOPWORDS_FILE="${2:-../Assets/stopwords.txt}"

HADOOP_STREAMING_JAR="/usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.6.jar"

# HDFS output directories
HDFS_BASE="hdfs:///user/$(whoami)/Assignment_1"
JOB1_HDFS_OUT="${HDFS_BASE}/job1_counts"
JOB2_HDFS_OUT="${HDFS_BASE}/job2_chi2"
JOB3_HDFS_OUT="${HDFS_BASE}/job3_top75"

# Local intermediate/final files
LOCAL_JOB1_OUT="counts.jsonl"
LOCAL_SIDE_DATA="side_data.json"
LOCAL_JOB3_OUT="top75_per_category.jsonl"
FINAL_OUT="output.txt"
LOG_FILE="log.txt"

echo "Input HDFS file:   $INPUT_HDFS"
echo "Stopwords file:    $STOPWORDS_FILE"
echo "Streaming JAR:     $HADOOP_STREAMING_JAR"
echo "HDFS base dir:     $HDFS_BASE"
echo "Final output:      $FINAL_OUT"

echo "=== START OF RUN ${START_SECONDS} with file ${INPUT_HDFS} ===" >> "$LOG_FILE"

echo
echo "=== Step 0: remove old HDFS outputs ==="
hadoop fs -rm -r -f "$JOB1_HDFS_OUT" || true
hadoop fs -rm -r -f "$JOB2_HDFS_OUT" || true
hadoop fs -rm -r -f "$JOB3_HDFS_OUT" || true

echo "Step 0 finished after: $(( SECONDS - START_SECONDS )) seconds" >> "$LOG_FILE"

echo
echo "=== Step 1: counting n, cat, w, A on Hadoop ==="
python3 job1_counts.py \
    --hadoop-streaming-jar "$HADOOP_STREAMING_JAR" \
    -r hadoop \
    --jobconf mapreduce.job.reduces=24 \
    "$INPUT_HDFS" \
    --stopwords "$STOPWORDS_FILE" \
    --output-dir "$JOB1_HDFS_OUT"

echo "Step 1 finished after: $(( SECONDS - START_SECONDS )) seconds" >> "$LOG_FILE"

echo
echo "=== Step 2: fetch Job1 output locally ==="
rm -f "$LOCAL_JOB1_OUT"
hadoop fs -getmerge "$JOB1_HDFS_OUT" "$LOCAL_JOB1_OUT"

echo "Step 2 finished after: $(( SECONDS - START_SECONDS )) seconds" >> "$LOG_FILE"

echo
echo "=== Step 3: build side data locally (n + cat counts) ==="
python3 build_side_data.py "$LOCAL_JOB1_OUT" "$LOCAL_SIDE_DATA"

echo "Step 3 finished after: $(( SECONDS - START_SECONDS )) seconds" >> "$LOG_FILE"

echo
echo "=== Step 4: compute Pearson chi^2 on Hadoop ==="
python3 job2_pearson.py \
    --hadoop-streaming-jar "$HADOOP_STREAMING_JAR" \
    -r hadoop \
    "$JOB1_HDFS_OUT" \
    --side-data "$LOCAL_SIDE_DATA" \
    --output-dir "$JOB2_HDFS_OUT"

echo "Step 4 finished after: $(( SECONDS - START_SECONDS )) seconds" >> "$LOG_FILE"

echo
echo "=== Step 5: top 75 per category on Hadoop ==="
python3 job3_top75_per_category.py \
    --hadoop-streaming-jar "$HADOOP_STREAMING_JAR" \
    -r hadoop \
    "$JOB2_HDFS_OUT" \
    --output-dir "$JOB3_HDFS_OUT"

echo "Step 5 finished after: $(( SECONDS - START_SECONDS )) seconds" >> "$LOG_FILE"

echo
echo "=== Step 6: fetch Job3 output locally ==="
rm -f "$LOCAL_JOB3_OUT"
hadoop fs -getmerge "$JOB3_HDFS_OUT" "$LOCAL_JOB3_OUT"

echo "Step 6 finished after: $(( SECONDS - START_SECONDS )) seconds" >> "$LOG_FILE"

echo
echo "=== Step 7: format final output.txt locally ==="
python3 format_output.py "$LOCAL_JOB3_OUT" "$FINAL_OUT"

echo "Step 7 finished after: $(( SECONDS - START_SECONDS )) seconds" >> "$LOG_FILE"

DURATION=$(( SECONDS - START_SECONDS ))
MINS=$(( DURATION / 60 ))
SECS=$(( DURATION % 60 ))

echo "Total runtime: $(( SECONDS - START_SECONDS )) seconds" >> "$LOG_FILE"
echo "=== END OF RUN ${END_SECONDS} ===" >> "$LOG_FILE"

echo
echo "Done."

echo
echo "======================================"
echo "GESAMTLAUFZEIT: ${MINS} Minuten und ${SECS} Sekunden"
echo "======================================"

echo "Final output written to: $FINAL_OUT"

# Optional local cleanup
rm -f "$LOCAL_JOB1_OUT" "$LOCAL_SIDE_DATA" "$LOCAL_JOB3_OUT"