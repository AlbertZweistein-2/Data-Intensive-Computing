#!/usr/bin/env bash

###############################################################################
# Assignment 2 - Part 1 (Optimized Spark RDD)
#
# HOW TO RUN:
#
# DEV mode:
#     ./run_part1.sh dev
#
# FULL mode (default):
#     ./run_part1.sh
#
# Output merged locally as:
#     outputs/output_part1_rdd.txt
###############################################################################

set -euo pipefail

###############################################################################
# TIMING
###############################################################################

START_SECONDS=$SECONDS

DATE=$(date "+%Y-%m-%d %H:%M:%S")

###############################################################################
# MODE
###############################################################################

MODE="${1:-full}"

if [[ "$MODE" == "dev" ]]; then
    INPUT="hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json"
    OUTPUT_HDFS="hdfs:///user/$USER/a2_part1_rdd_dev"
    echo "[MODE] Development mode"
else
    INPUT="hdfs:///dic_shared/amazon-reviews/full/reviewscombined.json"
    OUTPUT_HDFS="hdfs:///user/$USER/a2_part1_rdd_full"
    echo "[MODE] Full mode"
fi

###############################################################################
# PATHS
###############################################################################

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_SCRIPT="$SCRIPT_DIR/run_part1.py"
STOPWORDS="$SCRIPT_DIR/stopwords.txt"

LOGDIR="$SCRIPT_DIR/logs"
OUTDIR="$SCRIPT_DIR/outputs"
mkdir -p "$LOGDIR" "$OUTDIR"

LOGFILE="$LOGDIR/part1_rdd_$(date +%Y%m%d_%H%M%S).log"
LOCAL_OUT="$OUTDIR/output_part1_rdd.txt"

echo "===========================================" | tee -a "$LOGFILE"
echo " Assignment 2 - Part 1 RDD RUN" | tee -a "$LOGFILE"
echo " Start: $DATE" | tee -a "$LOGFILE"
echo " Mode: $MODE" | tee -a "$LOGFILE"
echo " Input: $INPUT" | tee -a "$LOGFILE"
echo " Output(HDFS): $OUTPUT_HDFS" | tee -a "$LOGFILE"
echo " Script: $PYTHON_SCRIPT" | tee -a "$LOGFILE"
echo "===========================================" | tee -a "$LOGFILE"

###############################################################################
# CLEAN OLD HDFS OUTPUT
###############################################################################

echo "[INFO] Removing old output directory on HDFS ..." | tee -a "$LOGFILE"
hdfs dfs -rm -r -f "$OUTPUT_HDFS" || true

###############################################################################
# SUBMIT SPARK JOB
###############################################################################

echo "[INFO] Submitting job to YARN ..." | tee -a "$LOGFILE"

spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --files "$STOPWORDS" \
    "$PYTHON_SCRIPT" \
    "$INPUT" "$OUTPUT_HDFS"

###############################################################################
# MERGE HDFS OUTPUT → LOCAL SINGLE FILE
###############################################################################

echo "[INFO] Retrieving output locally ..." | tee -a "$LOGFILE"

rm -f "$LOCAL_OUT"
hdfs dfs -getmerge "$OUTPUT_HDFS" "$LOCAL_OUT"

echo "[INFO] Output saved to: $LOCAL_OUT" | tee -a "$LOGFILE"

###############################################################################
# TIME FINISH
###############################################################################

ELAPSED=$((SECONDS - START_SECONDS))
MINS=$((ELAPSED / 60))
SECS=$((ELAPSED % 60))

echo "[DONE] Total Runtime: ${MINS} minutes ${SECS} seconds" | tee -a "$LOGFILE"
echo "===========================================" | tee -a "$LOGFILE"
