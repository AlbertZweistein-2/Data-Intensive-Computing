#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
    echo "Usage: $0 <reviews.json> [stopwords.txt]"
    exit 1
fi

INPUT_FILE="$1"
STOPWORDS_FILE="${2:-../Assets/stopwords.txt}"

JOB1_OUT="counts.jsonl"
SIDE_DATA="side_data.json"
JOB2_OUT="chi2.jsonl"
JOB3_OUT="top75_merged_terms.jsonl"
FINAL_OUT="output.txt"

echo "Input file:      $INPUT_FILE"
echo "Stopwords file:  $STOPWORDS_FILE"
echo "Final output:    $FINAL_OUT"

echo
echo "=== Step 1: counting n, cat, w, A ==="
python3 job1_counts.py "$INPUT_FILE" --stopwords "$STOPWORDS_FILE" > "$JOB1_OUT"

echo
echo "=== Step 2: building side data (n + cat counts) ==="
python3 build_side_data.py "$JOB1_OUT" "$SIDE_DATA"

echo
echo "=== Step 3: computing Pearson chi^2 ==="
python3 job2_pearson.py "$JOB1_OUT" --side-data "$SIDE_DATA" > "$JOB2_OUT"

echo
echo "=== Step 4: top 75 per category ==="
python3 job3_top75_per_category.py "$JOB2_OUT" > "$JOB3_OUT"

echo
echo "=== Step 5: formatting final output.txt ==="
python3 format_output.py "$JOB3_OUT" "$FINAL_OUT"

echo
echo "Final merged vocabulary written to: $FINAL_OUT"

# Cleanup intermediate files (optional)
rm "$JOB1_OUT" "$SIDE_DATA" "$JOB2_OUT" "$JOB3_OUT"