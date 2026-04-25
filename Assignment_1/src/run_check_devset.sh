#!/usr/bin/env bash
set -euo pipefail

./run_hadoop.sh hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json
python3 check_output_devset.py
