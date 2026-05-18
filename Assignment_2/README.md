# Assignment 2 Usage

This folder contains the implementations for all three assignment parts:

- `task1.py`: Part 1, RDD-based chi-square term selection.
- `task2.py`: Part 2, Spark ML pipeline with TF-IDF and chi-square feature selection.
- `task3.py`: Part 3, Spark ML text classification with One-vs-Rest Linear SVM.
- `part1.sh`, `part2.sh`, `part3.sh`: helper scripts for running the tasks locally or on the YARN cluster.

The default dataset is always the development set:

```bash
hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json
```

The full dataset can be selected with `full`:

```bash
hdfs:///dic_shared/amazon-reviews/full/reviewscombined.json
```

## Table of Contents

- [Required Files](#required-files)
- [Direct Python Usage](#direct-python-usage)
  - [Part 1](#part-1)
  - [Part 2](#part-2)
  - [Part 3](#part-3)
- [Helper Script Usage](#helper-script-usage)
- [Local Pod Runs](#local-pod-runs)
  - [Development Set](#development-set)
  - [Full Dataset](#full-dataset)
- [YARN Cluster Runs](#yarn-cluster-runs)
  - [Development Set](#development-set-1)
  - [Full Dataset](#full-dataset-1)
- [Logs and Runtime](#logs-and-runtime)
- [Part 3 Cluster Configuration](#part-3-cluster-configuration)
- [Checking HDFS Output Manually](#checking-hdfs-output-manually)
- [Recommended Testing Order](#recommended-testing-order)
- [Output and Analysis Files](#output-and-analysis-files)

## Required Files

Run all commands from the `Assignment_2` directory:

```bash
cd Assignment_2
```

Make sure the stopword file is available, usually:

```bash
stopwords.txt
```

When running in YARN cluster mode, the shell scripts automatically pass it with `--files`.

## Direct Python Usage

Each Python task accepts:

```bash
spark-submit taskX.py [input_path] [output_path] [stopwords_path]
```

If arguments are omitted, the task uses the dev dataset, its default output file, and `stopwords.txt`.

### Part 1

```bash
spark-submit task1.py \
  hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json \
  output_rdd.txt \
  stopwords.txt
```

Output:

```bash
output_rdd.txt
```

### Part 2

```bash
spark-submit task2.py \
  hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json \
  output_ds.txt \
  stopwords.txt
```

Output:

```bash
output_ds.txt
```

### Part 3

```bash
spark-submit task3.py \
  hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json \
  output_svm.txt \
  stopwords.txt
```

Output:

```bash
output_svm.txt
```

This file contains the validation grid results, best parameters, and test F1 score.

## Helper Script Usage

The helper scripts have the same interface:

```bash
bash partX.sh <local|cluster> [dev|full] [output_path] [stopwords_path]
```

Arguments:

- `<local|cluster>` is required.
- `[dev|full]` is optional. If omitted, `dev` is used.
- `[output_path]` is optional.
- `[stopwords_path]` is optional. If omitted, `stopwords.txt` is used.

Before using executable form, set permissions once:

```bash
chmod +x part1.sh part2.sh part3.sh
```

Then scripts can be run as `./part1.sh local`, or with `bash part1.sh local`.

## Local Pod Runs

Local mode runs `spark-submit` on the current cluster pod and writes output to the local filesystem.

### Development Set

```bash
bash part1.sh local
bash part2.sh local
bash part3.sh local
```

Equivalent explicit commands:

```bash
bash part1.sh local dev output_rdd.txt stopwords.txt
bash part2.sh local dev output_ds.txt stopwords.txt
bash part3.sh local dev output_svm.txt stopwords.txt
```

### Full Dataset

Use full only after the dev runs work correctly:

```bash
bash part1.sh local full output_rdd_full.txt stopwords.txt
bash part2.sh local full output_ds_full.txt stopwords.txt
bash part3.sh local full output_svm_full.txt stopwords.txt
```

## YARN Cluster Runs

Cluster mode runs:

```bash
spark-submit --master yarn --deploy-mode cluster
```

The scripts write Spark output to HDFS, remove old related HDFS output directories before starting, and then copy `part-00000` back to the current local directory.

### Development Set

```bash
bash part1.sh cluster
bash part2.sh cluster
bash part3.sh cluster
```

Default HDFS output directories:

```bash
hdfs:///user/$USER/output_rdd
hdfs:///user/$USER/output_ds
hdfs:///user/$USER/output_svm
```

Default local copied result files:

```bash
output_rdd.txt
output_ds.txt
output_svm.txt
```

### Full Dataset

```bash
bash part1.sh cluster full hdfs:///user/$USER/output_rdd_full stopwords.txt
bash part2.sh cluster full hdfs:///user/$USER/output_ds_full stopwords.txt
bash part3.sh cluster full hdfs:///user/$USER/output_svm_full stopwords.txt
```

These copy results back locally as:

```bash
output_rdd_full.txt
output_ds_full.txt
output_svm_full.txt
```

## Logs and Runtime

Each helper script appends runtime information to a log file:

```bash
part1.log
part2.log
part3.log
```

Each run is appended and separated by `---`.

The logs include:

```text
mode
dataset
execution_time_seconds
result_file
```

For Part 3, `part3.log` also includes the SVM/grid-search result content from `output_svm.txt`.

## Part 3 Cluster Configuration

Part 3 is much more expensive than Part 1 and Part 2 because it performs grid search with `OneVsRest(LinearSVC)`. Since `LinearSVC` is binary, `OneVsRest` trains one binary SVM per category for each grid configuration. Therefore, a 24-configuration grid can result in many individual SVM trainings.

To make the YARN cluster run more parallel, `part3.sh` requests:

```bash
--num-executors 6
--executor-cores 2
--executor-memory 4G
--driver-memory 4G
--conf spark.sql.shuffle.partitions=64
--conf spark.default.parallelism=64
```

This gives 12 executor cores in total. In `task3.py`, the input DataFrame is also repartitioned:

```python
.repartition(64)
```

This helps Spark distribute work across the requested executors. These settings only affect execution and parallelism; they do not change the feature extraction, model, or evaluation logic.

To verify that the executor settings were applied, inspect the YARN logs:

```bash
yarn logs -applicationId <application_id> | grep -- "--executor-id"
```

Expected signs are several executor IDs and `--cores 2`, for example:

```text
--executor-id 1 ... --cores 2
--executor-id 2 ... --cores 2
...
--executor-id 6 ... --cores 2
```


## Checking HDFS Output Manually

List an HDFS output directory:

```bash
hdfs dfs -ls hdfs:///user/$USER/output_rdd
```

Print the result:

```bash
hdfs dfs -cat hdfs:///user/$USER/output_rdd/part-00000
```

Copy it manually:

```bash
hdfs dfs -get -f hdfs:///user/$USER/output_rdd/part-00000 output_rdd.txt
```

Remove an old HDFS output directory manually:

```bash
hdfs dfs -rm -r -skipTrash hdfs:///user/$USER/output_rdd
```

## Recommended Testing Order

1. Run each part on the development set locally:

```bash
bash part1.sh local
bash part2.sh local
bash part3.sh local
```

2. Run each part on the development set in cluster mode:

```bash
bash part1.sh cluster
bash part2.sh cluster
bash part3.sh cluster
```

3. Only after dev runs are correct, test full-dataset runs if needed:

```bash
bash part1.sh cluster full hdfs:///user/$USER/output_rdd_full stopwords.txt
bash part2.sh cluster full hdfs:///user/$USER/output_ds_full stopwords.txt
bash part3.sh cluster full hdfs:///user/$USER/output_svm_full stopwords.txt
```

For the assignment submission, the requested result files are produced from the development set.

## Output and Analysis Files

The main output files are:

| File | Created by | Description |
| --- | --- | --- |
| `output.txt` | Assignment 1 | Assignment 1 result with top chi-square terms per category and the merged dictionary line for comparison with Part 1 from Assignment 2. |
| `output_rdd.txt` | `task1.py` / `part1.sh` | Part 1 result with top chi-square terms per category and the merged dictionary line. |
| `output_ds.txt` | `task2.py` / `part2.sh` | Part 2 result with the 2000 terms selected by Spark ML chi-square selection. |
| `output_svm.txt` | `task3.py` / `part3.sh` | Part 3 result with validation grid results, best SVM parameters, and final test F1. |
| `part1.log` | `part1.sh` | Runtime log for Part 1 runs. |
| `part2.log` | `part2.sh` | Runtime log for Part 2 runs. |
| `part3.log` | `part3.sh` | Runtime log for Part 3 runs, including SVM/grid-search results. |

The source files are:

| File | Purpose |
| --- | --- |
| `task1.py` | RDD implementation for Part 1. |
| `task2.py` | DataFrame/Spark ML feature extraction and selection for Part 2. |
| `task3.py` | Spark ML classification experiment for Part 3. |
| `part1.sh` | Run helper for Part 1. |
| `part2.sh` | Run helper for Part 2. |
| `part3.sh` | Run helper for Part 3. |
| `results_comparisons.ipynb` | Notebook to evaluate and compare results. |

The notebook `results_comparisons.ipynb` is used for analyzing and comparing the generated output files. First, it compares the Assignment 1 output with the Part 1 RDD output by parsing the category lines, extracted terms, and chi-square values. It checks whether the category sets match, whether the selected top terms per category are the same, and whether overlapping terms have equal chi-square scores.

The notebook also compares the final merged dictionaries. For Part 1, this means the merged dictionary built from the top 75 terms per category. For Part 2, this means the 2000 terms selected by Spark ML's TF-IDF and chi-square feature selection pipeline. The notebook computes the term counts, the overlap between both dictionaries, and the terms that occur only in one of the two outputs. These computations are used as supporting material for the report, where the actual observations and interpretation are discussed.


- SVM grid-search results across different parameter settings
