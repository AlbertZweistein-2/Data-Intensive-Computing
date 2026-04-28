# Assignment 1: Distributed Text Analysis with MapReduce

## Overview

This assignment implements a distributed text analysis of Amazon review data using MapReduce. The goal is to identify statistically significant words per product category by performing Chi-squared tests.

## Project Structure

```
Assignment_1/
├── Assets/                          # Input files
│   ├── reviews_devset.json         # Development dataset with reviews
│   └── stopwords.txt               # List of stopwords for filtering
├── src/                             # Python source code
│   ├── job1_counts.py              # MapReduce Job 1: Word and category counting
│   ├── job2_pearson.py             # MapReduce Job 2: Chi-squared calculation
│   ├── job3_top75_per_category.py  # MapReduce Job 3: Top 75 words per category
│   ├── build_side_data.py          # Helper script to prepare intermediate data
│   ├── check_output_devset.py      # Validation script for development dataset
│   ├── format_output.py            # Formats the output
│   ├── run_hadoop.sh               # Main execution script
│   ├── run_check_devset.sh         # Development dataset test script
│   └── Assignment_1_Instructions.pdf  # Detailed task description
└── README.md                        # This file
```

## Jobs in Detail

### Job 1: Frequency Counting (`job1_counts.py`)

**Purpose:** Counts frequencies for all relevant elements:
- Total number of reviews (`n`)
- Reviews per category (`cat`)
- Frequency of each word across all categories (`w`)
- Frequency of a word in a specific category (`A`)

**Workflow:**
1. Mapper reads JSON-formatted reviews
2. Extracts words and filters stopwords
3. Counts various combinations locally in a dictionary
4. Yields counts to reducer
5. Reducer sums all counts

### Job 2: Chi-Squared Calculation (`job2_pearson.py`)

**Purpose:** Calculates Chi-squared statistics for associations between words and categories

**Workflow:**
1. Reads side data (frequencies from Job 1) as side data
2. Mapper organizes counts for each word-category pair
3. Reducer calculates Chi-squared statistics
4. Outputs word-category pairs with Chi-squared values

**Formula:** Uses Pearson's Chi-squared test to measure the statistical significance of word-category associations

### Job 3: Top 75 Words per Category (`job3_top75_per_category.py`)

**Purpose:** Extracts the 75 statistically most significant words per category

**Workflow:**
1. Mapper organizes words by category
2. Reducer keeps the 75 highest Chi-squared values per category
3. Sorts results in descending order by Chi-squared

## Dependencies

- Python 3
- `mrjob` - MapReduce framework for Python
- Hadoop cluster (or local Hadoop installation)
- `hadoop-streaming-3.3.6.jar`

Install mrjob:
```bash
pip install mrjob
```

## Execution

### Complete Pipeline on Hadoop

```bash
cd src
bash run_hadoop.sh [input_hdfs_path] [stopwords_file]
```

**Parameters:**
- `input_hdfs_path` (optional): HDFS path to input dataset (default: `hdfs:///dic_shared/amazon-reviews/full/reviewscombined.json`)
- `stopwords_file` (optional): Path to stopwords file (default: `../Assets/stopwords.txt`)

**Example:**
```bash
bash run_hadoop.sh hdfs:///dic_shared/amazon-reviews/full/reviewscombined.json ../Assets/stopwords.txt
```

### Test with Development Dataset

```bash
cd src
bash run_check_devset.sh
```

This runs the pipeline with the smaller `reviews_devset.json` file for testing and validation purposes. The script executes the full three-job pipeline and validates the results against expected outputs using the `check_output_devset.py` script.

## Input and Output Formats

### Input Format
```json
{
  "category": "Electronics",
  "reviewText": "Great product! Works as expected..."
}
```

### Output Format (Job 3)
```
Category word1:chi2_score word2:chi2_score word3:chi2_score ...
```

**Example:**
```
Apps_for_Android games:3081.15 play:2158.37 graphics:1505.51 kindle:1470.82 addictive:1311.91
Automotive oem:1068.86 honda:1035.22 engine:763.28 vehicle:667.99 headlights:661.42
Baby diaper:2429.74 crib:2411.47 newborn:1292.01 seat:1260.45 pacifiers:1206.43
```

## Script Functions

- **`build_side_data.py`**: Converts Job 1 output to JSON format for use as side data in Job 2
- **`format_output.py`**: Formats the final output from Job 3 into a readable format
- **`check_output_devset.py`**: Validates output against expected results on the development dataset

## Notes

- Stopwords are used to filter common but non-informative words
- The mapper in Job 1 buffers counts locally to reduce overhead
- Chi-squared statistics help identify statistically significant word-category associations
- The pipeline is optimized for large datasets (with local buffering and top-K optimization)

## Output and Logging

- **Main output:** `output.txt` (formatted final results)
- **Logs:** `log.txt` (execution times and status updates)
- Intermediate results are stored on HDFS under `hdfs:///user/$(whoami)/Assignment_1/`
