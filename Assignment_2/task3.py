import sys

from pyspark.ml import Pipeline
from pyspark.ml.classification import LinearSVC, OneVsRest
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import (
    ChiSqSelector,
    CountVectorizer,
    IDF,
    Normalizer,
    RegexTokenizer,
    StopWordsRemover,
    StringIndexer,
)
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.sql import SparkSession


# Example usage locally:
# spark-submit task3.py hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json output_svm.txt stopwords.txt

# Example usage on a cluster:
# spark-submit --master yarn --deploy-mode cluster \
#   --files stopwords.txt \
#   task3.py \
#   hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json \
#   hdfs:///user/e01520348/output_svm \
#   stopwords.txt


DEFAULT_INPUT_PATH = "hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json"
DEFAULT_OUTPUT_PATH = "output_svm.txt"
DEFAULT_STOPWORDS_PATH = "stopwords.txt"
RANDOM_SEED = 42


def parse_args(args):
    # Read arguments
    input_path = args[1] if len(args) > 1 else DEFAULT_INPUT_PATH
    output_path = args[2] if len(args) > 2 else DEFAULT_OUTPUT_PATH
    stopwords_path = args[3] if len(args) > 3 else DEFAULT_STOPWORDS_PATH
    return input_path, output_path, stopwords_path


def load_stopwords(path):
    # Load stopwords, to lowercase
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip().lower() for line in f if line.strip()]


def write_lines(sc, lines, output_path):
    # Write results to HDFS or a local file depending on the path
    if output_path.startswith("hdfs://"):
        sc.parallelize(lines, 1).saveAsTextFile(output_path)
    else:
        with open(output_path, "w", encoding="utf-8") as f:
            for line in lines:
                f.write(line + "\n")


def main():
    # Parse configuration
    input_path, output_path, stopwords_path = parse_args(sys.argv)

    # Create Spark session and context
    spark = SparkSession.builder.appName("Assignment2_Task3_SVM").getOrCreate()
    spark_ctx = spark.sparkContext

    try:
        # Load the stopword list
        stopwords = load_stopwords(stopwords_path)

        # Read and cache the input reviews
        # Caching is to speed up multiple passes over the data during model selection and evaluation
        reviews_df = (
            spark.read.json(input_path)
            .select("category", "reviewText")
            .na.fill({"reviewText": ""})
            .cache()
        )

        # Split into train/validation and test
        train_valid_df, test_df = reviews_df.randomSplit([0.85, 0.15], seed=RANDOM_SEED)

        # Tokenize the review text
        text_tokenizer = RegexTokenizer(
            inputCol="reviewText",
            outputCol="tokens",
            pattern=r"[\s\d()\[\]{}.!?,;:+=\-_\"'`~#@&*%\u20ac$\u00a7\\/]+",
            gaps=True,
            toLowercase=True,
            minTokenLength=2,
        )

        # Remove stopwords from tokens
        stopword_filter = StopWordsRemover(
            inputCol="tokens",
            outputCol="filtered_tokens",
            stopWords=stopwords,
            caseSensitive=False,
        )

        # Map category labels to numeric indices
        label_indexer = StringIndexer(
            inputCol="category",
            outputCol="label",
            handleInvalid="skip",
        )

        # Build term frequency vectors
        tf_vectorizer = CountVectorizer(
            inputCol="filtered_tokens",
            outputCol="term_frequencies",
        )

        # Apply inverse document frequency weighting
        idf_transformer = IDF(
            inputCol="term_frequencies",
            outputCol="tfidf_features",
        )

        # Select top features by chi-square
        chi_square_selector = ChiSqSelector(
            featuresCol="tfidf_features",
            outputCol="selected_features",
            labelCol="label",
        )

        # Normalize features to unit length
        # https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.mllib.feature.Normalizer.html
        l2_normalizer = Normalizer(
            inputCol="selected_features",
            outputCol="normalized_features",
            p=2.0,
        )

        # Configure linear SVM classifier
        # https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.classification.LinearSVC.html
        svm_clf = LinearSVC(
            featuresCol="normalized_features",
            labelCol="label",
        )

        # Wrap SVM for multiclass classification
        # OneVsRest trains one binary classifier per category and picks the
        # best scoring category for each review.
        # https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.classification.OneVsRest.html
        ovr_clf = OneVsRest(
            classifier=svm_clf,
            featuresCol="normalized_features",
            labelCol="label",
            predictionCol="prediction",
        )

        # Assemble the ML pipeline stages
        ml_pipeline = Pipeline(
            stages=[
                text_tokenizer,
                stopword_filter,
                label_indexer,
                tf_vectorizer,
                idf_transformer,
                chi_square_selector,
                l2_normalizer,
                ovr_clf,
            ]
        )

        # Evaluate using F1 score
        # https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.evaluation.MulticlassClassificationEvaluator.html
        f1_evaluator = MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="f1",
        )

        # Hyperparameter grid for gridsearch
        # param_grid = (
        #     ParamGridBuilder()
        #     .addGrid(chi_square_selector.numTopFeatures, [2000, 300])
        #     .addGrid(svm_clf.regParam, [0.01, 0.1, 1.0])
        #     .addGrid(svm_clf.standardization, [True, False])
        #     .addGrid(svm_clf.maxIter, [10, 30])
        #     .build()
        # )

        # Test Grid
        param_grid = (
            ParamGridBuilder()
            .addGrid(chi_square_selector.numTopFeatures, [2000])
            .addGrid(svm_clf.regParam, [0.01])
            .addGrid(svm_clf.standardization, [True])
            .addGrid(svm_clf.maxIter, [30])
            .build()
        )

        # Train with a validation split for model selection
        train_validation_split = TrainValidationSplit(
            estimator=ml_pipeline,
            estimatorParamMaps=param_grid,
            evaluator=f1_evaluator,
            trainRatio=0.8,
            seed=RANDOM_SEED,
            parallelism=2,
        )

        # Fit models across the grid
        tv_model = train_validation_split.fit(train_valid_df)

        # Find the best validation score
        best_index = max(
            range(len(tv_model.validationMetrics)),
            key=lambda index: tv_model.validationMetrics[index],
        )
        best_params = param_grid[best_index]

        # Evaluate the best model on the test split
        test_predictions = tv_model.bestModel.transform(test_df)
        test_f1 = f1_evaluator.evaluate(test_predictions)

        result_lines = [
            "--- SVM classification results ---",
            f"input_path={input_path}",
            f"train_validation_rows={train_valid_df.count()}",
            f"test_rows={test_df.count()}",
            "",
            "Validation grid results:",
            "numTopFeatures,\tregParam,\tstandardization,\tmaxIter,\tvalidationF1",
        ]

        # Add each grid result to the report
        for params, validation_f1 in zip(param_grid, tv_model.validationMetrics):
            result_lines.append(
                ",\t".join(
                    [
                        str(params[chi_square_selector.numTopFeatures]),
                        str(params[svm_clf.regParam]),
                        str(params[svm_clf.standardization]),
                        str(params[svm_clf.maxIter]),
                        str(validation_f1),
                    ]
                )
            )

        result_lines.extend(
            [
                "",
                "--- Best validation parameters ---",
                f"numTopFeatures={best_params[chi_square_selector.numTopFeatures]}",
                f"regParam={best_params[svm_clf.regParam]}",
                f"standardization={best_params[svm_clf.standardization]}",
                f"maxIter={best_params[svm_clf.maxIter]}",
                f"bestValidationF1={tv_model.validationMetrics[best_index]}",
                "",
                f"testF1={test_f1}",
            ]
        )

        write_lines(spark_ctx, result_lines, output_path)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
