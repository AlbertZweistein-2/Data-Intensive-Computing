import sys

from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    ChiSqSelector,
    CountVectorizer,
    IDF,
    RegexTokenizer,
    StopWordsRemover,
    StringIndexer,
)
from pyspark.sql import SparkSession


# Example usage locally:
# spark-submit task2.py hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json output_ds.txt stopwords.txt

# Example usage on a cluster:
# spark-submit --master yarn --deploy-mode cluster \
#   --files stopwords.txt \
#   task2.py \
#   hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json \
#   hdfs:///user/e01520348/output_ds \
#   stopwords.txt


DEFAULT_INPUT_PATH = "hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json"
DEFAULT_OUTPUT_PATH = "output_ds.txt"
DEFAULT_STOPWORDS_PATH = "stopwords.txt"
NUM_TOP_FEATURES = 2000


def parse_args(args):
    # Read arguments or fall back to defaults
    input_path = args[1] if len(args) > 1 else DEFAULT_INPUT_PATH
    output_path = args[2] if len(args) > 2 else DEFAULT_OUTPUT_PATH
    stopwords_path = args[3] if len(args) > 3 else DEFAULT_STOPWORDS_PATH
    return input_path, output_path, stopwords_path


def load_stopwords(path):
    # Load stopwords and normalize to lowercase
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip().lower() for line in f if line.strip()]


def write_terms(sc, terms, output_path):
    # Serialize terms as a single space-separated line
    output_line = " ".join(terms)

    if output_path.startswith("hdfs://"):
        # https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.SparkContext.parallelize.html
        sc.parallelize([output_line], 1).saveAsTextFile(output_path)
    else:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(output_line + "\n")


def main():
    # Parse input/output configuration
    input_path, output_path, stopwords_path = parse_args(sys.argv)

    # Create Spark session and context
    spark = SparkSession.builder.appName("Assignment2_Task2_DS").getOrCreate()
    spark_ctx = spark.sparkContext

    try:
        # Load the stopword list
        stopwords = load_stopwords(stopwords_path)
        # Broadcast not necessary, because we pass the stopwords directly
        # to the StopWordsRemover in the pipeline, which handles distribution internally

        # Read reviews
        # https://spark.apache.org/docs/latest/sql-data-sources-json.html
        reviews_df = (
            spark.read.json(input_path)
            .select("category", "reviewText")
            .na.fill({"reviewText": ""})
        )

        # Tokenize the review text
        # https://spark.apache.org/docs/latest/ml-features.html#tokenizer
        text_tokenizer = RegexTokenizer(
            inputCol="reviewText",
            outputCol="tokens",
            pattern=r"[\s\d()\[\]{}.!?,;:+=\-_\"'`~#@&*%\u20ac$\u00a7\\/]+",
            gaps=True,
            toLowercase=True,
            minTokenLength=2,
        )

        # Remove stopwords from tokens
        # https://spark.apache.org/docs/latest/ml-features.html#stopwordsremover
        stopword_filter = StopWordsRemover(
            inputCol="tokens",
            outputCol="filtered_tokens",
            stopWords=stopwords,
            caseSensitive=False,
        )
        # Convert category labels to numeric indices for the ChiSqSelector
        # https://spark.apache.org/docs/latest/ml-features.html#stringindexer
        label_indexer = StringIndexer(
            inputCol="category",
            outputCol="label",
            handleInvalid="skip",
        )

        # CountVectorizer converts token lists into sparse term frequency vectors
        # https://spark.apache.org/docs/latest/ml-features.html#countvectorizer
        tf_vectorizer = CountVectorizer(
            inputCol="filtered_tokens",
            outputCol="term_frequencies",
        )

        # IDF computes the inverse document frequency and scales term frequencies to TF-IDF features
        # https://spark.apache.org/docs/latest/ml-features.html#tf-idf
        idf_transformer = IDF(
            inputCol="term_frequencies",
            outputCol="tfidf_features",
        )

        # ChiSqSelector selects the top features based on the chi-square test
        # https://spark.apache.org/docs/latest/ml-features.html#chisqselector
        chi_square_selector = ChiSqSelector(
            numTopFeatures=NUM_TOP_FEATURES,
            featuresCol="tfidf_features",
            outputCol="selected_features",
            labelCol="label",
        )
        # Pipeline has all transformations. Fit() executes them in order,
        # allowing us to extract the final selected features
        # https://spark.apache.org/docs/latest/ml-pipeline.html
        ml_pipeline = Pipeline(
            stages=[
                text_tokenizer,
                stopword_filter,
                label_indexer,
                tf_vectorizer,
                idf_transformer,
                chi_square_selector,
            ]
        )

        # Fit the pipeline to compute selected features
        model = ml_pipeline.fit(reviews_df)

        # Grab fitted models for vocabulary and selected indices
        tf_vectorizer_model = model.stages[3]
        chi_square_model = model.stages[5]

        # Map selected indices back to token strings
        vocabulary = tf_vectorizer_model.vocabulary
        selected_indices = chi_square_model.selectedFeatures
        selected_terms = sorted(vocabulary[index] for index in selected_indices)

        write_terms(spark_ctx, selected_terms, output_path)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
