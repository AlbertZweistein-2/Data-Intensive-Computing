from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import re
from collections import defaultdict


#This scans each review, counts how often words appear overall, within each category and how often the combination of category and word appears. 
#The output is a set of <key, value> pairs where the key is a tuple that starts with a tag (e.g., '_n_', '_cat_', '_w_', '_A_') followed by relevant identifiers (e.g., category name, word) and the value is a count. 
#The mapper uses local aggregation to reduce the number of emitted pairs, and the reducer sums these counts across all mappers to produce the final totals needed for chi-squared calculations in later steps.
class PearsonCounts(MRJob):
    STOPWORDS_PATH = 'stopwords.txt'

    # configure_args() defines command-line arguments for the MRJob runner.
    # Here it adds a stopwords file path that is available to the mapper.
    def configure_args(self):
        super().configure_args()
        self.add_file_arg('--stopwords', default=self.STOPWORDS_PATH)

    # mapper_init() runs once per mapper process.
    # It loads shared data and prepares local state before any input records are read.
    #It also precomputes a regex for splitting review text into words.
    def mapper_init(self):
        with open(self.options.stopwords, 'r') as f:
            self.stopwords = set(line.strip() for line in f if line.strip())
        # Precompute the regex used to split review text into words.
        self.word_split_re = re.compile(r'[^a-zA-Z<>^|]+')
        
        
        # Keep local counters so the mapper can combine repeated keys before emitting.
        self.local_counts = defaultdict(int)
    
    # mapper(_, entry) processes one input record.
    # The first argument is the input key from MRJob and is unused here.
    # The second argument is the JSON review string.
    def mapper(self, _, entry):
        data = json.loads(entry)
        category = data['category']
        review_text = data['reviewText']

        # Split the review text, normalize words to lowercase, and keep only unique terms.
        words = map(str.lower, self.word_split_re.split(review_text))

        unique_words = {
            w for w in words
            if len(w) > 1 and w not in self.stopwords
        }

        # Emit counts for the whole dataset: <('_n_',), 1> means one more review.
        self.local_counts[('_n_',)] += 1

        # Emit counts per category: <('_cat_', category), 1> means one more review in that category.
        self.local_counts[('_cat_', category)] += 1

        # Emit word statistics using <key,value> pairs:
        # <('_w_', word), 1> counts how many reviews contain the word overall.
        # <('_A_', category, word), 1> counts how many reviews in the category contain the word.
        for word in unique_words:
            self.local_counts[('_w_', word)] += 1
            self.local_counts[('_A_', category, word)] += 1

        # Flush local counts early if the dictionary grows too large.
        if len(self.local_counts) > 100000:
            for key, count in self.local_counts.items():
                yield key, count
            self.local_counts.clear()

    # mapper_final() runs once after all records were processed by this mapper.
    # It emits any remaining buffered <key,value> pairs.
    def mapper_final(self):
        for key, count in self.local_counts.items():
            yield key, count

    # reducer_sum(key, values) is used as both combiner and reducer.
    # It receives one key and an iterable of numeric values, then outputs the summed count.
    def reducer_sum(self, key, values):
        yield key, sum(values)

    # steps() defines the MapReduce pipeline.
    # MRStep wires the mapper, combiner, and reducer for this single job stage.
    def steps(self):
        return [
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper,
                mapper_final=self.mapper_final,
                combiner=self.reducer_sum,
                reducer=self.reducer_sum
            )
        ]

if __name__ == '__main__':
    PearsonCounts.run()