from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import re


class PearsonCounts(MRJob):
    STOPWORDS_PATH = '../Assets/stopwords.txt'

    def configure_args(self):
        super().configure_args()
        self.add_file_arg('--stopwords', default=self.STOPWORDS_PATH)

    def mapper_init(self):
        with open(self.options.stopwords, 'r') as f:
            self.stopwords = set(line.strip() for line in f if line.strip())

    def mapper(self, _, entry):
        data = json.loads(entry)
        category = data['category']
        review_text = data['reviewText']

        words = map(str.lower, re.split(r'[^a-zA-Z<>^|]+', review_text))
        unique_words = {
            w for w in words
            if len(w) > 1 and w not in self.stopwords
        }

        yield ('_n_',), 1
        yield ('_cat_', category), 1

        for word in unique_words:
            yield ('_w_', word), 1
            yield ('_A_', category, word), 1

    def reducer_sum(self, key, values):
        yield key, sum(values)

    def steps(self):
        return [
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper,
                combiner=self.reducer_sum,
                reducer=self.reducer_sum
            )
        ]


if __name__ == '__main__':
    PearsonCounts.run()