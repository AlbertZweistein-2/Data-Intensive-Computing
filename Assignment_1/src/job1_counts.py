from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import re
from collections import defaultdict


class PearsonCounts(MRJob):
    STOPWORDS_PATH = '../Assets/stopwords.txt'

    def configure_args(self):
        super().configure_args()
        self.add_file_arg('--stopwords', default=self.STOPWORDS_PATH)

    def mapper_init(self):
        with open(self.options.stopwords, 'r') as f:
            self.stopwords = set(line.strip() for line in f if line.strip())
        #Precompute Regex
        self.word_split_re = re.compile(r'[^a-zA-Z<>^|]+')
        #Initialize local counter dictionary
        self.local_counts = defaultdict(int)
    
    def mapper(self, _, entry):
        data = json.loads(entry)
        category = data['category']
        review_text = data['reviewText']

        words = map(str.lower, self.word_split_re.split(review_text))

        unique_words = {
            w for w in words
            if len(w) > 1 and w not in self.stopwords
        }
        self.local_counts[('_n_',)] += 1
        self.local_counts[('_cat_', category)] += 1

        for word in unique_words:
            self.local_counts[('_w_', word)] += 1
            self.local_counts[('_A_', category, word)] += 1

        if len(self.local_counts) > 100000:
            for key, count in self.local_counts.items():
                yield key, count
            self.local_counts.clear()

    def mapper_final(self):
        for key, count in self.local_counts.items():
            yield key, count

    def reducer_sum(self, key, values):
        yield key, sum(values)

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