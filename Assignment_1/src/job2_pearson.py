from mrjob.job import MRJob
from mrjob.step import MRStep
import json


# PearsonChi2 is the second MapReduce job.
# It joins the word/category counts from job 1 with the side data and computes
# the chi-squared score for each category-word pair.
class PearsonChi2(MRJob):
    # configure_args() defines command-line arguments for the MRJob runner.
    # The side-data file contains the total number of reviews and per-category counts.
    def configure_args(self):
        super().configure_args()
        self.add_file_arg('--side-data', help='JSON file containing n and category counts')

    # reducer_init() runs once before the reducer processes any values.
    # It loads the side data needed to compute chi-squared statistics.
    def reducer_init(self):
        with open(self.options.side_data, 'r') as f:
            side_data = json.load(f)

        self.n_total = side_data['n']
        self.cat_counts = side_data['cat_counts']

    # mapper(_, line) processes one line of the job 1 output.
    # The first argument is the input key from MRJob and is unused here.
    # The second argument is a tab-separated line containing <key, value> JSON data.
    def mapper(self, _, line):
        line = line.rstrip('\n')
        if not line:
            return

        key_json, count_json = line.split('\t', 1)
        key = json.loads(key_json)
        count = json.loads(count_json)

        tag = key[0]

        # Forward global word counts as <word, ('_w_', count)> pairs.
        if tag == '_w_':
            word = key[1]
            yield word, ('_w_', count)

        # Forward category-word counts as <word, ('_A_', category, count)> pairs.
        elif tag == '_A_':
            category, word = key[1], key[2]
            yield word, ('_A_', category, count)

    # reducer(word, values) receives all counts associated with one word.
    # It combines the global word total with the per-category counts and computes chi-squared.
    def reducer(self, word, values):
        w_total = None
        a_entries = []

        for value in values:
            tag = value[0]

            if tag == '_w_':
                w_total = value[1]
            elif tag == '_A_':
                _, category, a_count = value
                a_entries.append((category, a_count))

        if w_total is None:
            return

        n_total = self.n_total

        # For each category, build the 2x2 contingency table:
        # A = category and word
        # B = word but not category
        # C = category but not word
        # D = neither category nor word
        for category, A in a_entries:
            cat_total = self.cat_counts.get(category)
            if cat_total is None:
                continue

            B = w_total - A
            C = cat_total - A
            D = n_total - w_total - cat_total + A

            enum = n_total * ((A * D - B * C) ** 2)
            denom = (A + B) * (A + C) * (B + D) * (C + D)
            if denom == 0:
                continue

            chi2 = enum / denom

            # Emit the final result as <(category, word), {'chi2': value}>.
            yield (category, word), {
                'chi2': chi2,
            }

    # steps() defines the single MapReduce stage for this job.
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer_init=self.reducer_init,
                reducer=self.reducer
            )
        ]

if __name__ == '__main__':
    PearsonChi2.run()