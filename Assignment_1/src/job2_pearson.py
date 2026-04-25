from mrjob.job import MRJob
from mrjob.step import MRStep
import json


class PearsonChi2(MRJob):
    def configure_args(self):
        super().configure_args()
        self.add_file_arg('--side-data', help='JSON file containing n and category counts')

    def reducer_init(self):
        with open(self.options.side_data, 'r') as f:
            side_data = json.load(f)

        self.n_total = side_data['n']
        self.cat_counts = side_data['cat_counts']

    def mapper(self, _, line):
        line = line.rstrip('\n')
        if not line:
            return

        key_json, count_json = line.split('\t', 1)
        key = json.loads(key_json)
        count = json.loads(count_json)

        tag = key[0]

        if tag == '_w_':
            word = key[1]
            yield word, ('_w_', count)

        elif tag == '_A_':
            category, word = key[1], key[2]
            yield word, ('_A_', category, count)

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

            yield (category, word), {
                'chi2': chi2,
            }

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