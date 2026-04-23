from mrjob.job import MRJob
from mrjob.step import MRStep
import json


class PearsonChi2(MRJob):
    """
    Job 2: Compute the Pearson chi-square statistic for every (category, word) pair.

    Reads the output of Job 1 (counts.jsonl / HDFS job1_counts) and a
    pre-built side-data JSON file that contains:
      - n          : total number of reviews
      - cat_counts : dict mapping category -> number of reviews in that category

    The chi-square formula uses the standard 2x2 contingency table:

                   in category    not in category
    contains w  |      A        |       B        |  A+B  = w_total
    no w        |      C        |       D        |
                   A+C=cat_total   B+D

    chi2 = n * (A*D - B*C)^2 / ((A+B)*(A+C)*(B+D)*(C+D))

    Key-Value design:
      Mapper  input  : raw Job-1 output lines  (key_json TAB count_json)
      Mapper  output : word -> ('_w_', w_total)
                       word -> ('_A_', category, a_count)
      Reducer input  : grouped by word
      Reducer output : (category, word) -> {'chi2': float}

    Optimisation: JOBCONF sets multiple reducers so the reduce phase is
    parallelised across workers (each reducer handles a disjoint key range
    of words because mrjob uses a hash partitioner by default).
    """

    # Run 8 parallel reducers — adjust to the number of available nodes.
    # More reducers = less wall-clock time for the reduce/shuffle phase.
    JOBCONF = {
        'mapreduce.job.reduces': 8,
    }

    def configure_args(self):
        super().configure_args()
        self.add_file_arg('--side-data', help='JSON file containing n and category counts')

    def reducer_init(self):
        """
        Load the side-data file once per reducer task.

        side_data keys:
          n          (int)  : total review count
          cat_counts (dict) : category -> review count in that category
        """
        with open(self.options.side_data, 'r') as f:
            side_data = json.load(f)

        self.n_total = side_data['n']
        self.cat_counts = side_data['cat_counts']

    def mapper(self, _, line):
        """
        Re-emit only the _w_ and _A_ records from Job 1 output, keyed by word.

        Input  key:   byte offset (ignored)
        Input  value: one tab-separated line from Job 1 (key_json TAB count_json)

        Output key:   word (string)
        Output value: ('_w_', count)  or  ('_A_', category, count)

        _n_ and _cat_ records are silently dropped here — they are not
        needed in this job (they live in side_data instead).
        """
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
        """
        Compute chi-square for each (category, word) pair.

        Input  key:   word (string)
        Input  value: stream of ('_w_', count) and ('_A_', category, count) tuples

        Output key:   (category, word) tuple
        Output value: {'chi2': float}

        We must see the _w_ record before we can compute chi2 for any
        (category, word) pair.  mrjob does not guarantee arrival order
        within a group, so we buffer _A_ entries and compute at the end.
        """
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

            denom = (A + B) * (A + C) * (B + D) * (C + D)
            if denom == 0:
                continue

            chi2 = (n_total * ((A * D - B * C) ** 2)) / denom

            yield (category, word), {'chi2': chi2}

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