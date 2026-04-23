from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import heapq


class Top75PerCategory(MRJob):
    """
    Job 3: Select the top 75 terms per category by chi-square value.

    Reads the output of Job 2 ((category, word) -> {'chi2': float}) and
    emits, for each category, the 75 words with the highest chi2 score
    in descending order.

    Key-Value design:
      Mapper  input  : Job-2 output lines  (key_json TAB value_json)
      Mapper  output : category -> (chi2, word)
      Reducer input  : grouped by category
      Reducer output : category -> [[chi2, word], ...]   (top 75, desc)

    heapq.nlargest is used to avoid a full sort of potentially millions
    of (chi2, word) pairs per category — O(n log k) with k=75.

    Note: only 1 reducer is needed here because there are only 22
    categories (one output record per category).  The data volume is
    tiny at this stage.
    """

    def mapper(self, _, line):
        """
        Parse one Job-2 output line and re-key by category.

        Input  key:   byte offset (ignored)
        Input  value: one tab-separated line  ([category, word] TAB {'chi2': float})

        Output key:   category (string)
        Output value: (chi2, word) tuple
        """
        line = line.rstrip('\n')
        if not line:
            return

        key_json, value_json = line.split('\t', 1)
        key = json.loads(key_json)
        value = json.loads(value_json)

        category, word = key
        chi2 = value['chi2']

        yield category, (chi2, word)

    def reducer(self, category, values):
        """
        Keep the top 75 (chi2, word) pairs for this category.

        Input  key:   category (string)
        Input  value: stream of (chi2, word) tuples

        Output key:   category (string)
        Output value: list of [chi2, word] pairs, sorted descending by chi2,
                      length <= 75
        """
        top75 = heapq.nlargest(75, values, key=lambda x: x[0])
        top75.sort(key=lambda x: x[0], reverse=True)
        yield category, top75

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer
            )
        ]


if __name__ == '__main__':
    Top75PerCategory.run()