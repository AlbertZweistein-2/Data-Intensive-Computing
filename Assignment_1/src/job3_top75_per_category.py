from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import heapq


class Top75PerCategory(MRJob):

    def mapper(self, _, line):
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
        # keep top 75 highest chi2 values for this category
        top75 = heapq.nlargest(75, values, key=lambda x: x[0])

        # output sorted descending by chi2
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