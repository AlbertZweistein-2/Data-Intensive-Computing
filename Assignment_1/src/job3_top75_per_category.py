from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import heapq


# Top75PerCategory is the third MapReduce job.
# It reads the chi-squared scores from job 2 and keeps the 75 strongest words
# for each category.
class Top75PerCategory(MRJob):

    # mapper(_, line) processes one line from the job 2 output.
    # The first argument is the input key from MRJob and is unused here.
    # The second argument is a tab-separated line containing <key, value> JSON data.
    def mapper(self, _, line):
        line = line.rstrip('\n')
        if not line:
            return

        key_json, value_json = line.split('\t', 1)
        key = json.loads(key_json)
        value = json.loads(value_json)

        category, word = key
        chi2 = value['chi2']

        # Emit <category, (chi2, word)> so the reducer can group all words by category.
        yield category, (chi2, word)

    # reducer(category, values) receives all (chi2, word) pairs for one category.
    # It keeps the 75 highest chi-squared values and returns them sorted descending.
    def reducer(self, category, values):
        top75 = heapq.nlargest(75, values, key=lambda x: x[0])

        top75.sort(key=lambda x: x[0], reverse=True)

        # Emit <category, top75_list> for the final formatted output.
        yield category, top75

    # steps() defines the single MapReduce stage for this job.
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer
            )
        ]

if __name__ == '__main__':
    Top75PerCategory.run()