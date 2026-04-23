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
        
        # Regex HIER vorkompilieren für massive CPU-Ersparnis
        self.word_split_re = re.compile(r'[^a-zA-Z<>^|]+')
        
        # In-Mapper Combiner: Lokales Dictionary für die Zähler
        self.local_counts = defaultdict(int)

    def mapper(self, _, entry):
        data = json.loads(entry)
        category = data['category']
        review_text = data['reviewText']

        # Vorkompilierten Regex nutzen
        words = map(str.lower, self.word_split_re.split(review_text))
        
        unique_words = {
            w for w in words
            if len(w) > 1 and w not in self.stopwords
        }

        # Zähler lokal aggregieren statt für jedes Wort an das Hadoop-Framework zu übergeben
        self.local_counts[('_n_',)] += 1
        self.local_counts[('_cat_', category)] += 1

        for word in unique_words:
            self.local_counts[('_w_', word)] += 1
            self.local_counts[('_A_', category, word)] += 1
            
        # Sicherheitsnetz: Wenn das Dictionary zu groß wird, Zwischenergebnisse ausgeben
        # und leeren, um Out-Of-Memory-Fehler im Map-Container zu vermeiden.
        if len(self.local_counts) > 100000:
            for key, count in self.local_counts.items():
                yield key, count
            self.local_counts.clear()

    def mapper_final(self):
        # Am Ende jedes Map-Splits die verbleibenden Zähler ausgeben
        for key, count in self.local_counts.items():
            yield key, count

    def reducer_sum(self, key, values):
        yield key, sum(values)

    def steps(self):
        return [
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper,
                mapper_final=self.mapper_final,  # Neue Phase hinzugefügt
                combiner=self.reducer_sum,
                reducer=self.reducer_sum
            )
        ]


if __name__ == '__main__':
    PearsonCounts.run()