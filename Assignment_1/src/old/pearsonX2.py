# "reviewText"
# "category"

from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import re
import sys

STOPWORDS = '../Assets/stopwords.txt'

def load_stopwords():
    stopwords_path = sys.argv[1] if len(sys.argv) > 1 else STOPWORDS
    with open(stopwords_path, 'r') as f:
        return set(f.read().splitlines())
    
STOPWORDS = load_stopwords()

class PearsonX2(MRJob):
    
    def mapper(self, _, entry):
        
        data = json.loads(entry)
        category = data['category']
        reviewText = data['reviewText']
        
        #Tokenize
        #Case-fold
        reviewWords = map(str.lower, re.split('[^a-zA-Z<>^|]+', reviewText))
        
        
        # total reviews
        yield ('_n_',), 1
        # category total
        yield ('_cat_', category), 1
        #stop word filtering 
        #yielding
        for word in set(reviewWords):
            if word not in STOPWORDS and len(word) > 1:
                # word total
                yield ('_w_', word), 1
                # category-word total
                yield ('_A_', category, word), 1
                
                
    def reducer_count(self, key, counts):
        # sums up the values
        yield (key, sum(counts))
        
    def mapper_word_matcher(self, key, count):
        tag = key[0]

        if tag == '_A_':
            _, category, word = key
            yield word, ('_A_', category, count)

        elif tag == '_w_':
            _, word = key
            yield word, ('_w_', count)

        elif tag == '_cat_':
            _, category = key
            yield ('_cat_', category), count

        elif tag == '_n_':
            yield ('_n_',), count
            
    def reducer_words(self, key, values):
        if isinstance(key, list) and key[0] in ('_cat_', '_n_'):
            for value in values:
                yield tuple(key), value
            return

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

        for category, a_count in a_entries:
            yield (category, key), ('_A_', a_count)
            yield (category, key), ('_w_', w_total)
            
    def mapper_category_matcher(self, key, value):
        # category total
        if key[0] == '_cat_':
            _, category = key
            yield category, ('_cat_', value)
            
        # passthrough n
        elif key[0] == '_n_':
            yield key, value
        
        # partial (category, word)
        else:
            category, word = key
            tag, val = value
            yield category, ('_partial_', word, tag, val)
            
    def reduce_category(self, key, values):
        # passthrough global n
        if isinstance(key, list) and key[0] == '_n_':
            for value in values:
                yield tuple(key), value
            return

        cat_total = None
        partials = []

        for value in values:
            tag = value[0]

            if tag == '_cat_':
                cat_total = value[1]

            elif tag == '_partial_':
                _, word, inner_tag, inner_val = value
                partials.append((word, inner_tag, inner_val))

        if cat_total is None:
            return

        for word, inner_tag, inner_val in partials:
            yield (key, word), (inner_tag, inner_val)
            yield (key, word), ('_cat_', cat_total)
        
        
    def mapper_N_distributor(self, key, value):
        
            
    def steps(self):
        return [
            MRStep(mapper  = self.mapper,
                   reducer = self.reducer_count),
            MRStep(mapper = self.mapper_word_matcher,
                   reducer = self.reducer_words),
            MRStep(mapper = self.mapper_category_matcher,
                   reducer = self.reduce_category)
        ]
        
if __name__ == '__main__':
    PearsonX2.run()
        
        
