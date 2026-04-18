# Basic Word Count Map-Reduce in Python

from mrjob.job import MRJob
from mrjob.step import MRStep

import re

class WordCounter(MRJob):

    def mapper(self, _, line):
 
        # tokenises each line by using whitespaces, tabs, digits, and the characters ()[]{}.!?,;:+=-_"'`~#@&*%€$§\/ as delimiters 
        word_list = re.split('[^a-zA-Z<>^|]+', line)
              
        #for loop through the terms in pre-processed list
        for word in word_list:
            yield (word, 1)
                
    def reducer_count(self, word, counts):
        # sums up the values of all appearances of the term
        yield  (word, sum(counts))
    
    def steps(self):
        return [
            MRStep(mapper  = self.mapper,
                   reducer = self.reducer_count)
        ]
    
if __name__ == '__main__':
    
    WordCounter.run()