# runner script to run the Word Count

from wordcount import WordCounter
import json

if __name__ == '__main__':
    
    myjob1 = WordCounter()
    with myjob1.make_runner() as runner:
        runner.run()
        
        for key, value in myjob1.parse_output(runner.cat_output()):           
            print(key, value, "\n", end='')
