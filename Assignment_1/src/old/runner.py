from Assignment_1.src.old.pearsonX2 import PearsonX2
import json

if __name__ == '__main__':
    
    myjob1 = PearsonX2()
    with myjob1.make_runner() as runner:
        runner.run()
        
        for key, value in myjob1.parse_output(runner.cat_output()):           
            print(key, value, "\n", end='')