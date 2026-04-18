import pyspark
import random
from pyspark.sql import SparkSession

sc = pyspark.SparkContext(appName="Pi")
sc.setLogLevel('OFF')

num_samples = 10000
def inside(p):     
  x, y = random.random(), random.random()
  return x*x + y*y < 1
count = sc.parallelize(range(0, num_samples)).filter(inside).count()
pi = 4 * count / num_samples
print(pi)
sc.stop()


