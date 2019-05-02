
import os
import sys
from pyspark import SparkContext
import unicodecsv
sc = SparkContext()

input_file = sys.argv[1]
tweets = sc.textFile(input_file).cache()

def extractTweetWords(pid,rows):
  import csv
  
  reader = csv.reader(rows,delimiter='|')
  
  for fields in reader:
    yield (fields[0],(float(fields[1]), float(fields[2])))
    
    
tweets_rdd = tweets.mapPartitionsWithIndex(extractTweetWords)


tweets_view = tweets_rdd.collect()
print(tweets_view[0:5])
