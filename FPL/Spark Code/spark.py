#import findspark
#findspark.init()
import time
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

'''
def aggregate_tweets_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)
'''

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,2)
ssc.checkpoint("checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",6100)
dataStream.pprint()

'''
#tweet=dataStream.map(tmp)
# OR
tweet=dataStream.map(lambda w:(w.split(';')[0],1))
count=tweet.reduceByKey(lambda x,y:x+y)
#count.pprint()

#TO maintain state
totalcount=tweet.updateStateByKey(aggregate_tweets_count)
totalcount.pprint()
'''

ssc.start()
ssc.awaitTermination(14)
ssc.stop()
