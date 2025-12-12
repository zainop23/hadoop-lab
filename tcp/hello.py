from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext("local[2]","TCP Socket Wordcount")
sc.setLogLevel("ERROR")
ssc=StreamingContext(sc,5)

lines = ssc.socketTextStream("localhost",9999)
words = lines.flatMap(lambda x: x.split(" "))
pairs = words.map(lambda x: (x,1))
wordCounts = pairs.reduceByKey(lambda x,y:x+y)

wordCounts.pprint()

ssc.start()

ssc.awaitTermination()