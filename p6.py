from pyspark import SparkContext
sc = SparkContext()

f = sc.textFile('Weather.txt')

temp = f.map(lambda x: (int(x[15:19]), int(x[87:92])))

mini = temp.reduceByKey(lambda a, b: a if a < b else b)
mini.saveAsTextFile('mini')

maxi = temp.reduceByKey(lambda a, b: a if a > b else b)
maxi.saveAsTextFile('maxi')
