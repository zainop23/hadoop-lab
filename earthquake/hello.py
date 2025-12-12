from pyspark import SparkContext
import csv

sc = SparkContext()

rdd = sc.textFile("file:///Users/anwerbasha/hadoop-lab/earthquake/input.csv")
base = "file:///Users/anwerbasha/hadoop-lab/earthquake/"
header = rdd.first()

rows = rdd.filter(lambda r: r!=header).map(lambda r: next(csv.reader([r])))

tasks = {
    4 : "lat",
    5 : "lon",
    6 : "mag",
    7 : "dep"
}

for col,folder in tasks.items():
    rows.map(lambda r: (r[9], float(r[col]))) \
    .reduceByKey(max).saveAsTextFile(base+folder)