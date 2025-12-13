from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import split, col

spark = SparkSession.builder.appName("StreamML").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

model = PipelineModel.load("ml_model_lr")

lines = spark.readStream.format("socket") \
    .option("host", "localhost").option("port", 9999).load()

df = lines.select(
    split(col("value"), ",").getItem(0).cast("double").alias("f1"),
    split(col("value"), ",").getItem(1).cast("double").alias("f2"),
    split(col("value"), ",").getItem(2).cast("double").alias("f3")
)

pred = model.transform(df)

pred.select("f1", "f2", "f3", "prediction", "probability") \
    .writeStream.format("console").outputMode("append").start() \
    .awaitTermination()