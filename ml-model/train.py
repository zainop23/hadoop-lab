from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression

spark = SparkSession.builder.appName("Train").getOrCreate()

data = spark.createDataFrame([
    (0.0, 1.0, 3.0, 0.0),
    (2.0, 0.5, 1.4, 1.0),
    (3.1, 2.0, 0.1, 1.0)
],["f1","f2","f3","label"])

assembler = VectorAssembler(inputCols=["f1","f2","f3"], outputCol="features")
lr = LogisticRegression(featuresCol="features", labelCol="label")

pipeline = Pipeline(stages=[assembler,lr])
model = pipeline.fit(data)

model.save("file:///Users/anwerbasha/hadoop-lab/ml-model/ml_model")
print("MODEL SAVED")
spark.stop()
