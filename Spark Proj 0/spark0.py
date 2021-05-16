from pyspark.sql import SparkSession

# Note , we have not given any master here
# but when we get parameters through spark.sparkContext.getConf().getAll() it has already taken it as local.
spark = SparkSession.builder.appName("SparkDB").getOrCreate()

a = spark.sparkContext.textFile("sample_data.csv", 3)
type(a)  # will give : RDD
print(a.getNumPartitions())

b=a.map(lambda x : x.split(","))
print(type(b))
print(b.take(2))