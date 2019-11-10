# important concept to remember
from pyspark.sql import SparkSession
String = "a quick quick little brown fox jumps over the lazy dog"

sparkp = SparkSession.builder.appName("Word Count").master("local[*]").getOrCreate()

# all the below statements have different results
# reduce by key will not work  on map because map will create
# list of list of characters like [[]] which can not be iterated
# rdd=sparkt.sparkContext.parallelize([string]).flatMap(lambda x : x.split(" ")).map(lambda w:(w,1)).reduceByKey(lambda a,b:a+b)
# here wc2 and wc4 will throw error if you try to run reduce by key--> collect on on it.


wc1 = sparkp.sparkContext.parallelize([String]).flatMap(lambda l: l.split(' ')) # .map(lambda w:(w,1)).reduceByKey(lambda a,b:a+b)
wc2 = sparkp.sparkContext.parallelize([String]).map(lambda l: l.split(' ')) # .map(lambda w:(w,1)).reduceByKey(lambda a,b:a+b)
wc3 = sparkp.sparkContext.parallelize(String).flatMap(lambda l: l.split(' ')) # .map(lambda w:(w,1)).reduceByKey(lambda a,b:a+b)
wc4 = sparkp.sparkContext.parallelize(String).map(lambda l: l.split(' ')) # .map(lambda w:(w,1)).reduceByKey(lambda a,b:a+b)

print(wc1.collect())
print(wc2.collect())
print(wc3.collect())
print(wc4.collect())


