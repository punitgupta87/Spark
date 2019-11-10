# Streaming in spark
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext


# simple spark streaming
sparkStreaming2 = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()
print(sparkStreaming2.sparkContext.uiWebUrl)

spark_confs = sparkStreaming2.sparkContext.getConf().getAll()
print("Current Spark Configuration \n", spark_confs)

# read one file to get schema
spark_json = sparkStreaming2.read.json("part-00008-tid-730451297822678341-1dda7027-2071-4d73-a0e2-7fb6a91e1d1f-0-c000.json")
json_schema = spark_json.schema

# create a streaming context
setJsonStream = sparkStreaming2.readStream.schema(json_schema).option("maxFilesPerTrigger", 1). \
    csv("activity-data")
sql1 = setJsonStream.withColumn("timestamp", f.current_timestamp()).withWatermark("timestamp", "2 minutes"). \
    groupby(f.window("timestamp", "2 minutes"), "gt").count()

# start streaming
jsonQuery = sql1.writeStream.outputMode("update").trigger(processingTime='15 seconds').format("console").start()
jsonQuery.awaitTermination()

