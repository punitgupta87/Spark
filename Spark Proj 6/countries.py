
from pyspark.sql.catalog import *
spark = SparkSession.builder.appName("SparkDB").getOrCreate()

countries = spark.read.option("multiline", "true").option("inferSchema","true").json("countries.json")
# countries.show(truncate=False)
# countries.printSchema()

region=countries.select("name","capital","region","translations.es")
region.write.partitionBy("region").mode("overwrite").format("json").save("./out/")
# we can also add a line separator like
# region.write.partitionBy("region").mode("overwrite").option("lineSep",",\r\n").format("json").save("./out/")


Africa=spark.read.option("inferSchema", "true").json("./out/region=Africa")
Africa.show(truncate=False)