from pyspark.sql import SparkSession, Row
from pyspark.sql import dataframe
from pyspark.sql.types import *
from pyspark.sql.catalog import *

# Note , we have not given any master here
# but when we get parameters through spark.sparkContext.getConf().getAll() it has already taken it as local.
spark = SparkSession.builder.appName("SparkDB").enableHiveSupport().getOrCreate()
# this will help us getting the web-url for our spark session to check progress
print(spark.sparkContext.uiWebUrl)
print(spark.sparkContext.getConf().getAll())


# lets create a schema to be used in a data-frame
field = [StructField("field1", StringType(), True),StructField("field2", IntegerType(), True)]
schema = StructType(field)

# access spark SQL, here we are creating database named "grouped" and creating a table in it
spark.conf.set("spark.sql.shuffle.partitions", "5")
spark.sql("""Create database grouped2""")
spark.catalog.setCurrentDatabase("grouped2")
spark.createDataFrame(spark.sparkContext.emptyRDD(), schema).write.mode("overwrite").saveAsTable("aggregates")
# a partition by column can also be specified in above statement before saveastable
spark.sql("""insert into aggregates values ('Punit',2)""")
spark.sql("""select * from aggregates""").show()
spark.sql("""create table test (name string) using hive """)
spark.sql("""insert into test values ('teradata')""")
spark.sql("""select * from test""").show()

# you can use spark catalog to create, list and drop tables
# print(spark.catalog.listTables())
# print(spark.catalog.listDatabases)
# print(spark.table("aggregates"))
print(spark.catalog.listTables("grouped2"))
# print(spark.sql("DESCRIBE FORMATTED aggregates"))

