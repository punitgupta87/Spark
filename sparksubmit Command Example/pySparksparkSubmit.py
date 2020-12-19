## A simple spark submit example

from pyspark.sql import SparkSession

sparkX = SparkSession.builder.appName("pySparksqLite_test").getOrCreate()
print(sparkX.sparkContext.getConf().getAll())
df_flight_info = sparkX.read.format("jdbc").\
    options(url ="jdbc:sqlite:my-sqlite.db",driver="org.sqlite.JDBC",
            dbtable="(select DEST_COUNTRY_NAME,ORIGIN_COUNTRY_NAME,count from flight_info)").load()
print(sparkX.sparkContext.getConf().getAll())
df_flight_info.show()

