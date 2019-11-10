# ALL about spark partitions
# partition concept in spark
# repartition data using a column
# Spark data frame operations and partitioning
# not created any functions this is for learning

import pandas as pd
import datetime
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import regexp_replace,col
from pyspark.sql.functions import sum as _SUM
from pyspark.sql.functions import round as Round



# start time of script , this is to see exact time of execution
print(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
# Create a spark Session
sparkp = SparkSession.builder.appName("partitions").master("local[*]").getOrCreate()
# Load Data in a dataframe and partition it
chicago=sparkp.read.csv("C:/Users/pg186028/Documents/DataVisualization/chicago.csv"
                , inferSchema=True, header=True, nullValue="XX")
chicago2 = chicago.repartition(8, f.col("Department"))

# initial check
print("Default number of partitions:{}".format(chicago.rdd.getNumPartitions()))
print("Number of partitions  after repartitioning:{}".format(chicago2.rdd.getNumPartitions()))
print("Partition column :{}".format(chicago2.rdd.partitioner))

# check length (count) of each partition
print(chicago2.rdd.glom().map(len).collect())

# now lets see how data is distributed in partitions
print("partition Structure")

def f(index,it):
    for x in it:
        yield {str(index) : str(x["Department"])}
rdd_partition = chicago2.rdd.mapPartitionsWithIndex(f).collect() # this will return a list

# take distinct values key value of each partition and put it in a list.
x=[]
for i in rdd_partition:
    if i not in x:
        x.append(i)
print(x)

# let's see which partition has what data
rdd_partiton_key=pd.DataFrame([(k, v) for p in x for (k, v) in p.items()],columns=["a", "b"])
print(rdd_partiton_key)

print("Data Summary")

chicago3=chicago2.withColumn("Employee Annual Salary" ,regexp_replace(col("Employee Annual Salary"), '\\$','')).\
where(col("Department").isNotNull())
chicago3.head(5)

chicago4=chicago3.groupby("Department").agg(Round(_SUM('Employee Annual Salary'),2).alias("X"))
chicago4.show(10,False)  # pretty in spark

# to-do : Write this dataframe to MondoDb-document
print(sparkp.sparkContext._jsc.sc().getExecutorMemoryStatus().size())

# end time of script , this is to see end time of execution
print(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))

