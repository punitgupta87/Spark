from pyspark.sql import SparkSession
from pyspark import SparkConf
import numpy

# change default configurations
cnf = SparkConf()

# get the meaning of local here https://stackoverflow.com/questions/32356143/what-does-setmaster-local-mean-in-spark
# we can pass jar file in spark-submit as well, putting here for testing (as running this script directly from pycharm )
cnf.set("spark.jars", "sqlite-jdbc-3.27.2.jar")
# not setting isolation level for sqplite
#cnf.set("isolationLevel","SERIALIZABLE")
sparkX = SparkSession.builder.appName("pySparksqLite_test").master("local[2]").config(conf=cnf).getOrCreate()

# Setting up a log file through log4j
log4jLogger = sparkX.sparkContext._jvm.org.apache.log4j
log = log4jLogger.LogManager.getLogger("__name__")
log.warn("\n Execution Started \n")

# Configs
spark_confs = sparkX.sparkContext.getConf().getAll()
# this can help check run time configuration
log.warn("Current Spark Configuration: \n {} \n".format(spark_confs))

# here we are reading data from asqlite database by running an aggregate query on a table
# we can also implement error handling in section to check if database exists and it is up and running
def read_sqlite():
    X=sparkX.read.format("jdbc").\
        options(url = "jdbc:sqlite:my-sqlite.db",
                driver="org.sqlite.JDBC",
                dbtable="(select DEST_COUNTRY_NAME,count(count) as Flights from flight_info group by 1 having count(count)>1)").\
        load()
    return X


# in overwrite mode we are writing the data to a table in sqlite data base. ( as a demonstration we will write it to
# MondoDB later)
# We are also creating a table named : "flight-schema2" , on the go to save the result.
# Also here we are using "createTableColumnTypes" option provided by spark
# to manually define the datatype of columns in table.
def write_sqlite(df_flight_info_p_new):
    df_flight_info_p_new.write.mode("overwrite").option("createTableColumnTypes", "DEST_COUNTRY_NAME VARCHAR(200), Flights INT").\
        format("jdbc").options(url="jdbc:sqlite:my-sqlite.db",
                                                 driver="org.sqlite.JDBC",dbtable="flightschema2")\
        .saveAsTable("flightschema2")
    log.warn(" data written successfully to database ")


# Simply demonstrating how we can convert a spark data frame to a pandas data frame and vice-versa
def convert_to_pandas_to_spark(df_flight_info):
    df_flight_info_p = df_flight_info.toPandas()
    # astype is used to shrink the dataframe size for a huge dataset
    df_flight_info_p["Flights"] = df_flight_info_p["Flights"].astype(numpy.int32)
    df_flight_info_p["DEST_COUNTRY_NAME"] = df_flight_info_p["DEST_COUNTRY_NAME"].astype(numpy.str)

    df_flight_info_p_new = sparkX.createDataFrame(df_flight_info_p)
    print(type(df_flight_info_p))
    return df_flight_info_p_new


# main call
if __name__ == "__main__":
    a1 = read_sqlite()
    a2 = convert_to_pandas_to_spark(a1)
    write_sqlite(a2)


