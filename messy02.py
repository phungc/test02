from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import HiveContext

conf = SparkConf().setAppName("HiveStreamingApp")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, batchDuration=10)

hive_context = HiveContext(sc)

hive_table = "my_table"
sql_query = f"SELECT * FROM {hive_table}"

hive_rdd = hive_context.sql(sql_query).rdd

def process_hive_rdd(time, rdd):
    if not rdd.isEmpty():
        # process rdd here

hive_rdd.foreachRDD(process_hive_rdd)

ssc.start()
ssc.awaitTermination()
