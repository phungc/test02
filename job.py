import yaml
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession

# Read YAML config file
with open("config.yaml", 'r') as stream:
    try:
        config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

# Set up Spark configuration
conf = SparkConf().setAppName("Hive/Impala to Kafka")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 10)

# Set up Kafka parameters
kafka_params = {
    "bootstrap.servers": config['destination']['bootstrap_servers'],
    "security.protocol": config['destination']['security_protocol'],
    "ssl.ca.location": config['destination']['ssl_cafile'],
    "ssl.certificate.location": config['destination']['ssl_certfile'],
    "ssl.key.location": config['destination']['ssl_keyfile']
}

# Create SparkSession for Hive/Impala source
spark = SparkSession.builder \
    .appName("Hive/Impala Source") \
    .config("hive.metastore.uris", "thrift://<metastore>:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# Read data from Hive/Impala
df = spark.sql(config['source']['query'])

# Convert DataFrame to RDD of key-value pairs
rdd = df.rdd.map(lambda row: (row.key, row.value))

# Create Kafka DStream from RDD
kafka_stream = KafkaUtils.createDirectStream(ssc, [config['destination']['topic']], kafka_params, valueDecoder=lambda x: x)

# Write data to Kafka
kafka_stream.foreachRDD(lambda rdd: rdd.foreach(lambda message: kafka_producer.send(config['destination']['topic'], message[1])))

# Start streaming job
ssc.start()
ssc.awaitTermination()
