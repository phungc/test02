import yaml
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from typing import Union


class ConnectorFactory:
    def __init__(self, config_file: str):
        with open(config_file, 'r') as stream:
            try:
                self.config = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)

        # Set up Kafka parameters
        if self.config['destination']['type'] == 'kafka':
            self.kafka_params = {
                "bootstrap.servers": self.config['destination']['bootstrap_servers'],
                "security.protocol": self.config['destination']['security_protocol'],
                "ssl.ca.location": self.config['destination']['ssl_cafile'],
                "ssl.certificate.location": self.config['destination']['ssl_certfile'],
                "ssl.key.location": self.config['destination']['ssl_keyfile']
            }

        # Set up Spark configuration
        self.conf = SparkConf().setAppName("Data Connector")
        self.sc = SparkContext(conf=self.conf)
        self.ssc = StreamingContext(self.sc, 10)

        # Create SparkSession
        if self.config['source']['type'] == 'hive':
            self.spark = SparkSession.builder \
                .appName("Hive Source") \
                .config("hive.metastore.uris", "thrift://<metastore>:9083") \
                .enableHiveSupport() \
                .getOrCreate()
        elif self.config['source']['type'] == 'impala':
            self.spark = SparkSession.builder \
                .appName("Impala Source") \
                .config("spark.impala.host", "<impala_host>") \
                .config("spark.impala.port", "<impala_port>") \
                .enableHiveSupport() \
                .getOrCreate()
        else:
            self.spark = None

    def get_data_stream(self) -> Union[None, KafkaUtils]:
        if self.config['source']['type'] == 'hive':
            df = self.spark.sql(self.config['source']['query'])
            rdd = df.rdd.map(lambda row: (row.key, row.value))
            return rdd
        elif self.config['source']['type'] == 'impala':
            df = self.spark.sql(self.config['source']['query'])
            rdd = df.rdd.map(lambda row: (row.key, row.value))
            return rdd
        elif self.config['source']['type'] == 'kafka':
            kafka_stream = KafkaUtils.createDirectStream(self.ssc, [self.config['source']['topic']], self.kafka_params, valueDecoder=lambda x: x)
            return kafka_stream
        else:
            return None
