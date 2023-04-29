# Hive source data info
hive_source:
  database: my_database
  table: my_table
  columns:
    - column_1
    - column_2
    - column_3
  rules:
    - column_1: "string"
    - column_2: "int"
    - column_3: "double"

# Kafka destination data info
kafka_destination:
  topic: my_topic
  servers:
    - kafka-server1:9092
    - kafka-server2:9092
  key_serializer: org.apache.kafka.common.serialization.StringSerializer
  value_serializer: org.apache.kafka.common.serialization.StringSerializer

    from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import yaml

# Read the YAML config file
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Set up the SparkSession
spark = SparkSession \
    .builder \
    .appName("hive-to-kafka") \
    .getOrCreate()

# Set up the Kafka producer
kafka_producer = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", ",".join(config["kafka_destination"]["servers"])) \
    .option("topic", config["kafka_destination"]["topic"]) \
    .option("key.serializer", config["kafka_destination"]["key_serializer"]) \
    .option("value.serializer", config["kafka_destination"]["value_serializer"]) \
    .load()

# Set up the Hive source query
hive_query = "SELECT " + ", ".join(config["hive_source"]["columns"]) + " FROM " + \
             config["hive_source"]["database"] + "." + config["hive_source"]["table"]

# Read data from Hive
hive_data = spark \
    .readStream \
    .format("hive") \
    .option("query", hive_query) \
    .load()

# Apply data type casting rules
for rule in config["hive_source"]["rules"]:
    column_name, data_type = rule.items()[0]
    hive_data = hive_data.withColumn(column_name, col(column_name).cast(data_type))

# Write data to Kafka
hive_data \
    .selectExpr(*config["hive_source"]["columns"]) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", ",".join(config["kafka_destination"]["servers"])) \
    .option("topic", config["kafka_destination"]["topic"]) \
    .option("checkpointLocation", "/path/to/checkpoint/dir") \
    .start()

# Start the streaming query
spark.streams.awaitAnyTermination()


# Hive source data info
hive_source:
  database: my_database
  table: my_table
  columns:
    - column_1
    - column_2
    - column_3
  rules:
    - column_1:
        equals: "foo"
    - column_2:
        regex: "^\\d{3}-\\d{2}-\\d{4}$"
    - column_3:
        range: [0.0, 1.0]

# Kafka destination data info
kafka_destination:
  topic: my_topic
  servers:
    - kafka-server1:9092
    - kafka-server2:9092
  key_serializer: org.apache.kafka.common.serialization.StringSerializer
  value_serializer: org.apache.kafka.common.serialization.StringSerializer

    from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import yaml

# Read the YAML config file
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Set up the SparkSession
spark = SparkSession \
    .builder \
    .appName("hive-to-kafka") \
    .getOrCreate()

# Set up the Kafka producer
kafka_producer = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", ",".join(config["kafka_destination"]["servers"])) \
    .option("topic", config["kafka_destination"]["topic"]) \
    .option("key.serializer", config["kafka_destination"]["key_serializer"]) \
    .option("value.serializer", config["kafka_destination"]["value_serializer"]) \
    .load()

# Set up the Hive source query
hive_query = "SELECT " + ", ".join(config["hive_source"]["columns"]) + " FROM " + \
             config["hive_source"]["database"] + "." + config["hive_source"]["table"]

# Read data from Hive
hive_data = spark \
    .readStream \
    .format("hive") \
    .option("query", hive_query) \
    .load()

# Apply data type casting rules
for rule in config["hive_source"]["rules"]:
    column_name, rule_dict = rule.items()[0]
    if "equals" in rule_dict:
        hive_data = hive_data.filter(col(column_name) == rule_dict["equals"])
    elif "regex" in rule_dict:
        hive_data = hive_data.filter(col(column_name).rlike(rule_dict["regex"]))
    elif "range" in rule_dict:
        hive_data = hive_data.filter(col(column_name).between(rule_dict["range"][0], rule_dict["range"][1]))
    else:
        raise ValueError(f"Invalid rule for column {column_name}")

# Write data to Kafka
hive_data \
    .selectExpr(*config["hive_source"]["columns"]) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap

            from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import yaml

# Read the YAML config file
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Set up the SparkSession
spark = SparkSession \
    .builder \
    .appName("impala-to-kafka") \
    .config("spark.impala.host", config["impala_source"]["host"]) \
    .config("spark.impala.port", config["impala_source"]["port"]) \
    .getOrCreate()

# Set up the Kafka producer
kafka_producer = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", ",".join(config["kafka_destination"]["servers"])) \
    .option("topic", config["kafka_destination"]["topic"]) \
    .option("key.serializer", config["kafka_destination"]["key_serializer"]) \
    .option("value.serializer", config["kafka_destination"]["value_serializer"]) \
    .load()

# Set up the Impala source query
impala_query = "SELECT " + ", ".join(config["impala_source"]["columns"]) + " FROM " + \
               config["impala_source"]["database"] + "." + config["impala_source"]["table"]

# Read data from Impala
impala_data = spark \
    .readStream \
    .format("jdbc") \
    .option("url", f"jdbc:impala://{config['impala_source']['host']}:{config['impala_source']['port']}/{config['impala_source']['database']}") \
    .option("dbtable", f"({impala_query}) AS impala_query") \
    .option("user", config["impala_source"]["user"]) \
    .option("password", config["impala_source"]["password"]) \
    .option("driver", "com.cloudera.impala.jdbc41.Driver") \
    .load()

# Apply data type casting rules
for rule in config["impala_source"]["rules"]:
    column_name, rule_dict = rule.items()[0]
    if "equals" in rule_dict:
        impala_data = impala_data.filter(col(column_name) == rule_dict["equals"])
    elif "regex" in rule_dict:
        impala_data = impala_data.filter(col(column_name).rlike(rule_dict["regex"]))
    elif "range" in rule_dict:
        impala_data = impala_data.filter(col(column_name).between(rule_dict["range"][0], rule_dict["range"][1]))
    else:
        raise ValueError(f"Invalid rule for column {column_name}")

# Write data to Kafka
impala_data \
    .selectExpr(*config["impala_source"]["columns"]) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", ",".join(config["kafka_destination"]["servers"])) \
    .option("topic", config["kafka_destination"]["topic"]) \
    .option("key.serializer", config["kafka_destination"]["key_serializer"]) \
    .option("value.serializer", config["kafka_destination"]["value_serializer"]) \
    .start() \
    .awaitTermination()

            
            from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import yaml

# Read the YAML config file
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Set up the SparkSession
spark = SparkSession \
    .builder \
    .appName("kafka-to-kafka") \
    .getOrCreate()

# Set up the Kafka consumer
kafka_consumer = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", ",".join(config["kafka_source"]["servers"])) \
    .option("subscribe", config["kafka_source"]["topic"]) \
    .load()

# Convert Kafka message value to string
kafka_data = kafka_consumer \
    .selectExpr("CAST(value AS STRING)")

# Convert string to structured data
kafka_data = kafka_data \
    .select(from_json(col("value"), config["kafka_source"]["schema"]).alias("data")) \
    .selectExpr("data.*")

# Apply data type casting rules
for rule in config["kafka_source"]["rules"]:
    column_name, rule_dict = rule.items()[0]
    if "equals" in rule_dict:
        kafka_data = kafka_data.filter(col(column_name) == rule_dict["equals"])
    elif "regex" in rule_dict:
        kafka_data = kafka_data.filter(col(column_name).rlike(rule_dict["regex"]))
    elif "range" in rule_dict:
        kafka_data = kafka_data.filter(col(column_name).between(rule_dict["range"][0], rule_dict["range"][1]))
    else:
        raise ValueError(f"Invalid rule for column {column_name}")

# Set up the Kafka producer
kafka_producer = kafka_data \
    .selectExpr(*config["kafka_source"]["columns"]) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", ",".join(config["kafka_destination"]["servers"])) \
    .option("topic", config["kafka_destination"]["topic"]) \
    .option("key.serializer", config["kafka_destination"]["key_serializer"]) \
    .option("value.serializer", config["kafka_destination"]["value_serializer"]) \
    .start() \
    .awaitTermination()

            
            from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import yaml

# Read the YAML config file
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Set up the SparkSession
spark = SparkSession \
    .builder \
    .appName("kafka-to-kafka") \
    .getOrCreate()

# Set up the Kafka consumer
kafka_consumer = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", ",".join(config["kafka_source"]["servers"])) \
    .option("subscribe", config["kafka_source"]["topic"]) \
    .load()

# Convert Kafka message value to bytes and deserialize to Arvo format
kafka_data = kafka_consumer \
    .selectExpr("value AS bytes") \
    .select(from_avro(col("bytes"), config["kafka_source"]["schema"]).alias("data")) \
    .selectExpr("data.*")

# Apply data type casting rules
for rule in config["kafka_source"]["rules"]:
    column_name, rule_dict = rule.items()[0]
    if "equals" in rule_dict:
        kafka_data = kafka_data.filter(col(column_name) == rule_dict["equals"])
    elif "regex" in rule_dict:
        kafka_data = kafka_data.filter(col(column_name).rlike(rule_dict["regex"]))
    elif "range" in rule_dict:
        kafka_data = kafka_data.filter(col(column_name).between(rule_dict["range"][0], rule_dict["range"][1]))
    else:
        raise ValueError(f"Invalid rule for column {column_name}")

# Set up the Kafka producer
kafka_producer = kafka_data \
    .select(to_avro(struct(*config["kafka_source"]["columns"])).alias("value")) \
    .selectExpr("NULL AS key", "value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", ",".join(config["kafka_destination"]["servers"])) \
    .option("topic", config["kafka_destination"]["topic"]) \
    .option("key.serializer", config["kafka_destination"]["key_serializer"]) \
    .option("value.serializer", config["kafka_destination"]["value_serializer"]) \
    .start() \
    .awaitTermination()

            from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import yaml

# Read the YAML config file
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Set up the SparkSession
spark = SparkSession \
    .builder \
    .appName("hive-to-kafka") \
    .config("hive.metastore.uris", config["hive_source"]["metastore_uris"]) \
    .enableHiveSupport() \
    .getOrCreate()

# Set up the Hive data source
hive_data = spark.sql(f"SELECT {','.join(config['hive_source']['columns'])} FROM {config['hive_source']['table']}")

# Apply mapping rules
for rule in config["hive_source"]["mapping_rules"]:
    source_column, dest_column = rule.items()[0]
    hive_data = hive_data.withColumnRenamed(source_column, dest_column)

# Apply data type casting rules
for rule in config["hive_source"]["rules"]:
    column_name, rule_dict = rule.items()[0]
    if "equals" in rule_dict:
        hive_data = hive_data.filter(col(column_name) == rule_dict["equals"])
    elif "regex" in rule_dict:
        hive_data = hive_data.filter(col(column_name).rlike(rule_dict["regex"]))
    elif "range" in rule_dict:
        hive_data = hive_data.filter(col(column_name).between(rule_dict["range"][0], rule_dict["range"][1]))
    else:
        raise ValueError(f"Invalid rule for column {column_name}")

# Set up the Kafka producer
kafka_producer = hive_data \
    .select(to_avro(struct(*[col(c).alias(config["hive_source"]["mapping_rules"].get(c, c)) for c in config["hive_source"]["columns"]])).alias("value")) \
    .selectExpr("NULL AS key", "value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", ",".join(config["kafka_destination"]["servers"])) \
    .option("topic", config["kafka_destination"]["topic"]) \
    .option("key.serializer", config["kafka_destination"]["key_serializer"]) \
    .option("value.serializer", config["kafka_destination"]["value_serializer"]) \
    .start() \
    .awaitTermination()

            
            hive_source:
  metastore_uris: "thrift://localhost:9083"
  table: "my_table"
  columns:
    - "col1"
    - "col2"
    - "col3"
  mapping_rules:
    col1: new_col1
    col2: new_col2
  rules:
    - col1:
        equals: "some value"
    - col2:
        regex: "^pattern.*$"
    - col3:
        range:
          - 0
          - 100
kafka_destination:
  servers:
    - "localhost:9092"
  topic: "my_topic"
  key_serializer: "org.apache.kafka.common.serialization.StringSerializer"
  value_serializer: "io.confluent.kafka.serializers.KafkaAvroSerializer"

            import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from kafka import KafkaProducer


def load_config():
    with open("config.yaml", "r") as f:
        return yaml.safe_load(f)


def create_spark_session(app_name):
    return SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .getOrCreate()


def start_streaming_job():
    config = load_config()
    hive_config = config['hive_source']
    kafka_config = config['kafka_destination']
    mapping_rules = hive_config.get('mapping_rules', {})

    # create the Spark session
    spark = create_spark_session(app_name="HiveToKafkaStreamingJob")

    # create the Kafka producer
    kafka_producer = KafkaProducer(
        bootstrap_servers=kafka_config['servers'],
        key_serializer=kafka_config['key_serializer'],
        value_serializer=kafka_config['value_serializer'],
    )

    # define the schema for the Hive table
    schema = StructType([
        StructField(mapping_rules.get(col_name, col_name), StringType(), True)
        for col_name in hive_config['columns']
    ])

    # define the query to read from Hive and push to Kafka
    query = spark.table(hive_config['table']) \
        .selectExpr([f"{col_name} as {mapping_rules.get(col_name, col_name)}" for col_name in hive_config['columns']]) \
        .writeStream \
        .foreachBatch(lambda batch_df, batch_id: push_to_kafka(batch_df, kafka_producer)) \
        .start()

    # check the timestamp column in the Hive table
    timestamp_col = hive_config.get('timestamp_column')
    if timestamp_col:
        initial_timestamp = spark.sql(f"SELECT MAX({timestamp_col}) FROM {hive_config['table']}").collect()[0][0]
        while not query.exception:
            current_timestamp = spark.sql(f"SELECT MAX({timestamp_col}) FROM {hive_config['table']}").collect()[0][0]
            if current_timestamp != initial_timestamp:
                print(f"Data in Hive table {hive_config['table']} has been truncated and repopulated with new data.")
                initial_timestamp = current_timestamp

    query.awaitTermination()


def push_to_kafka(df, producer):
    # define the schema for the Avro data
    schema = {
        "type": "record",
        "name": "my_record",
        "fields": [
            {"name": col_name, "type": ["null", "string"]} for col_name in df.columns
        ]
    }

    # serialize the data as Avro and push to Kafka
    avro_data = df.toJSON().map(lambda x: json.loads(x)).map(lambda x: {col_name: x.get(col_name) for col_name in df.columns})
    avro_data = avro_data.map(lambda x: to_json(x, schema))
    avro_data.foreach(lambda x: producer.send(topic=kafka_config['topic'], value=x))


if __name__ == '__main__':
    start_streaming_job()
