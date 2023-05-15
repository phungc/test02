avro_schema = {
    "type": "record",
    "name": "Example",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
        {"name": "age", "type": "int"}
    ]
}
from confluent_kafka import Producer
from confluent_kafka.avro import AvroProducer

kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "your_topic"

producer_config = {
    "bootstrap.servers": kafka_bootstrap_servers,
    "schema.registry.url": "http://localhost:8081"  # Replace with your schema registry URL
}

producer = AvroProducer(producer_config)
from pyspark.sql.functions import monotonically_increasing_id

df_with_id = df.withColumn("row_id", monotonically_increasing_id())
batch_size = 1000  # Number of rows to process at a time
total_rows = df_with_id.count()

# Iterate over batches
for i in range(0, total_rows, batch_size):
    batch_df = df_with_id.filter((df_with_id.row_id >= i) & (df_with_id.row_id < i + batch_size)).drop("row_id")

    # Convert the batch DataFrame to a list of dictionaries
    batch_data = batch_df.toJSON().collect()
    batch_records = [json.loads(record) for record in batch_data]

    # Publish the Avro records to Kafka
    for record in batch_records:
        producer.produce(topic=kafka_topic, value=record, value_schema=avro_schema)

    # Flush the producer after processing each batch
    producer.flush()

# Close the producer when done
producer.close()
