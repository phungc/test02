from confluent_kafka import Producer
import io
import fastavro

# Define the Kafka connection parameters
kafka_params = {'bootstrap.servers': 'localhost:9092'}

# Define the Avro schema for the messages
schema = {
    'namespace': 'example.avro',
    'type': 'record',
    'name': 'MyRecord',
    'fields': [
        {'name': 'id', 'type': 'int'},
        {'name': 'name', 'type': 'string'},
        {'name': 'value', 'type': 'float'}
    ]
}

# Create a Kafka producer
producer = Producer(kafka_params)

# Define the topic to send messages to
topic = 'my_topic'

# Create a message to send
message = {'id': 1, 'name': 'John', 'value': 3.14}

# Serialize the message using Avro
bytes_io = io.BytesIO()
fastavro.schemaless_writer(bytes_io, schema, message)
avro_message = bytes_io.getvalue()

# Send the message to Kafka
producer.produce(topic=topic, value=avro_message)

# Wait for any outstanding messages to be delivered and delivery reports to be received
producer.flush()


from confluent_kafka import Consumer, KafkaError
import io
import fastavro

# Define the Kafka connection parameters
kafka_params = {'bootstrap.servers': 'localhost:9092', 'group.id': 'my_group'}

# Define the Avro schema for the messages
schema = {
    'namespace': 'example.avro',
    'type': 'record',
    'name': 'MyRecord',
    'fields': [
        {'name': 'id', 'type': 'int'},
        {'name': 'name', 'type': 'string'},
        {'name': 'value', 'type': 'float'}
    ]
}

# Create a Kafka consumer
consumer = Consumer(kafka_params)

# Subscribe to the Kafka topic
consumer.subscribe(['my_topic'])

# Consume messages from Kafka
while True:
    msg = consumer.poll(timeout=1.0)

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            print('End of partition reached')
        else:
            print('Error while consuming messages: {}'.format(msg.error()))
       
