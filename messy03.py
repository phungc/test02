from io import BytesIO
from avro.io import DatumWriter, BinaryEncoder
from avro.schema import Parse

# Import the necessary KafkaProducer and KafkaProducerConfig classes
from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka.producer import ProducerRecord

# Define the function that will be called for each RDD
def send_to_kafka_avro(rdd):
    # Define the Kafka topic name and the path to the Avro schema file
    kafka_topic = "my-topic"
    avro_schema_path = "/path/to/schema.avsc"
    
    # Parse the Avro schema file and create an Avro writer
    avro_schema = Parse(open(avro_schema_path).read())
    avro_writer = DatumWriter(avro_schema)
    
    # Create a Kafka producer instance
    producer = KafkaProducer(bootstrap_servers=["localhost:9092"])
    
    # Loop through the rows in the RDD
    for row in rdd.collect():
        # Convert the row to an Avro binary representation
        bytes_writer = BytesIO()
        avro_encoder = BinaryEncoder(bytes_writer)
        avro_writer.write(row.asDict(), avro_encoder)
        avro_bytes = bytes_writer.getvalue()

        # Create a Kafka record and send it to the topic
        kafka_record = ProducerRecord(kafka_topic, value=avro_bytes)
        try:
            producer.send(kafka_record).get()
        except KafkaError as e:
            print("Failed to send message to Kafka: ", e)

    # Close the Kafka producer connection
    producer.close()


from kafka import KafkaProducer, KafkaConsumer

# Test producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
producer.send('test_topic', b'Testing Kafka connection from Python')
producer.flush()
print('Message sent to Kafka')

# Test consumer
consumer = KafkaConsumer('test_topic', bootstrap_servers=['localhost:9092'])
for message in consumer:
    print(f"Received message: {message.value.decode('utf-8')}")
