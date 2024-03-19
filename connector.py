import os
import json
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SpanExportResult, SpanExporter, SpanExportResult
from confluent_kafka import Producer

class KafkaExporter(SpanExporter):
    def __init__(self, bootstrap_servers, topic):
        self.producer = Producer({'bootstrap.servers': bootstrap_servers})
        self.topic = topic

    def export(self, spans):
        for span in spans:
            # Convert span data to JSON
            span_data = {
                "name": span.name,
                "span_id": span.span_id,
                "trace_id": span.context.trace_id,
                "parent_id": span.parent.span_id if span.parent else None,
                "attributes": span.attributes,
                # Add more fields as needed
            }

            # Produce the span data to Kafka
            self.producer.produce(self.topic, key=str(span.span_id), value=json.dumps(span_data))

        self.producer.flush()

        return SpanExportResult.SUCCESS

    def shutdown(self):
        self.producer.flush()
        self.producer.close()

def init_tracer():
    # Configure trace provider
    trace.set_tracer_provider(TracerProvider())
    tracer_provider = trace.get_tracer_provider()

    # Initialize Kafka exporter
    kafka_exporter = KafkaExporter(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        topic=os.getenv("KAFKA_TOPIC", "otel-traces")
    )

    # Create a batch span processor
    span_processor = SimpleExportSpanProcessor(kafka_exporter)

    # Add the span processor to the trace provider
    tracer_provider.add_span_processor(span_processor)

    return tracer_provider.get_tracer(__name__)

def send_trace_to_kafka():
    tracer = init_tracer()

    with tracer.start_as_current_span("send_trace_to_kafka"):
        # Simulate an operation
        print("Sending trace to Kafka...")

if __name__ == "__main__":
    send_trace_to_kafka()
