pip install opentelemetry-sdk opentelemetry-instrumentation-kafka
import os
import time
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchExportSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.kafka import KafkaInstrumentor

def init_tracer():
    # Configure OTLP exporter to send traces to Kafka
    otlp_exporter = OTLPSpanExporter(
        endpoint=os.getenv("OTLP_ENDPOINT", "localhost:4317")
    )

    # Configure trace provider
    trace.set_tracer_provider(TracerProvider())
    tracer_provider = trace.get_tracer_provider()

    # Create a batch span processor
    span_processor = BatchExportSpanProcessor(otlp_exporter)

    # Add the span processor to the trace provider
    tracer_provider.add_span_processor(span_processor)

    # Enable instrumentation for Kafka
    KafkaInstrumentor().instrument()

    return tracer_provider.get_tracer(__name__)

def send_trace_to_kafka():
    tracer = init_tracer()

    with tracer.start_as_current_span("send_trace_to_kafka"):
        # Simulate sending a trace to Kafka
        print("Sending trace to Kafka...")
        time.sleep(1)
        print("Trace sent to Kafka.")

if __name__ == "__main__":
    send_trace_to_kafka()
