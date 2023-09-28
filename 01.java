import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.exporter.jaeger.JaegerGrpcSpanExporter;

public class OpenTelemetryConfig {
    public static void initializeOpenTelemetry() {
        JaegerGrpcSpanExporter jaegerExporter = JaegerGrpcSpanExporter.builder()
                .setEndpoint("http://jaeger-collector:14268/api/traces") // Change to your Jaeger collector URL
                .build();

        Tracer tracer = GlobalOpenTelemetry.getTracer("your-application-name");

        // Register the tracer and exporter
        GlobalOpenTelemetry.set(OpenTelemetrySdk.builder()
                .setTracerProvider(SdkTracerProvider.builder()
                        .addSpanProcessor(SimpleSpanProcessor.builder(jaegerExporter).build())
                        .build())
                .buildAndRegisterGlobal());
    }
}
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class KafkaWordCount {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
            .appName("KafkaWordCount")
            .getOrCreate();

        // Initialize the OpenTelemetry Tracer
        Tracer tracer = GlobalOpenTelemetry.getTracer("your-application-name");

        // Read from Kafka
        Dataset<Row> kafkaStream = spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", "your_kafka_broker")
            .option("subscribe", "your_kafka_topic")
            .load();

        // Process the Kafka records
        kafkaStream
            .selectExpr("CAST(value AS STRING) as message")
            .select(functions.explode(functions.split(functions.col("message"), "\\s+")).as("word"))
            .foreach(row -> {
                // Create a span for received message
                Span receivedSpan = tracer.spanBuilder("Received Message")
                    .startSpan();
                try {
                    // Process the data
                    String word = row.getAs("word");
                    // Create a span for the processing of the word
                    Span processingSpan = tracer.spanBuilder("Process Word")
                        .setAttribute("word", word)
                        .startSpan();
                    try {
                        // Your word processing logic goes here
                        // For example, word counting
                    } finally {
                        processingSpan.end();
                    }
                } finally {
                    receivedSpan.end();
                }
            });

        StreamingQuery query = kafkaStream.writeStream()
            .outputMode("complete")
            .format("console")
            .start();

        query.awaitTermination();
        spark.stop();
    }
}
