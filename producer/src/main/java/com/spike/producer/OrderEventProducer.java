package com.spike.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class OrderEventProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);

    private static final String TOPIC = System.getenv().getOrDefault("KAFKA_TOPIC", "orders");
    private static final String BOOTSTRAP_SERVERS = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    private static final int MESSAGE_COUNT = Integer.parseInt(System.getenv().getOrDefault("MESSAGE_COUNT", "20"));
    private static final long DELAY_MS = Long.parseLong(System.getenv().getOrDefault("DELAY_MS", "500"));

    public static void main(String[] args) throws Exception {
        log.info("Starting OrderEventProducer — broker={}, topic={}, count={}", BOOTSTRAP_SERVERS, TOPIC, MESSAGE_COUNT);

        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        Properties props = producerProperties();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                OrderEvent event = OrderEvent.random();
                String key = event.getOrderId();
                String value = mapper.writeValueAsString(event);

                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, value);

                try {
                    RecordMetadata meta = producer.send(record).get();
                    log.info("Sent [{}] → partition={}, offset={} | {}",
                            i + 1, meta.partition(), meta.offset(), event);
                } catch (ExecutionException e) {
                    log.error("Failed to send record {}: {}", i + 1, e.getCause().getMessage());
                }

                if (DELAY_MS > 0) {
                    Thread.sleep(DELAY_MS);
                }
            }

            producer.flush();
            log.info("Done. {} messages sent to topic '{}'.", MESSAGE_COUNT, TOPIC);
        }
    }

    private static Properties producerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Durability: wait for all in-sync replicas to acknowledge
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // Retry on transient failures
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 500);
        // Idempotent producer — prevents duplicate messages on retry
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return props;
    }
}
