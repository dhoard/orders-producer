package com.github.dhoard.orders.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private static final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";

    // Configuration
    private static final String BOOTSTRAP_SERVERS = "cp-7-4-x:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://cp-7-4-x:8081";
    private static final String ACKS_CONFIG = "all";
    private static final String TOPIC = "order";

    public static void main(String[] args) throws Exception {
        new Main().run(args);
    }

    public void run(String[] args) throws Exception {
        KafkaProducer<String, OrderEvent> kafkaProducer = null;

        try {
            Properties properties = new Properties();

            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class.getName());
            properties.put(ProducerConfig.ACKS_CONFIG, ACKS_CONFIG);
            properties.put(SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);

            Map<Integer, String> facilityMap = new HashMap<>();
            for (int i = 0; i < 1000000; i++) {
                facilityMap.put(i, "facility-" + i);
            }

            kafkaProducer = new KafkaProducer<>(properties);

            for (int i = 0; i < 1000; i++) {
                try {
                    Thread.sleep(randomLong(1, 1000));
                } catch (Throwable t) {
                    // DO NOTHING
                }

                String orderId = UUID.randomUUID().toString();
                String facilityId = facilityMap.get(randomInt(0, 9));

                OrderEvent orderEvent =
                        new OrderEvent(
                                "order.placed",
                                System.currentTimeMillis(),
                                orderId,
                                facilityId);

                LOGGER.info(String.format("event [%s]", orderEvent));

                kafkaProducer.send(new ProducerRecord<>(TOPIC, orderId, orderEvent));

                try {
                    Thread.sleep(randomLong(1, 1000));
                } catch (Throwable t) {
                    // DO NOTHING
                }

                orderEvent =
                        new OrderEvent(
                                "order.fulfilled",
                                System.currentTimeMillis(),
                                orderId,
                                facilityId);

                LOGGER.info(String.format("event [%s]", orderEvent));

                kafkaProducer.send(new ProducerRecord<>(TOPIC, orderId, orderEvent));

                try {
                    Thread.sleep(randomLong(1, 500));
                } catch (InterruptedException e) {
                    // DO NOTHING
                }
            }
        } finally {
            if (null != kafkaProducer) {
                kafkaProducer.flush();
                kafkaProducer.close();
            }
        }
    }

    private static long randomLong(long min, long max) {
        if (max == min) {
            return min;
        }

        if (min > max) {
            throw new IllegalArgumentException("min must be <= max, min = [" + min + "] max = [" + max + "]");
        }

        return (long) (Math.random() * (max - min));
    }

    private static int randomInt(int min, int max) {
        if (max == min) {
            return min;
        }

        if (min > max) {
            throw new IllegalArgumentException("min must be <= max, min = [" + min + "] max = [" + max + "]");
        }

        return (int) (Math.random() * (max - min));
    }
}
