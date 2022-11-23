package com.github.dhoard.orders.kafka.producer;

import com.google.gson.JsonObject;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Future;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private static final String BOOTSTRAP_SERVERS = "cp-7-2-x:9092";

    private static final String TOPIC = "order";

    private static final Random RANDOM = new Random();

    public static void main(String[] args) throws Exception {
        new Main().run(args);
    }

    public void run(String[] args) throws Exception {
        KafkaProducer<String, String> kafkaProducer = null;

        try {
            Properties properties = new Properties();

            properties.setProperty(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

            properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

            Map<Integer, String> facilityMap = new HashMap<>();
            for (int i = 0; i < 10; i++) {
                facilityMap.put(i, "facility-" + i);
            }

            kafkaProducer = new KafkaProducer<>(properties);

            for (int i = 0; i < 1000000; i++) {
                try {
                    Thread.sleep(randomLong(1, 100));
                } catch (Throwable t) {
                    // DO NOTHING
                }

                String orderId = UUID.randomUUID().toString();
                String facilityId = facilityMap.get(randomInt(0, 9));

                JsonObject jsonObject = new JsonObject();
                jsonObject.addProperty("event.type", "order.placed");
                jsonObject.addProperty("event.timestamp", System.currentTimeMillis());
                jsonObject.addProperty("facility.id", facilityId);
                jsonObject.addProperty("order.id", orderId);

                LOGGER.info(String.format("event [%s]", jsonObject));

                kafkaProducer.send(new ProducerRecord<>(TOPIC, null, jsonObject.toString()));

                try {
                    Thread.sleep(randomLong(1, 100));
                } catch (Throwable t) {
                    // DO NOTHING
                }

                jsonObject = new JsonObject();
                jsonObject.addProperty("event.type", "order.fulfilled");
                jsonObject.addProperty("event.timestamp", System.currentTimeMillis());
                jsonObject.addProperty("facility.id", facilityId);
                jsonObject.addProperty("order.id", orderId);

                LOGGER.info(String.format("event [%s]", jsonObject));

                kafkaProducer.send(new ProducerRecord<>(TOPIC, null, jsonObject.toString()));

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
