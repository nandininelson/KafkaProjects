package com.git.nandini.kafka.core.clients;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerGroupDemo {
    public static void main(String args[]) {

        Logger logger = LoggerFactory.getLogger(ConsumerGroupDemo.class);
        String bootstrapServers = "localhost:9091";
        String topic = "test_topic_2";
        String groupId = "test_group_1";


        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singleton(topic)); // for single topic

        logger.info("Polling for messages");

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10));
                for (ConsumerRecord record : records) {
                    logger.info(    "Topic: " + record.topic() +
                                    " Offset: " + record.offset() +
                                    " Key: " + record.key() +
                                    " Value: " + record.value());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
