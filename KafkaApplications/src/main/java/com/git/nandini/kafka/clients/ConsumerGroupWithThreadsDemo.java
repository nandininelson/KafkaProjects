package com.git.nandini.kafka.clients;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.zookeeper.server.quorum.CommitProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerGroupWithThreadsDemo {
    private ConsumerGroupWithThreadsDemo() {
    }

    public static void main(String args[]) {
        new ConsumerGroupWithThreadsDemo().run();

    }

    private void run() {

        Logger logger = LoggerFactory.getLogger(ConsumerGroupWithThreadsDemo.class);
        String bootstrapServers = "localhost:9091";
        String topic = "test_topic_2";
        String groupId = "test_group_1";

        CountDownLatch latch = new CountDownLatch(1);
        logger.info("Creating consumer thread");
        Runnable consumerThread = new ConsumerRunnable(latch, topic, bootstrapServers, groupId);

        Thread myThread = new Thread(consumerThread);
        myThread.start();

        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( ()-> {
            logger.info("Caught Shutdown Hook");
            ((ConsumerRunnable) consumerThread).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.error("Application Interrupted" + e);
            }
            logger.info("Application exited");
        }));


        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application Interrupted" + e);
        } finally {
            logger.info("Closing the Application.");
        }

    }

}

class ConsumerRunnable implements Runnable{

        Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

        CountDownLatch latch;
        KafkaConsumer<String, String> consumer;
        public ConsumerRunnable(CountDownLatch latch, String topic, String bootstrapServers, String groupId){
            this.latch = latch;
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

            consumer = new KafkaConsumer<String, String>(props);

            consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            logger.info("Polling for messages");

            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord record : records) {
                        logger.info( "Topic: " + record.topic() +
                                " Offset: " + record.offset() +
                                " Key: " + record.key() +
                                " Value: " + record.value());
                    }
                }
            } catch (WakeupException we){
                logger.info("Shutdown signal");
            }
            finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown(){
            consumer.wakeup(); // interrupts consumer.poll() and throws wakeup exception
        }
}

