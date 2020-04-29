package com.git.nandini.kafka.core.clients;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerIdempotent {
    public static void main(String args[]){

        String topic = "test_topic_2";
        String bootstrapServers = "localhost:9091"; // for multiple brokers separate with comma localhost:9092,localhost:9093,localhost:9094

        Properties props = new Properties();
        //required configs
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //"org.apache.kafka.common.serialization.StringSerializer"
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //safe producer configs
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);

        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);

        System.out.println("Producing messages");
        try{
            for (int i = 0; i < 100; i++) {
                //sends data asynchronously
                producer.send(new ProducerRecord<String, String>(topic, Integer.toString(i), Integer.toString(i)));
            }

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            producer.flush();
            producer.close(); // flushes and closes.
        }
    }
}
