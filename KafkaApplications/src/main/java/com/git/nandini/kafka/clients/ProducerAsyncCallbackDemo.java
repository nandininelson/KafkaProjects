package com.git.nandini.kafka.clients;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerAsyncCallbackDemo {
    public static void main(String args[]){

        Logger logger = LoggerFactory.getLogger(ProducerAsyncCallbackDemo.class);
        String topic = "test_topic_2";
        String bootstrapServers = "localhost:9091"; // for multiple brokers separate with comma localhost:9092,localhost:9093,localhost:9094

        Properties props = new Properties();
        //required configs
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //"org.apache.kafka.common.serialization.StringSerializer"
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);

        logger.info("Producing messages");
        try{
            for (int i = 0; i < 10; i++) {
                //sends data asynchronously with Keys
                producer.send(new ProducerRecord<String, String>(topic, Integer.toString(i), "Hello" + Integer.toString(i)), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                       if(null == exception)
                       {
                           logger.info("Received metadata" +
                                   "Topic: " + metadata.topic() +
                                   " Offset: " + metadata.offset() +
                                   " Partition: "+ metadata.partition() +
                                   " TimeStamp: "+ metadata.timestamp()
                           );
                       }else{
                           logger.error("Exception occurred", exception);
                       }
                    }
                });
            }

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            producer.flush();
            producer.close(); // flushes and closes.
        }
    }
}
