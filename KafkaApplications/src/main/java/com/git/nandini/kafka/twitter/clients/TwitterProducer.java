package com.git.nandini.kafka.twitter.clients;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    public static final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
    public static final String consumerKey = "consumerKey";
    public static final String consumerSecret = "consumerSecret";
    public static final String token = "token";
    public static final String secret = "secret";

    public static final String topic = "twitter_tweets";
    public static final String bootstrapServers = "localhost:9091";

    public TwitterProducer(){

    }

    public void run(){
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        // create twitter client
        Client twitterClient = createTwitterClient(msgQueue);
        // Attempts to establish a connection.
        twitterClient.connect();

        // create a kafka producer
        Producer<String,String> producer = createKafkaProducer();

        Runtime.getRuntime().addShutdownHook(new Thread( ()-> {
            logger.info("Stopping the Application");
            logger.info("Shtting down twitter client");
            twitterClient.stop();
            logger.info("Closing Producer");
            producer.close();
            logger.info("Application exited");
        }));


        // send tweets in a loop
        // on a different thread, or multiple different threads....
        while (!twitterClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);

            } catch (InterruptedException e) {
                e.printStackTrace();
                twitterClient.stop();
            }
            if(msg !=null ){
                producer.send(new ProducerRecord<String, String>(topic, null, msg), new Callback() {
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
        }
    }

    private Producer<String, String> createKafkaProducer() {
        Properties props = new Properties();
        //required configs
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //"org.apache.kafka.common.serialization.StringSerializer"
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
        return producer;
    }

    public Client createTwitterClient(BlockingQueue msgQueue){
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();


        List<String> terms = Lists.newArrayList("kafka", "api");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file

        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);


        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        return hosebirdClient;

    }

    public static void main(String args[]){
        new TwitterProducer().run();
    }
}
