package com.debug;

import kafka.admin.AdminClient;
import org.apache.kafka.clients.CommonClientConfigs;
import java.util.Properties;

public class Utility {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        //props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient adminClient = AdminClient.create(props);
        final AdminClient.ConsumerGroupSummary consumerGroupSummary = adminClient.describeConsumerGroup(args[1], 100);
        //final AdminClient.ConsumerGroupSummary consumerGroupSummary = adminClient.describeConsumerGroup("console-consumer-70972", 100);
        System.out.println(consumerGroupSummary);
        System.out.println("----");
        System.out.println(consumerGroupSummary.consumers());
    }
}
