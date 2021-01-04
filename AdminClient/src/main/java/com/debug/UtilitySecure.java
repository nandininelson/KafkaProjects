package com.debug;

import kafka.admin.AdminClient;
import org.apache.kafka.clients.CommonClientConfigs;

import java.util.Properties;

public class UtilitySecure {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "c279-node3.coelab.cloudera.com:6668");
        props.put("security.protocol", "PLAINTEXTSASL");
        props.put("sasl.jaas.config", "com.sun.security.auth.module.Krb5LoginModule required useTicketCache=true renewTicket=true serviceName=\"kafka\";");
        AdminClient adminClient = AdminClient.create(props);
        final AdminClient.ConsumerGroupSummary consumerGroupSummary = adminClient.describeConsumerGroup("test-group", 100);
        System.out.println(consumerGroupSummary);
        System.out.println("----");
        System.out.println(consumerGroupSummary.consumers());
    }
}
