import org.apache.kafka.clients.admin.*;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaUtility {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient admin = AdminClient.create(config);
        listTopics(admin);
        listGroups(admin);
        describeAllGroups(admin);
    }

    private static void describeAllGroups(AdminClient admin) throws InterruptedException, ExecutionException {
        System.out.println("==== ALL GROUPS DESCRIBE ====");
        for( ConsumerGroupListing groupListing : admin.listConsumerGroups().all().get()){
            System.out.println("----Describing "+ groupListing.groupId());
                //System.out.println(admin.describeConsumerGroups(Arrays.asList(groupListing.groupId().toString())).describedGroups().toString());

        }
    }

    private static void listGroups(AdminClient admin) throws InterruptedException, ExecutionException {
        System.out.println("==== ALL GROUPS ====");
        for( ConsumerGroupListing groupListing : admin.listConsumerGroups().all().get()){
            System.out.println(groupListing);
        }
    }

    private static void listTopics(AdminClient admin) throws InterruptedException, ExecutionException {
        System.out.println("==== ALL TOPICS ====");
        for (TopicListing topicListing : admin.listTopics().listings().get()) {
            System.out.println(topicListing);
        }
    }
}
