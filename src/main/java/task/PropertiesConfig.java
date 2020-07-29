package task;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;


import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PropertiesConfig {
    private final static String BOOTSTRAP_SERVER = "localhost:9092";
    private final static String GROUP_ID = "testGroup02";
    private final static String CLIENT_ID = "client1";
    private final static String ENABLE_AUTO_COMMIT = "true";
    private final static String AUTO_COMMIT_INTERVAL_MS = "1000";
    private final static String SESSION_TIMEOUT_MS = "30000";
    private final static String KEY_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    private final static String VALUE_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    private final static String ACKS = "all";
    private final static int RETRIES = 0;
    private final static int BATCH_SIZE = 16384;
    private final static int LINGER_MS  = 1;
    private final static int BUFFER_MEMORY = 33554432;
    private final static String KEY_SERIALIZER  = "org.apache.kafka.common.serialization.StringSerializer";
    private final static String VALUE_SERIALIZER =  "org.apache.kafka.common.serialization.StringSerializer";


    public static KafkaConsumer<String, String> initConsumer() {
        Properties props = new Properties();

        props.put("bootstrap.servers", BOOTSTRAP_SERVER);
        props.put("group.id", GROUP_ID);
        props.put("client.id", CLIENT_ID);
        props.put("enable.auto.commit", ENABLE_AUTO_COMMIT);
        props.put("auto.commit.interval.ms", AUTO_COMMIT_INTERVAL_MS);
        props.put("session.timeout.ms", SESSION_TIMEOUT_MS);

        props.put("key.deserializer", KEY_DESERIALIZER);
        props.put("value.deserializer", VALUE_DESERIALIZER);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        return consumer;

    }

    public static Producer<String, String> initProducer() {
        Properties props = new Properties();

        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);

        props.put("acks", ACKS);

        props.put("retries", RETRIES);

        props.put("batch.size", BATCH_SIZE);

        props.put("linger.ms",LINGER_MS);

        props.put("buffer.memory", BUFFER_MEMORY);
        props.put("key.serializer",KEY_SERIALIZER);
        props.put("value.serializer",VALUE_SERIALIZER);
        Producer<String, String> producer = new KafkaProducer(props);
        return producer;
    }

    public static void createTopic(String topicName) {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);

        Map<String, String> configs = new HashMap<>();
        int partitions = 1;
        int replication = 1;
        KafkaAdminClient.create(config).createTopics(Collections.singletonList(new NewTopic("topic", partitions, (short) replication).configs(configs)));

    }
}
