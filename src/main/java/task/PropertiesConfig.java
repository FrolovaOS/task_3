package task;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;


import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PropertiesConfig {
    private final static String BOOTSTRAP_SERVER = "localhost:9092";
    private final static String GROUP_ID = "testGroup02";
    private final static String CLIENT_ID = "client1";
    private final static String ENABLE_AUTO_COMMIT = "true";
    private final static String AUTO_COMMIT_INTERVAL_MS = "100";
    private final static String SESSION_TIMEOUT_MS = "30000";
    private final static String KEY_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    private final static String VALUE_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    private final static String ACKS = "all";
    private final static int RETRIES = 10;
    private final static int BATCH_SIZE = 16384;
    private final static int LINGER_MS  = 1;
    private final static int BUFFER_MEMORY = 33554432;
    private final static String KEY_SERIALIZER  = "org.apache.kafka.common.serialization.StringSerializer";
    private final static String VALUE_SERIALIZER =  "org.apache.kafka.common.serialization.StringSerializer";
    private final static String AUTO_OFFSET_RESET =  "earliest";

    public static KafkaConsumer<String, String> initConsumer() {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ENABLE_AUTO_COMMIT);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, AUTO_COMMIT_INTERVAL_MS);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, SESSION_TIMEOUT_MS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KEY_DESERIALIZER);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VALUE_DESERIALIZER);
        props.put( ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,AUTO_OFFSET_RESET);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        return consumer;

    }

    public static Producer<String, String> initProducer() {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.put(ProducerConfig.ACKS_CONFIG, ACKS);
        props.put(ProducerConfig.RETRIES_CONFIG, RETRIES);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, BATCH_SIZE);
        props.put(ProducerConfig.LINGER_MS_CONFIG, LINGER_MS);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, BUFFER_MEMORY);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VALUE_SERIALIZER);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true);

        Producer<String, String> producer = new KafkaProducer(props);

        return producer;
    }

    public static void createTopic(String topicName) {
        Properties config = new Properties();


        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);

        Map<String, String> configs = new HashMap<>();


        int partitions = 1;
        int replication = 1;
        KafkaAdminClient.create(config).createTopics(Collections.singletonList(new NewTopic(topicName, partitions, (short) replication).configs(configs)));

    }
}
