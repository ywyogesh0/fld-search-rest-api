package com.fld.search.rest.api.sample;

import com.fld.search.rest.api.util.DateConversionUtility;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Utils;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ConsumerMainByOffset {

    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "test-t-1-g-1";
        String topic = "test-t-1";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        TopicPartition topicPartition1 = new TopicPartition(topic, partition(topic, "yogesh", consumer));
        TopicPartition topicPartition2 = new TopicPartition(topic, partition(topic, "walia", consumer));

        consumer.assign(Arrays.asList(topicPartition1, topicPartition2));

        // seek from 2 offset
        consumer.seek(topicPartition1, 2);

        // poll for new data
        ConsumerRecords<String, String> records =
                consumer.poll(Duration.ofMillis(3000)); // new in Kafka 2.0.0

        for (ConsumerRecord<String, String> record : records) {
            System.out.println("Topic:" + record.topic());
            System.out.println("Key: " + record.key() + ", Value: " + record.value());
            System.out.println("Timestamp: " + DateConversionUtility.timestampToFormattedDate(record.timestamp(),
                    "yyyy-MM-dd HH:mm:ss.SSS"));
            System.out.println("Partition: " + record.partition() + ", Offset:" + record.offset());
        }
    }

    private static int partition(String topic, String key, KafkaConsumer consumer) {
        if (key == null) {
            throw new IllegalArgumentException("ERROR : Key is NULL");
        }

        List partitions = consumer.partitionsFor(topic);
        int numPartitions = partitions.size();

        // hash the keyBytes to choose a partition
        return Utils.toPositive(Utils.murmur2(key.getBytes())) % numPartitions;
    }
}
