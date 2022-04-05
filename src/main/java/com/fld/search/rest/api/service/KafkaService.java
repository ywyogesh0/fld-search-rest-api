package com.fld.search.rest.api.service;

import com.cedarsoftware.util.io.JsonWriter;
import com.fld.search.rest.api.model.ConfigurationParams;
import com.fld.search.rest.api.model.KafkaTopicParams;
import com.fld.search.rest.api.util.DateConversionUtility;
import com.opencsv.CSVReader;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Utils;
import org.json.JSONObject;

import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.*;

public class KafkaService implements KafkaServiceInterface {

    private String topics;
    private String poll;
    private String csvPath;
    private String dateFormatter;
    private Properties consumerProperties = new Properties();
    private Set<String> topicsSet = new HashSet<>();
    private Set<KafkaTopicParams> topicsParams = new HashSet<>();
    private List<JSONObject> jsonObjectList = new ArrayList<>();

    @Override
    public String findMessagesByTopicAndKey(String topic, String key, ConfigurationParams configurationParams) {
        topicsParams.add(new KafkaTopicParams(topic, key, null, null));

        setParams(configurationParams);
        prepareTopicMessagesDump();

        return formattedJSON();
    }

    @Override
    public String findMessagesByKey(String key, ConfigurationParams configurationParams) {
        this.topics = configurationParams.getTopics();

        populateTopicsSet();
        topicsSet.forEach(topic -> topicsParams.add(new KafkaTopicParams(topic, key, null, null)));

        setParams(configurationParams);
        prepareTopicMessagesDump();

        return formattedJSON();
    }

    @Override
    public String findMessagesByTopicWithKeyAndOffset(String topic, String key, String offset,
                                                      ConfigurationParams configurationParams) {
        topicsParams.add(new KafkaTopicParams(topic, key, offset, null));

        setParams(configurationParams);
        prepareTopicMessagesDump();

        return formattedJSON();
    }

    @Override
    public String findMessagesByKeyAndOffset(String key, String offset, ConfigurationParams configurationParams) {
        this.topics = configurationParams.getTopics();

        populateTopicsSet();
        topicsSet.forEach(topic -> topicsParams.add(new KafkaTopicParams(topic, key, offset, null)));

        setParams(configurationParams);
        prepareTopicMessagesDump();

        return formattedJSON();
    }

    @Override
    public String findMessagesByTopicWithKeyAndTimestamp(String topic, String key, String timestamp,
                                                         ConfigurationParams configurationParams) {
        topicsParams.add(new KafkaTopicParams(topic, key, null, timestamp));

        setParams(configurationParams);
        prepareTopicMessagesDump();

        return formattedJSON();
    }

    @Override
    public String findMessagesByKeyAndTimestamp(String key, String timestamp, ConfigurationParams configurationParams) {
        this.topics = configurationParams.getTopics();

        populateTopicsSet();
        topicsSet.forEach(topic -> topicsParams.add(new KafkaTopicParams(topic, key, null, timestamp)));

        setParams(configurationParams);
        prepareTopicMessagesDump();

        return formattedJSON();
    }

    @Override
    public String findMessages(ConfigurationParams configurationParams) throws IOException {
        this.topics = configurationParams.getTopics();
        this.csvPath = configurationParams.getCsvPath();

        populateTopicsSet();
        populateTopicParamsWithCSV();
        setParams(configurationParams);
        prepareTopicMessagesDump();

        return formattedJSON();
    }

    private void setParams(ConfigurationParams configurationParams) {
        this.poll = configurationParams.getPollDuration();
        this.dateFormatter = configurationParams.getDateFormatter();

        // create consumer configs
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configurationParams.getBootstrapServers());
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, configurationParams.getGroupId());
        consumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    private void populateTopicParamsWithCSV() throws IOException {
        String[] csvLines;
        try (CSVReader csvReader = new CSVReader(new FileReader(csvPath))) {
            csvReader.skip(1);
            while ((csvLines = csvReader.readNext()) != null) {
                for (String topic : topicsSet) {
                    topicsParams.add(new KafkaTopicParams(topic, csvLines[0], csvLines[1], csvLines[2]));
                }
            }
        }
    }

    private void populateTopicsSet() {
        String[] topicsArr = topics.split(",");
        if (topicsArr.length > 1) {
            Collections.addAll(topicsSet, topicsArr);
        } else {
            topicsSet.add(topics);
        }
    }

    private void prepareTopicMessagesDump() {
        topicsParams.forEach(topicsParam -> {

            boolean exitCondition = false;
            final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);

            String topic = topicsParam.getTopic();
            String key = topicsParam.getKey();
            String offset = topicsParam.getOffset();
            String date = topicsParam.getDate();

            TopicPartition topicPartition = new TopicPartition(topic, partition(topic, key, consumer));
            consumer.assign(Collections.singleton(topicPartition));

            // find last partition offset
            consumer.seekToEnd(Collections.singleton(topicPartition));
            long lastOffset = consumer.position(topicPartition) - 1;

            // invalid key case, no messages in that partition present
            if (lastOffset != -1) {

                // returning to first offset
                consumer.seekToBeginning(Collections.singleton(topicPartition));

                // filter by offset or timestamp
                if (offset != null && !offset.trim().isEmpty()) {
                    exitCondition = filterByOffSet(consumer, topicPartition, Long.parseLong(offset), lastOffset);
                } else if (date != null && !date.trim().isEmpty()) {
                    try {
                        exitCondition = filterByTimestamp(consumer, topicPartition,
                                DateConversionUtility.FormattedDateToTimestamp(date, dateFormatter), lastOffset);
                    } catch (ParseException e) {
                        throw new RuntimeException(e.getMessage());
                    }
                }

                while (!exitCondition) {
                    // poll for data
                    ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(Long.parseLong(poll)));

                    for (ConsumerRecord<String, String> record : consumerRecords) {
                        if (key.equals(record.key())) {

                            JSONObject jsonObject = new JSONObject();
                            jsonObject.put("Topic", record.topic())
                                    .put("Partition", record.partition())
                                    .put("Offset", record.offset())
                                    .put("Key", record.key())
                                    .put("Value", record.value())
                                    .put("Timestamp", DateConversionUtility
                                            .timestampToFormattedDate(record.timestamp(), dateFormatter));

                            jsonObjectList.add(jsonObject);
                        }

                        if (lastOffset == record.offset())
                            exitCondition = true;
                    }
                }

                consumer.commitAsync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(lastOffset + 1)),
                        (map, e) -> {
                            if (e != null) {
                                System.out.println("ERROR: [" + map + "], MESSAGE: " + e.getMessage());
                            }
                        });
            }

            consumer.close();
        });
    }

    private boolean filterByOffSet(KafkaConsumer<String, String> consumer, TopicPartition topicPartition, Long offset,
                                   Long lastOffset) {
        boolean exitCondition = true;

        // offset in partition range
        if (offset <= lastOffset) {
            consumer.seek(topicPartition, offset);
            exitCondition = false;
        }

        return exitCondition;
    }

    private boolean filterByTimestamp(KafkaConsumer<String, String> consumer, TopicPartition topicPartition, long timestamp,
                                      long lastOffset) {
        boolean exitCondition = true;

        Map<TopicPartition, Long> topicPartitionWithTTimestamps = new HashMap<>();
        topicPartitionWithTTimestamps.put(topicPartition, timestamp);

        for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : consumer.offsetsForTimes(topicPartitionWithTTimestamps).entrySet()) {
            TopicPartition topicPartitionKey = entry.getKey();
            OffsetAndTimestamp offsetAndTimestamp = entry.getValue();

            if (offsetAndTimestamp != null) {
                long offset = offsetAndTimestamp.offset();

                // offset in partition range
                if (offset <= lastOffset) {
                    consumer.seek(topicPartitionKey, offsetAndTimestamp.offset());
                    exitCondition = false;
                }
            }
        }

        return exitCondition;
    }

    private int partition(String topic, String key, KafkaConsumer consumer) {
        if (key == null) {
            throw new IllegalArgumentException("ERROR : Key is NULL");
        }

        List partitions = consumer.partitionsFor(topic);
        int numPartitions = partitions.size();

        // hash the keyBytes to choose a partition
        return Utils.toPositive(Utils.murmur2(key.getBytes())) % numPartitions;
    }

    private String formattedJSON() {
        StringBuilder builder = new StringBuilder();
        jsonObjectList.forEach(json -> {
            builder.append(JsonWriter.formatJson(json.toString()));
            builder.append("\n");
        });

        return builder.toString();
    }
}
