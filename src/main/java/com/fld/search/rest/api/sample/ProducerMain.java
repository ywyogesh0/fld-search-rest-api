package com.fld.search.rest.api.sample;

import com.fld.search.rest.api.util.DateConversionUtility;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create a producer record

        String topic = "test-topic-100";

        int i = 1;
        while (i <= 100) {

            String key = String.valueOf(i);

            for (int j = 1; j <= 1000; j++) {
                String value = topic + "-" + key + "-" + j;

                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
                System.out.println("Key: " + key); // log the key

                // send data - asynchronous
                producer.send(record, (recordMetadata, e) -> {
                    // executes every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        // the record was successfully sent
                        System.out.println("Received new metadata. \n" +
                                "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + DateConversionUtility.timestampToFormattedDate(recordMetadata.timestamp(),
                                "yyyy-MM-dd HH:mm:ss.SSS"));
                    } else {
                        System.out.println("Error while producing");
                    }
                }).get();
            }

            i += 1;
        }

        // flush data
        producer.flush();
        // flush and close producer
        producer.close();
    }
}
