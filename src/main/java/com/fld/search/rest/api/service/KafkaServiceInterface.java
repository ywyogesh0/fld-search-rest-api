package com.fld.search.rest.api.service;

import com.fld.search.rest.api.model.ConfigurationParams;

import java.io.IOException;

public interface KafkaServiceInterface {
    String findMessagesByTopicAndKey(String topic, String key, ConfigurationParams configurationParams);

    String findMessagesByKey(String key, ConfigurationParams configurationParams);

    String findMessagesByTopicWithKeyAndOffset(String topic, String key, String offset, ConfigurationParams configurationParams);

    String findMessagesByKeyAndOffset(String key, String offset, ConfigurationParams configurationParams);

    String findMessagesByTopicWithKeyAndTimestamp(String topic, String key, String timestamp, ConfigurationParams configurationParams);

    String findMessagesByKeyAndTimestamp(String key, String timestamp, ConfigurationParams configurationParams);

    String findMessages(ConfigurationParams configurationParams) throws IOException;
}
