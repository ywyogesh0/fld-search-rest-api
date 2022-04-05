package com.fld.search.rest.api.model;

import javax.json.bind.annotation.JsonbProperty;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ConfigurationParams {
    @JsonbProperty("kafka.bootstrap.servers")
    private String bootstrapServers;

    @JsonbProperty("kafka.topics")
    private String topics;

    @JsonbProperty("kafka.csv.path")
    private String csvPath;

    @JsonbProperty("kafka.partition.date.formatter")
    private String dateFormatter;

    @JsonbProperty("kafka.group.id")
    private String groupId;

    @JsonbProperty("kafka.poll.duration.ms")
    private String pollDuration;

    @JsonbProperty("cassandra.host")
    private String host;

    @JsonbProperty("cassandra.keyspace")
    private String keyspace;

    @JsonbProperty("cassandra.partition.column.name")
    private String partitionColumnName;

    @JsonbProperty("cassandra.partition.column.values")
    private String partitionColumnValues;

    public ConfigurationParams() {
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getTopics() {
        return topics;
    }

    public void setTopics(String topics) {
        this.topics = topics;
    }

    public String getCsvPath() {
        return csvPath;
    }

    public void setCsvPath(String csvPath) {
        this.csvPath = csvPath;
    }

    public String getDateFormatter() {
        return dateFormatter;
    }

    public void setDateFormatter(String dateFormatter) {
        this.dateFormatter = dateFormatter;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getPollDuration() {
        return pollDuration;
    }

    public void setPollDuration(String pollDuration) {
        this.pollDuration = pollDuration;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public String getPartitionColumnName() {
        return partitionColumnName;
    }

    public void setPartitionColumnName(String partitionColumnName) {
        this.partitionColumnName = partitionColumnName;
    }

    public String getPartitionColumnValues() {
        return partitionColumnValues;
    }

    public void setPartitionColumnValues(String partitionColumnValues) {
        this.partitionColumnValues = partitionColumnValues;
    }

    @Override
    public String toString() {
        return "ConfigurationParams{" +
                "bootstrapServers='" + bootstrapServers + '\'' +
                ", topics='" + topics + '\'' +
                ", csvPath='" + csvPath + '\'' +
                ", dateFormatter='" + dateFormatter + '\'' +
                ", groupId='" + groupId + '\'' +
                ", pollDuration='" + pollDuration + '\'' +
                ", host='" + host + '\'' +
                ", keyspace='" + keyspace + '\'' +
                ", partitionColumnName='" + partitionColumnName + '\'' +
                ", partitionColumnValues='" + partitionColumnValues + '\'' +
                '}';
    }
}
