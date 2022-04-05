package com.fld.search.rest.api.service;

import com.cedarsoftware.util.io.JsonWriter;
import com.datastax.driver.core.*;
import com.fld.search.rest.api.model.ConfigurationParams;
import org.json.JSONObject;

import java.util.*;

public class CassandraService implements CassandraServiceInterface {

    private Cluster cluster;
    private Session session;
    private String partitionColumnValues;
    private String cassandraKeyspace;
    private String cassandraHost;
    private String partitionColumnName;
    private Set<String> tableNamesSet = new HashSet<>();
    private List<JSONObject> tableStringJSONList = new ArrayList<>();
    private Set<String> partitionColumnValuesSet = new HashSet<>();

    @Override
    public String findMessages(ConfigurationParams configurationParams) {
        this.partitionColumnValues = configurationParams.getPartitionColumnValues();
        this.cassandraKeyspace = configurationParams.getKeyspace();
        this.cassandraHost = configurationParams.getHost();
        this.partitionColumnName = configurationParams.getPartitionColumnName();

        return fetchTableString();
    }

    private String fetchTableString() {
        try {
            System.out.println("CassandraMessagesDump started...");

            connectCassandraCluster();
            filterTableNames();
            populatePartitionColumnValuesSet();
            prepareTablesDumpData();

            System.out.println("CassandraMessagesDump finished successfully...");
        } catch (Exception e) {
            System.out.println("CassandraMessagesDump - ERROR: " + e.getMessage());
        } finally {
            if (session != null) {
                session.close();
            }

            if (cluster != null) {
                cluster.close();
            }
        }

        return formattedJSON();
    }

    private void connectCassandraCluster() {
        System.out.println("Connecting Cassandra Cluster on " + cassandraHost + "...");
        cluster = Cluster.builder().addContactPoint(cassandraHost).build();
        session = cluster.connect();
        System.out.println("Cassandra Cluster Connected Successfully...");
    }

    private void filterTableNames() {
        Metadata metadata = cluster.getMetadata();

        Collection<TableMetadata> tablesMetadata = metadata.getKeyspace(cassandraKeyspace).getTables();
        for (TableMetadata tm : tablesMetadata) {

            String tableName = tm.getName();
            Collection<ColumnMetadata> columnsMetadata = tm.getColumns();

            for (ColumnMetadata cm : columnsMetadata) {
                String columnName = cm.getName().toLowerCase();

                if (columnName.equals(partitionColumnName)) {
                    tableNamesSet.add(tableName);
                }
            }
        }
    }

    private void populatePartitionColumnValuesSet() {
        String[] partitionColumnValueArray = partitionColumnValues.split(",");
        if (partitionColumnValueArray.length > 1) {
            Collections.addAll(partitionColumnValuesSet, partitionColumnValueArray);
        } else {
            partitionColumnValuesSet.add(partitionColumnValues);
        }
    }

    private void prepareTablesDumpData() {
        for (String tableName : tableNamesSet) {

            JSONObject tableJSON = new JSONObject();
            List<JSONObject> parentRowJSONList = new ArrayList<>();

            for (String partitionColumnValue : partitionColumnValuesSet) {

                String cqlQuery = "select * from " + cassandraKeyspace + "." + tableName + " where " + partitionColumnName + "=" +
                        Integer.parseInt(partitionColumnValue);

                ResultSet resultSet = session.execute(cqlQuery);
                List<Row> rowList = resultSet.all();

                int rowCount = rowList.size();
                if (rowCount > 0) {
                    ColumnDefinitions columnDefinitions = resultSet.getColumnDefinitions();
                    int columnsCount = columnDefinitions.size();

                    for (Row row : rowList) {
                        JSONObject rowJSON = new JSONObject();
                        for (int j = 0; j < columnsCount; j++) {
                            rowJSON.put(columnDefinitions.getName(j), row.getObject(j));
                        }

                        parentRowJSONList.add(rowJSON);
                    }
                }
            }

            tableJSON.put(tableName, parentRowJSONList);
            tableStringJSONList.add(tableJSON);
        }
    }

    private String formattedJSON() {
        StringBuilder builder = new StringBuilder();
        tableStringJSONList.forEach(json -> {
            builder.append(JsonWriter.formatJson(json.toString()));
            builder.append("\n");
        });

        return builder.toString();
    }
}
