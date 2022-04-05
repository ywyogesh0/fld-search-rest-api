package com.fld.search.rest.api.service;

import com.fld.search.rest.api.model.ConfigurationParams;

public interface CassandraServiceInterface {
    String findMessages(ConfigurationParams configurationParams);
}
