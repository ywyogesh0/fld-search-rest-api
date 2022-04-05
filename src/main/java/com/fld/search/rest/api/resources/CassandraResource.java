package com.fld.search.rest.api.resources;

import com.fld.search.rest.api.model.ConfigurationParams;
import com.fld.search.rest.api.service.CassandraService;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/search/cassandra")
public class CassandraResource {

    private CassandraService cassandraService = new CassandraService();

    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public String findMessages(ConfigurationParams configurationParams) {
        return cassandraService.findMessages(configurationParams);
    }
}
