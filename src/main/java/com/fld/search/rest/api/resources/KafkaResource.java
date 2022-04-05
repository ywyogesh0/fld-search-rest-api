package com.fld.search.rest.api.resources;

import com.fld.search.rest.api.model.ConfigurationParams;
import com.fld.search.rest.api.service.KafkaService;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.IOException;

@Path("/search/kafka")
public class KafkaResource {

    private KafkaService kafkaService = new KafkaService();

    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/topic/{topicName}/key/{keyName}")
    public String findMessagesByTopicAndKey(@PathParam("topicName") String topicName,
                                            @PathParam("keyName") String keyName, ConfigurationParams configurationParams) {
        return kafkaService.findMessagesByTopicAndKey(topicName, keyName, configurationParams);
    }

    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/key/{keyName}")
    public String findMessagesByKey(@PathParam("keyName") String keyName, ConfigurationParams configurationParams) {
        return kafkaService.findMessagesByKey(keyName, configurationParams);
    }

    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/topic/{topicName}/key/{keyName}/offset/{offset}")
    public String findMessagesByTopicWithKeyAndOffset(@PathParam("topicName") String topicName,
                                                      @PathParam("keyName") String keyName, @PathParam("offset") String offset,
                                                      ConfigurationParams configurationParams) {
        return kafkaService.findMessagesByTopicWithKeyAndOffset(topicName, keyName, offset, configurationParams);
    }

    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/key/{keyName}/offset/{offset}")
    public String findMessagesByKeyAndOffset(@PathParam("keyName") String keyName, @PathParam("offset") String offset,
                                             ConfigurationParams configurationParams) {
        return kafkaService.findMessagesByKeyAndOffset(keyName, offset, configurationParams);
    }

    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/topic/{topicName}/key/{keyName}/timestamp/{timestamp}")
    public String findMessagesByTopicWithKeyAndTimestamp(@PathParam("topicName") String topicName,
                                                         @PathParam("keyName") String keyName, @PathParam("timestamp") String timestamp,
                                                         ConfigurationParams configurationParams) {
        return kafkaService.findMessagesByTopicWithKeyAndTimestamp(topicName, keyName, timestamp, configurationParams);
    }

    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/key/{keyName}/timestamp/{timestamp}")
    public String findMessagesByKeyAndTimestamp(@PathParam("keyName") String keyName, @PathParam("timestamp") String timestamp,
                                                ConfigurationParams configurationParams) {
        return kafkaService.findMessagesByKeyAndTimestamp(keyName, timestamp, configurationParams);
    }

    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public String findMessages(ConfigurationParams configurationParams) throws IOException {
        return kafkaService.findMessages(configurationParams);
    }
}
