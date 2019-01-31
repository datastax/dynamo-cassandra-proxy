package com.datastax.powertools.dcp.resources;

import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.powertools.dcp.DynamoDSETranslator;
import com.datastax.powertools.dcp.api.DynamoDBRequest;
import com.datastax.powertools.dcp.managed.dse.DatastaxManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import org.glassfish.jersey.server.ManagedAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.*;
import java.io.IOException;

@Path("/")
@Produces(MediaType.APPLICATION_JSON)
public class DCProxyResource {

    private final DatastaxManager dseManager;
    private final ObjectMapper mapper;
    private DynamoDSETranslator ddt;
    private Logger logger = LoggerFactory.getLogger(DCProxyResource.class);

    public DCProxyResource(DatastaxManager dsManager, DynamoDSETranslator ddt) {
        this.dseManager = dsManager;
        this.ddt = ddt;
        mapper = new ObjectMapper().setPropertyNamingStrategy(PropertyNamingStrategy.UPPER_CAMEL_CASE);
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    @POST
    @ManagedAsync
    @Consumes("application/x-amz-json-1.0")
    @Produces("application/json")
    public void asyncDynamoRequestHandler(@Suspended final AsyncResponse asyncResponse, @Context HttpHeaders headers, @HeaderParam("X-Amz-Target") String target, String payload) {
        target = target.split("\\.")[1];

        //TODO: better type than Object?
        Object response = null;
        boolean success= true;
        try {
            DynamoDBRequest dbr = mapper.readValue(payload, DynamoDBRequest.class);

            switch (target){
                case "CreateTable": response = ddt.createTable(dbr);
                break;
                case "PutItem": response = ddt.putItem(dbr);
                break;
                case "DeleteItem": {
                    DeleteItemRequest dir = mapper.readValue(payload, DeleteItemRequest.class);
                    response = ddt.deleteItem(dir);
                }
                    break;
                case "Query": response = ddt.query(dbr);
                break;
                default: {
                    logger.error("query type not supported");
                    success = false;
                }
                break;
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (DriverException e) {
            e.printStackTrace();
        }
        finally {

            byte[] bytes = null;
            try {
                bytes = mapper.writeValueAsBytes(response);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }

            Response httpResponse;
            if (success) {
                Response.ResponseBuilder responseBuilder = Response.ok(bytes);
                httpResponse = responseBuilder.build();
            }else{
                Response.ResponseBuilder responseBuilder = Response.status(500);
                httpResponse = responseBuilder.build();
            }
            asyncResponse.resume(httpResponse);
        }
    }
}