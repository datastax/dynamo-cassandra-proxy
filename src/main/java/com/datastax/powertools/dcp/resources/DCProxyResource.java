package com.datastax.powertools.dcp.resources;

import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.powertools.dcp.DynamoDSETranslator;
import com.datastax.powertools.dcp.api.DynamoDBRequest;
import com.datastax.powertools.dcp.managed.DatastaxManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.impl.LogKitLogger;
import org.glassfish.jersey.server.ManagedAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;

@Path("/")
@Produces(MediaType.APPLICATION_JSON)
public class DCProxyResource {

    private final DatastaxManager dseManager;
    private DynamoDSETranslator ddt;
    private Logger logger = LoggerFactory.getLogger(DCProxyResource.class);

    public DCProxyResource(DatastaxManager dsManager, DynamoDSETranslator ddt) {
        this.dseManager = dsManager;
        this.ddt = ddt;
    }

    @POST
    @ManagedAsync
    @Consumes("application/x-amz-json-1.0")
    public void asyncDynamoRequestHandler(@Suspended final AsyncResponse asyncResponse, @HeaderParam("X-Amz-Target") String target, String payload) {
        target = target.split("\\.")[1];

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        QueryResult response = new QueryResult();
        try {
            DynamoDBRequest dbr = objectMapper.readValue(payload, DynamoDBRequest.class);
            /*
            SEE : package com.amazonaws.services.dynamodbv2.local.server;
                classes.put("BatchGetItem", BatchGetItemRequest.class);
                classes.put("BatchWriteItem", BatchWriteItemRequest.class);
                classes.put("CreateTable", CreateTableRequest.class);
                classes.put("DeleteItem", DeleteItemRequest.class);
                classes.put("DeleteTable", DeleteTableRequest.class);
                classes.put("DescribeStream", DescribeStreamRequest.class);
                classes.put("DescribeTable", DescribeTableRequest.class);
                classes.put("DescribeLimits", DescribeLimitsRequest.class);
                classes.put("DescribeTimeToLive", DescribeTimeToLiveRequest.class);
                classes.put("GetItem", GetItemRequest.class);
                classes.put("GetRecords", GetRecordsRequest.class);
                classes.put("GetShardIterator", GetShardIteratorRequest.class);
                classes.put("ListStreams", ListStreamsRequest.class);
                classes.put("ListTables", ListTablesRequest.class);
                classes.put("ListTagsOfResource", ListTagsOfResourceRequest.class);
                classes.put("PutItem", PutItemRequest.class);
                classes.put("Query", QueryRequest.class);
                classes.put("Scan", ScanRequest.class);
                classes.put("TagResource", TagResourceRequest.class);
                classes.put("UntagResource", UntagResourceRequest.class);
                classes.put("UpdateItem", UpdateItemRequest.class);
                classes.put("UpdateTable", UpdateTableRequest.class);
                classes.put("UpdateTimeToLive", UpdateTimeToLiveRequest.class);
             */
            switch (target){
                case "CreateTable": ddt.createTable(dbr);
                break;
                case "PutItem": ddt.putItem(dbr);
                break;
                case "Query": response = ddt.query(dbr);
                break;
                default: {
                    logger.error("query type not supported");
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
            System.out.println(payload);
            try {
                String stringResponse = objectMapper.writeValueAsString(response);
                asyncResponse.resume(Response.ok().entity(response).build());
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            //asyncResponse.resume(Response.ok("true").build());
        }
    }
}