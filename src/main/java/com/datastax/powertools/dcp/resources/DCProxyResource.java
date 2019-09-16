/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.powertools.dcp.resources;

import com.amazonaws.AmazonWebServiceResult;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.powertools.dcp.DynamoDSETranslator;
import com.datastax.powertools.dcp.api.DynamoDBRequest;
import com.datastax.powertools.dcp.api.DynamoDBResponse;
import com.datastax.powertools.dcp.api.DynamoStatementType;
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
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
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

        DynamoStatementType statementType = DynamoStatementType.valueOf(target);

        //TODO: better type than Object?
        //AmazonWebServiceResult response = null;
        DynamoDBResponse response = null;
        try {
            DynamoDBRequest dbr = mapper.readValue(payload, DynamoDBRequest.class);

            switch (statementType){
                case CreateTable: response = ddt.createTable(dbr);
                break;
                case DeleteTable : response = ddt.deleteTable(dbr);
                break;
                case DescribeTable: response = ddt.describeTable(dbr);
                break;
                case PutItem: response = ddt.putItem(dbr);
                break;
                case GetItem: {
                    GetItemRequest gir = mapper.readValue(payload, GetItemRequest.class);
                    response = ddt.getItem(gir);
                }
                break;
                case DeleteItem: {
                    DeleteItemRequest dir = mapper.readValue(payload, DeleteItemRequest.class);
                    response = ddt.deleteItem(dir);
                }
                    break;
                case Query: response = ddt.query(dbr);
                break;
                default: {
                    logger.error("query type not supported");
                    response = new DynamoDBResponse(new AmazonWebServiceResult(), 400);
                    response.setError("query type not supported");
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
                bytes = mapper.writeValueAsBytes(response.getResult());
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            if (response.getStatusCode() == 200) {
                Response httpResponse;
                Response.ResponseBuilder responseBuilder = Response.ok(bytes).status(response.getStatusCode());
                httpResponse = responseBuilder.build();
                asyncResponse.resume(httpResponse);
            }else{
                throw new WebApplicationException(response.getError(), response.getStatusCode());
            }
        }
    }
}