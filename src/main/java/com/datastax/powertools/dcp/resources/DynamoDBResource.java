package com.datastax.powertools.dcp.resources;

/*
 *
 * @author Sebastián Estévez on 12/6/18.
 *
 */


import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import com.datastax.powertools.dcp.managed.dynamodb.DynamoManager;
import org.glassfish.jersey.server.ManagedAsync;

import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.*;

@Path("/ddb")
@Produces(MediaType.APPLICATION_JSON)
public class DynamoDBResource {

    private final AmazonDynamoDB ddb;

    public DynamoDBResource(DynamoManager ddbManager) {
        this.ddb = ddbManager.createOrGetDDB();
    }

    @ManagedAsync
    @POST
    @Path("/createTable")
    public void asyncCreateTable(@Suspended final AsyncResponse asyncResponse,
                             @DefaultValue("test") @QueryParam(value="table_name") String table_name,
                             @DefaultValue("hash_key") @QueryParam(value="hash_key") String hashKey,
                             @DefaultValue("sort_key") @QueryParam(value="sort_key") String sortKey,
                             @DefaultValue("sort_type_string") @QueryParam(value="N") String sortAttributeTypeString,
                             @DefaultValue("hash_type_string") @QueryParam(value="S") String hashAttributeTypeString
    ) {
        ScalarAttributeType hashAttributeType = getAttributeTypeFromString(hashAttributeTypeString);
        ScalarAttributeType sortAttributeType = getAttributeTypeFromString(sortAttributeTypeString);

        CreateTableRequest request = new CreateTableRequest()
                .withAttributeDefinitions(new AttributeDefinition(
                        hashKey, hashAttributeType), new AttributeDefinition(
                                sortKey, sortAttributeType
                ))
                .withKeySchema(
                        new KeySchemaElement(hashKey, KeyType.HASH),
                        new KeySchemaElement(sortKey, KeyType.RANGE))
                .withProvisionedThroughput(new ProvisionedThroughput(
                        new Long(10), new Long(10)))
                .withTableName(table_name);
        CreateTableResult response = ddb.createTable(request);

        asyncResponse.resume(Response.ok(response).build());
    }

    private ScalarAttributeType getAttributeTypeFromString(String attributeTypeString) {
        ScalarAttributeType attributeType;
        if (attributeTypeString.equals("S")){
            attributeType = ScalarAttributeType.S;
        }else if (attributeTypeString.equals("B")){
            attributeType = ScalarAttributeType.B;
        }else {
            attributeType = ScalarAttributeType.N;
        }
        return attributeType;
    }

    @ManagedAsync
    @POST
    @Path("/putItem")
    public void putItem(@Suspended final AsyncResponse asyncResponse,
                                 @DefaultValue("test") @QueryParam(value="table_name") String table_name,
                                 @DefaultValue("hash_key") @QueryParam(value="hash_key") String hashKey,
                                 @DefaultValue("1.0") @QueryParam(value="hash_value") String hashValue,
                                 @DefaultValue("sort_key") @QueryParam(value="sort_key") String sortKey,
                                 @DefaultValue("2.0") @QueryParam(value="sort_value") String sortValue
    ) {

        List<String> favorites = Arrays.asList("puppies", "kittens", "other cute animals");
        Map<String, AttributeValue> item = new HashMap<>();
        AttributeValue attributeValue = new AttributeValue(favorites);
        item.put(hashKey, new AttributeValue(hashValue));
        item.put(sortKey, new AttributeValue(sortValue));
        item.put("favorites", attributeValue);
        item.put("city", new AttributeValue("NYC"));
        PutItemRequest putItemRequest = new PutItemRequest(table_name, item);

        PutItemResult response = ddb.putItem(putItemRequest);

        asyncResponse.resume(Response.ok().entity(response).build());
    }

    @ManagedAsync
    @POST
    @Path("/query")
    public void query(@Suspended final AsyncResponse asyncResponse,
                                 @DefaultValue("test") @QueryParam(value="table_name") String table_name,
                                 @DefaultValue("hash_key") @QueryParam(value="hash_key") String hashKey,
                                 @DefaultValue("1.0") @QueryParam(value="hash_key_value") String hashKeyValue
    ) {

        String alias = ":v_id";
        AttributeValue attributeValue = new AttributeValue().withN(hashKeyValue);
        Map<String, AttributeValue> attributeValues = new HashMap<>();
        attributeValues.put(alias, attributeValue);

        Condition hasKeyCondition = new Condition().
                withComparisonOperator(ComparisonOperator.EQ).
                withAttributeValueList(attributeValue);

        HashMap<String, Condition> keyConditions = new HashMap<String, Condition>();

        keyConditions.put(alias , hasKeyCondition);

        QueryRequest queryReq = new QueryRequest()
                .withTableName(table_name)
                .withKeyConditionExpression(hashKey + "= "+alias)
                .withExpressionAttributeValues(attributeValues);

       // TODO add new method with key condition
       //         .withKeyConditions(keyConditions);

        QueryResult response = ddb.query(queryReq);

        asyncResponse.resume(Response.ok().entity(response).build());
    }
}
