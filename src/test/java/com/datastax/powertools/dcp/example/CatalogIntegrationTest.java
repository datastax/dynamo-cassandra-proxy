package com.datastax.powertools.dcp.example;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DeleteTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.datastax.powertools.dcp.AbstractDCPTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class CatalogIntegrationTest extends AbstractDCPTest
{
    @Test
    public void testCatalog()
    {
        AmazonDynamoDB client = getProxyClient();

        DynamoDB dynamoDB = new DynamoDB(client);

        List<AttributeDefinition> attributeDefinitions= new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition().withAttributeName("Id").withAttributeType("N"));
        attributeDefinitions.add(new AttributeDefinition().withAttributeName("Title").withAttributeType("S"));

        List<KeySchemaElement> keySchema = new ArrayList<>();
        keySchema.add(new KeySchemaElement()
                .withAttributeName("Id").withKeyType(KeyType.HASH));

        keySchema.add(new KeySchemaElement()
                .withAttributeName("Title").withKeyType(KeyType.RANGE));

        String tableName= "ProductCatalog";

        CreateTableRequest request = new CreateTableRequest()
                .withTableName(tableName)
                .withKeySchema(keySchema)
                .withAttributeDefinitions(attributeDefinitions)
                .withProvisionedThroughput(new ProvisionedThroughput()
                        .withReadCapacityUnits(5L)
                        .withWriteCapacityUnits(6L));

        CreateTableResult createTable = client.createTable(request);

        DynamoDBMapper mapper = new DynamoDBMapper(client, DynamoDBMapperConfig.builder().withSaveBehavior(DynamoDBMapperConfig.SaveBehavior.PUT).build());

        CatalogItem item = new CatalogItem();
        item.setId(102);
        item.setTitle("Book 102 Title");
        item.setISBN("222-2222222222");
        item.setBookAuthors(new HashSet<>(Arrays.asList("Author 1", "Author 2")));
        item.setSomeProp("Test");
        item.setBooksInStock(10);

        mapper.save(item);

        //Read Back
        CatalogItem getItem = new CatalogItem();

        getItem.setId(102);
        getItem.setTitle("Book 102 Title");


        CatalogItem partitionKey = new CatalogItem();
        partitionKey.setId(102);
        Map<String, Condition> rangeKeyCondititions = new HashMap<>();

        //NOTE: The current DynamoDB service only allows up to one range key condition per query. Providing more than one range key condition will result in a SdkClientException.
        //TODO: test Between ComparisonOperator
        Collection<AttributeValue> attributeValueList = Arrays.asList(new AttributeValue("B"));
        Condition rangeCondition = new Condition().withAttributeValueList(attributeValueList)
                .withComparisonOperator(ComparisonOperator.GE);

        rangeKeyCondititions.put("Title", rangeCondition);

        DynamoDBQueryExpression<CatalogItem> queryExpression = new DynamoDBQueryExpression<CatalogItem>()
                .withHashKeyValues(partitionKey)
                .withRangeKeyConditions(rangeKeyCondititions);

        List<CatalogItem> itemList = mapper.query(CatalogItem.class, queryExpression);

        Assert.assertTrue(itemList.size() == 1);
        CatalogItem r = itemList.get(0);
        Assert.assertEquals(item, r);

        //Complex Query
        String keyCoditionExpression = "Id = :v_id and Title > :v_title";
        String filterExpression = "ISBN = :v_isbn and booksInStock :v_books_in_stock";
        //String projectionExpression = "";
        //Integer limit = 1;
        //String select = "";
        //boolean consistentRead = false;

        Map<String, AttributeValue> expressionAttibuteValues = new HashMap<>();

        expressionAttibuteValues.put("Id", new AttributeValue().withN("102"));
        expressionAttibuteValues.put("Title", new AttributeValue().withS("B"));
        expressionAttibuteValues.put("ISBN", new AttributeValue().withS("222-2222222222"));
        expressionAttibuteValues.put("booksInStock", new AttributeValue().withN("2"));

        queryExpression = new DynamoDBQueryExpression<CatalogItem>()
                .withKeyConditionExpression(keyCoditionExpression)
                .withFilterExpression(filterExpression)
                .withExpressionAttributeValues(expressionAttibuteValues);
         //       .withProjectionExpression(projectionExpression)
         //       .withLimit(limit)
         //       .withSelect(select)
         //       .withConsistentRead(consistentRead);

        // consider implementing ExclusiveStartKey and ExpressionAttributeNames and Values

        itemList = mapper.query(CatalogItem.class, queryExpression);

        Assert.assertTrue(itemList.size() == 1);
        r = itemList.get(0);
        Assert.assertEquals(item, r);


        //Get Item
        r = mapper.load(getItem);
        Assert.assertEquals(item, r);

        //Delete Item
        mapper.delete(getItem);
        r = mapper.load(getItem);
        Assert.assertTrue(null ==  r);


        DeleteTableResult deleteTable = client.deleteTable(tableName);
    }
}
