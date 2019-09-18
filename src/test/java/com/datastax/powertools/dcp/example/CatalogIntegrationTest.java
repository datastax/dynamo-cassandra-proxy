package com.datastax.powertools.dcp.example;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.datastax.powertools.dcp.AbstractDCPTest;

public class CatalogIntegrationTest extends AbstractDCPTest
{
    @Test
    public void testCatalog()
    {
        AmazonDynamoDB client = getProxyClient();

        List<AttributeDefinition> attributeDefinitions= new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition().withAttributeName("Id").withAttributeType("N"));

        List<KeySchemaElement> keySchema = new ArrayList<>();
        keySchema.add(new KeySchemaElement().withAttributeName("Id").withKeyType(KeyType.HASH));

        CreateTableRequest request = new CreateTableRequest()
                .withTableName("ProductCatalog")
                .withKeySchema(keySchema)
                .withAttributeDefinitions(attributeDefinitions)
                .withProvisionedThroughput(new ProvisionedThroughput()
                        .withReadCapacityUnits(5L)
                        .withWriteCapacityUnits(6L));

        CreateTableResult table = client.createTable(request);

        DynamoDBMapper mapper = new DynamoDBMapper(client, DynamoDBMapperConfig.builder().withSaveBehavior(DynamoDBMapperConfig.SaveBehavior.PUT).build());

        CatalogItem item = new CatalogItem();
        item.setId(102);
        item.setTitle("Book 102 Title");
        item.setISBN("222-2222222222");
        item.setBookAuthors(new HashSet<>(Arrays.asList("Author 1", "Author 2")));
        item.setSomeProp("Test");

        mapper.save(item);

        //Read Back
        CatalogItem partitionKey = new CatalogItem();

        partitionKey.setId(102);
        DynamoDBQueryExpression<CatalogItem> queryExpression = new DynamoDBQueryExpression<CatalogItem>().withHashKeyValues(partitionKey);

        List<CatalogItem> itemList = mapper.query(CatalogItem.class, queryExpression);

        Assert.assertTrue(itemList.size() == 1);
        CatalogItem r = itemList.get(0);
        Assert.assertEquals(item, r);

        //Get Item
        r = mapper.load(partitionKey);
        Assert.assertEquals(item, r);

        //Delete Item
        mapper.delete(partitionKey);
        r = mapper.load(partitionKey);
        Assert.assertTrue(null ==  r);


        DeleteTableResult deleteTable = client.deleteTable("ProductCatalog");
    }
}
