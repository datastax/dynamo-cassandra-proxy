package com.datastax.powertools.dcp;

import java.util.Date;

import org.junit.Test;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import org.testng.Assert;

public class CRUDTest extends AbstractDCPTest
{
    @Test
    public void testCreate() {
        AmazonDynamoDB proxyClient = getProxyClient();
        AmazonDynamoDB awsClient = getAwsClient();

        CreateTableRequest req = new CreateTableRequest()
                .withTableName("foo")
                .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(100L).withWriteCapacityUnits(100L))
                .withKeySchema(new KeySchemaElement("Name", KeyType.HASH))
                .withAttributeDefinitions(new AttributeDefinition("Name", ScalarAttributeType.S));

        proxyClient.createTable(req);
        awsClient.createTable(req);

        DescribeTableResult r = proxyClient.describeTable("foo");
        DescribeTableResult r2 = proxyClient.describeTable("foo");

        Date now = new Date();
        r.getTable().withCreationDateTime(now);
        r2.getTable().withCreationDateTime(now);

        Assert.assertEquals(r, r2);
    }
}
