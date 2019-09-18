package com.datastax.powertools.dcp;

import org.junit.ClassRule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.datastax.powertools.dcp.managed.dynamodb.DynamoManager;
import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.junit.DropwizardAppRule;

public class AbstractDCPTest
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractDCPTest.class);
    private static AmazonDynamoDB awsClient = DynamoDBEmbedded.create().amazonDynamoDB();
    private static DynamoManager proxyClient;

    @ClassRule
    public static final DropwizardAppRule<DCProxyConfiguration> RULE = new DropwizardAppRule<>(DCProxyApplication.class, "",
            ConfigOverride.config("dockerCassandra", "true"));

    protected synchronized AmazonDynamoDB getProxyClient()
    {
        if (proxyClient == null)
        {
            proxyClient = new DynamoManager();
            proxyClient.configure(RULE.getConfiguration());
        }

        return proxyClient.get();
    }

    protected AmazonDynamoDB getAwsClient()
    {
        return awsClient;
    }
}
