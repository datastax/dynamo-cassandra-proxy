package com.datastax.powertools.dcp.managed;

/*
 *
 * @author Sebastián Estévez on 12/6/18.
 *
 */


import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.datastax.powertools.dcp.DCProxyConfiguration;
import io.dropwizard.lifecycle.Managed;

import java.util.Properties;

public class DynamoManager implements Managed {

    private AmazonDynamoDB ddb ;
    private String dynamodbEndpoint;
    private String signinRegion;

    public void configure(DCProxyConfiguration config) {
        this.dynamodbEndpoint = config.getDynamodbEndpoint();
        this.signinRegion = config.getDynamoRegion();
    }


    public void start() throws Exception {
    }

    public void stop() throws Exception {

    }

    public AmazonDynamoDB createAndGetDdb() {
        Properties props = System.getProperties();
        props.setProperty("aws.accessKeyId", "fakeID");
        props.setProperty("aws.secretKey", "fakeKey");


        AwsClientBuilder.EndpointConfiguration endpointConfiguration = new AwsClientBuilder.EndpointConfiguration(dynamodbEndpoint, signinRegion);
        ddb = AmazonDynamoDBClientBuilder.standard().withEndpointConfiguration(endpointConfiguration).build();
        return ddb;
    }
}
