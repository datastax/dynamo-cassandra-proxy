package com.datastax.powertools.dcp.managed.dynamodb;

/*
 *
 * @author Sebastián Estévez on 12/6/18.
 *
 */


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
    private String accessKey;
    private String secretKey;

    public void configure(DCProxyConfiguration config) {
        this.dynamodbEndpoint = config.getDsDynamodbEndpoint();
        this.signinRegion = config.getDynamoRegion();
        this.accessKey = config.getDynamoAccessKey();
        this.secretKey = config.getDynamoSecretKey();

        Properties props = System.getProperties();
        props.setProperty("aws.accessKeyId", accessKey);
        props.setProperty("aws.secretKey", secretKey);
    }


    public void start() throws Exception {
    }

    public void stop() throws Exception {

    }

    public AmazonDynamoDB createOrGetDDB() {
        AwsClientBuilder.EndpointConfiguration endpointConfiguration = new AwsClientBuilder.EndpointConfiguration(dynamodbEndpoint, signinRegion);
        if (ddb != null){
            return ddb;
        }
        else {
            ddb = AmazonDynamoDBClientBuilder.standard().withEndpointConfiguration(endpointConfiguration).build();
            return ddb;
        }
    }
}
