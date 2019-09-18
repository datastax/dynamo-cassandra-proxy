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
package com.datastax.powertools.dcp.managed.dynamodb;

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

    public synchronized AmazonDynamoDB get() {
        if (ddb == null) {
            AwsClientBuilder.EndpointConfiguration endpointConfiguration = new AwsClientBuilder.EndpointConfiguration(dynamodbEndpoint, signinRegion);
            ddb = AmazonDynamoDBClientBuilder.standard().withEndpointConfiguration(endpointConfiguration).build();
        }

        return ddb;
    }
}
