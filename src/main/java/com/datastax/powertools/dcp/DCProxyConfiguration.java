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
package com.datastax.powertools.dcp;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;

public class DCProxyConfiguration extends Configuration {

    //Cassandra Stuff
    @JsonProperty
    private int cqlPort = 9042;
    @JsonProperty
    private String contactPoints = "localhost";
    @JsonProperty
    private String localDC = "dc1";
    @JsonProperty
    private String cqlUserName = "cassandra";
    @JsonProperty
    private String cqlPassword = "cassandra";
    @JsonProperty
    private String keyspaceName = "dynamoks";
    @JsonProperty
    private String replicationStrategy = "{'class': 'SimpleStrategy', 'replication_factor': 1 }";

    //Dynamo Stuff
    @JsonProperty
    private String dynamoRegion = "east-nyc-madeup";
    private String dsDynamodbEndpoint = "http://localhost:8080";
    @JsonProperty
    private String awsDynamodbEndpoint = "http://dynamodb.us-east-2.amazonaws.com";
    @JsonProperty
    private String streamsEndpoint =  "https://streams.dynamodb.us-east-2.amazonaws.com";;

    @JsonProperty
    private TranslatorType translatorImplementation = TranslatorType.JSON_BLOB;
    @JsonProperty
    private String dynamoAccessKey= "fake-key";
    @JsonProperty
    private String dynamoSecretKey= "fake-secret";
    @JsonProperty
    private boolean streamsEnabled;
    @JsonProperty
    private boolean dockerCassandra;

    @JsonProperty
    public void setContactPoints(String contactPoints) {
        this.contactPoints = contactPoints;
    }

    @JsonProperty
    public String getContactPoints() {
        return contactPoints;
    }

    @JsonProperty
    public int getCqlPort() {
        return cqlPort;
    }

    @JsonProperty
    public void setCqlPort(int cqlPort) {
        this.cqlPort = cqlPort;
    }

    @JsonProperty
    public String getCqlUserName() {
        return cqlUserName;
    }

    @JsonProperty
    public void setCqlUserName(String cqlUserName) {
        this.cqlUserName = cqlUserName;
    }

    @JsonProperty
    public String getCqlPassword() {
        return cqlPassword;
    }

    @JsonProperty
    public void setCqlPassword(String cqlPassword) {
        this.cqlPassword = cqlPassword;
    }

    @JsonProperty
    public String getKeyspaceName() {
        return keyspaceName;
    }

    @JsonProperty
    public void setKeyspaceName(String keyspaceName) {
        this.keyspaceName = keyspaceName;
    }

    @JsonProperty
    public String getReplicationStrategy() {
        return replicationStrategy;
    }

    @JsonProperty
    public void setReplicationStrategy(String replicationStrategy) {
        this.replicationStrategy = replicationStrategy;
    }

    @JsonProperty
    public String getDynamoRegion() {
        return dynamoRegion;
    }

    @JsonProperty
    public void setDynamoRegion(String dynamoRegion) {
        this.dynamoRegion = dynamoRegion;
    }

    @JsonProperty
    public String getDsDynamodbEndpoint() {
        return dsDynamodbEndpoint;
    }

    @JsonProperty
    public void setDsDynamodbEndpoint(String dsDynamodbEndpoint) {
        this.dsDynamodbEndpoint = dsDynamodbEndpoint;
    }

    public TranslatorType getTranslatorImplementation() {
        return this.translatorImplementation;
    }

    @JsonProperty
    public String getDynamoAccessKey() {
        return dynamoAccessKey;
    }

    @JsonProperty
    public String getDynamoSecretKey() {
        return dynamoSecretKey;
    }

    @JsonProperty
    public String getAwsDynamodbEndpoint() {
        return awsDynamodbEndpoint;
    }

    @JsonProperty
    public void setAwsDynamodbEndpoint(String awsDynamodbEndpoint) {
        this.awsDynamodbEndpoint = awsDynamodbEndpoint;
    }

    public String getStreamsEndpoint() {
        return streamsEndpoint;
    }

    public void setStreamsEndpoint(String streamsEndpoint) {
        this.streamsEndpoint = streamsEndpoint;
    }

    public boolean isStreamsEnabled() {
        return streamsEnabled;
    }

    public boolean isDockerCassandra() {
        return dockerCassandra;
    }

    public String getLocalDC() {
        return localDC;
    }
}
