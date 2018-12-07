package com.datastax.powertools.dcp;

/*
 *
 * @author Sebastián Estévez on 12/6/18.
 *
 */


import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;

public class DCProxyConfiguration extends Configuration {

    //DSE Stuff
    @JsonProperty
    private int cqlPort = 9042;
    @JsonProperty
    private String[] contactPoints = {"localhost"};
    @JsonProperty
    private String cqlUserName = "cassandra";
    @JsonProperty
    private String cqlPassword = "cassandra";
    @JsonProperty
    private String keyspaceName = "dynamoKS";
    @JsonProperty
    private String replicationStrategy = "{'class': 'SimpleStrategy', 'replication_factor': 1 }";

    //Dynamo Stuff
    @JsonProperty
    private String dynamoRegion = "east-nyc-madeup";
    @JsonProperty
    private String dynamodbEndpoint = "http://localhost:8080";

    @JsonProperty
    public void setContactPoints(String[] contactPoints) {
        this.contactPoints = contactPoints;
    }

    @JsonProperty
    public String[] getContactPoints() {
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
    public String getDynamodbEndpoint() {
        return dynamodbEndpoint;
    }

    @JsonProperty
    public void setDynamodbEndpoint(String dynamodbEndpoint) {
        this.dynamodbEndpoint = dynamodbEndpoint;
    }
}
