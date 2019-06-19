package com.datastax.powertools.dcp;

/*
 *
 * @author Sebastián Estévez on 12/15/18.
 *
 */


import com.amazonaws.services.dynamodbv2.model.*;
import com.datastax.powertools.dcp.api.DynamoDBRequest;
import com.datastax.powertools.dcp.api.DynamoDBResponse;
import com.datastax.powertools.dcp.managed.dse.DatastaxManager;

public abstract class DynamoDSETranslator {
    private final DatastaxManager datastaxManager;
    protected String keyspaceName;

    public DynamoDSETranslator(DatastaxManager datastaxManager) {
        this.datastaxManager = datastaxManager;
        this.keyspaceName = datastaxManager.getKeyspaceName();
    }

    public abstract DynamoDBResponse createTable(DynamoDBRequest payload);
    public abstract DynamoDBResponse putItem(DynamoDBRequest payload);
    public abstract DynamoDBResponse getItem(GetItemRequest payload);
    public abstract DynamoDBResponse query(DynamoDBRequest payload);
    public abstract DynamoDBResponse deleteItem(DeleteItemRequest dir);
    public abstract DynamoDBResponse describeTable(DynamoDBRequest dtr);

    protected String getKeyspaceName() {
        return keyspaceName;
    }
}
