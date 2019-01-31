package com.datastax.powertools.dcp;

/*
 *
 * @author Sebastián Estévez on 12/15/18.
 *
 */


import com.amazonaws.services.dynamodbv2.model.*;
import com.datastax.powertools.dcp.api.DynamoDBRequest;
import com.datastax.powertools.dcp.managed.dse.DatastaxManager;

public abstract class DynamoDSETranslator {
    private final DatastaxManager datastaxManager;
    protected String keyspaceName;

    public DynamoDSETranslator(DatastaxManager datastaxManager) {
        this.datastaxManager = datastaxManager;
        this.keyspaceName = datastaxManager.getKeyspaceName();
    }

    public abstract CreateTableResult createTable(DynamoDBRequest payload);
    public abstract PutItemResult putItem(DynamoDBRequest payload);
    public abstract QueryResult query(DynamoDBRequest payload);
    public abstract DeleteItemResult deleteItem(DeleteItemRequest dir);

    protected String getKeyspaceName() {
        return keyspaceName;
    }

}
