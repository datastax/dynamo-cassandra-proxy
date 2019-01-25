package com.datastax.powertools.dcp;

/*
 *
 * @author Sebastián Estévez on 12/15/18.
 *
 */


import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.datastax.powertools.dcp.api.DynamoDBRequest;
import com.datastax.powertools.dcp.managed.DatastaxManager;

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

    protected String getKeyspaceName() {
        return keyspaceName;
    }
}
