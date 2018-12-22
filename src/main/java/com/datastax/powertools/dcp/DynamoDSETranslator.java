package com.datastax.powertools.dcp;

/*
 *
 * @author Sebastián Estévez on 12/15/18.
 *
 */


import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.datastax.driver.dse.DseSession;
import com.datastax.powertools.dcp.api.DynamoDBRequest;
import com.datastax.powertools.dcp.managed.DatastaxManager;

public abstract class DynamoDSETranslator {
    private final DatastaxManager datastaxManager;
    protected String keyspaceName;

    public DynamoDSETranslator(DatastaxManager datastaxManager) {
        this.datastaxManager = datastaxManager;
        this.keyspaceName = datastaxManager.getKeyspaceName();
    }

    public abstract String createTable(DynamoDBRequest payload);
    public abstract String putItem(DynamoDBRequest payload);
    public abstract QueryResult query(DynamoDBRequest payload);

    protected String getKeyspaceName() {
        return keyspaceName;
    }
}
