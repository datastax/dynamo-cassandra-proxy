package com.datastax.powertools.dcp;

import com.datastax.powertools.dcp.managed.DatastaxManager;
import com.datastax.powertools.dcp.managed.DynamoManager;
import com.datastax.powertools.dcp.resources.DCProxyResource;
import com.datastax.powertools.dcp.resources.DynamoDBResource;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class DCProxyApplication extends Application<DCProxyConfiguration> {

    public static void main(String[] args) throws Exception {
        new DCProxyApplication().run(args);
    }

    @Override
    public String getName() {
        return "DynamoDB-Cassandra-Proxy";
    }

    @Override
    public void initialize(Bootstrap<DCProxyConfiguration> bootstrap) {
        // nothing to do yet
    }

    @Override
    public void run(DCProxyConfiguration configuration,
                    Environment environment) {

        //DataStax
        DatastaxManager dseManager = new DatastaxManager();
        dseManager.configure(configuration);
        environment.lifecycle().manage(dseManager);

        final DCProxyResource dcProxyResource = new DCProxyResource(dseManager);
        environment.jersey().register(dcProxyResource);

        //Dynamo
        DynamoManager dynamoManager = new DynamoManager();
        dynamoManager.configure(configuration);
        environment.lifecycle().manage(dynamoManager);

        final DynamoDBResource ddbResource = new DynamoDBResource(dynamoManager);
        environment.jersey().register(ddbResource);
    }

}
