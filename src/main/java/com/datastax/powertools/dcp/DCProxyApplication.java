package com.datastax.powertools.dcp;

import com.datastax.powertools.dcp.managed.ddbstreams.DynamoStreamsManager;
import com.datastax.powertools.dcp.managed.dse.DatastaxManager;
import com.datastax.powertools.dcp.managed.dynamodb.DynamoManager;
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

        TranslatorType translatorType = configuration.getTranslatorImplementation();
        DynamoDSETranslator ddt;
        if (translatorType == TranslatorType.JSON_BLOB){
             ddt = new DynamoDSETranslatorJSONBlob(dseManager);
        }else{
            // TODO: Implement other versions
            ddt = new DynamoDSETranslatorJSONBlob(dseManager);
        }
        final DCProxyResource dcProxyResource = new DCProxyResource(dseManager, ddt);
        environment.jersey().register(dcProxyResource);

        //Dynamo
        DynamoManager dynamoManager = new DynamoManager();
        dynamoManager.configure(configuration);
        environment.lifecycle().manage(dynamoManager);

        final DynamoDBResource ddbResource = new DynamoDBResource(dynamoManager);
        environment.jersey().register(ddbResource);


        //DynamoDBStreams
        if(configuration.isStreamsEnabled()){
            DynamoStreamsManager dynamoStreamsManager = new DynamoStreamsManager(dynamoManager.createOrGetDDB());
            dynamoStreamsManager.configure(configuration);
            environment.lifecycle().manage(dynamoStreamsManager);
        }

        /*
        Thread thread = new Thread(){
            @Override
            public void run() {
                super.run();
                dynamoStreamsManager.processStream();
            }
        };
        thread.setDaemon(true);
        thread.start();
        */

        //final DynamoStreamsResource streamsResource = new DynamoStreamsResource(dynamoStreamsManager);
        //environment.jersey().register(streamsResource);
    }

}
