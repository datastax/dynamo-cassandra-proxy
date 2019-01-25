package com.datastax.powertools.dcp.managed;

/*
 *
 * @author Sebastián Estévez on 12/26/18.
 *
 */


import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.internal.CredentialsEndpointProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.*;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.s3.model.Region;
import com.datastax.powertools.dcp.DCProxyConfiguration;
import io.dropwizard.lifecycle.Managed;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class DynamoStreamsManager implements Managed {
    private final AmazonDynamoDB ddbProxy;
    private String dynamodbEndpoint;
    private String signinRegion;
    private String accessKey;
    private String secretKey;
    private AmazonDynamoDBStreams streamsClient;
    private AmazonDynamoDB realDDB;
    private String streamArn;
    private StreamSpecification streamSpec;
    //TODO: support multiple streams from multiple tables
    private String lastEvaluatedShardId;

    public DynamoStreamsManager(AmazonDynamoDB ddb) {
        ddbProxy = ddb;
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {
    }

    public void configure(DCProxyConfiguration config) {
        this.dynamodbEndpoint = config.getAwsDynamodbEndpoint();
        this.signinRegion = config.getDynamoRegion();
        this.accessKey = config.getDynamoAccessKey();
        this.secretKey = config.getDynamoSecretKey();

        Properties props = System.getProperties();
        props.setProperty("aws.accessKeyId", accessKey);
        props.setProperty("aws.secretKey", secretKey);

        AwsClientBuilder.EndpointConfiguration endpointConfiguration =
                new AwsClientBuilder.EndpointConfiguration(dynamodbEndpoint, signinRegion);
        SystemPropertiesCredentialsProvider spcp = new SystemPropertiesCredentialsProvider();

        realDDB = AmazonDynamoDBClientBuilder.standard().
                withRegion(Regions.US_EAST_2).
                //withEndpointConfiguration(endpointConfiguration).
                withCredentials(spcp).build();

        streamArn = realDDB.describeTable("test").getTable().getLatestStreamArn();
        streamSpec = realDDB.describeTable("test").getTable().getStreamSpecification();
        streamsClient = AmazonDynamoDBStreamsClientBuilder.standard().withEndpointConfiguration(endpointConfiguration).build();
    }

    public void processStream(){

        lastEvaluatedShardId = null;

        do {
            DescribeStreamResult describeStreamResult;
            if (lastEvaluatedShardId == null){

                describeStreamResult = streamsClient.describeStream(
                        new DescribeStreamRequest()
                                .withStreamArn(streamArn));
            }else {
                describeStreamResult = streamsClient.describeStream(
                        new DescribeStreamRequest()
                                .withStreamArn(streamArn)
                                .withExclusiveStartShardId(lastEvaluatedShardId));
            }
            List<Shard> shards = describeStreamResult.getStreamDescription().getShards();

            // Process each shard on this page

            for (Shard shard : shards) {
                String shardId = shard.getShardId();

                // Get an iterator for the current shard
                GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest()
                        .withStreamArn(streamArn)
                        .withShardId(shardId)
                        .withShardIteratorType(ShardIteratorType.TRIM_HORIZON);
                GetShardIteratorResult getShardIteratorResult =
                        streamsClient.getShardIterator(getShardIteratorRequest);
                String currentShardIter = getShardIteratorResult.getShardIterator();

                // Shard iterator is not null until the Shard is sealed (marked as READ_ONLY).
                // To prevent running the loop until the Shard is sealed, which will be on average
                // 4 hours, we process only the items that were written into DynamoDB and then exit.
                int processedRecordCount = 0;
                while (currentShardIter != null /*&& processedRecordCount < maxItemCount*/ ) {
                    System.out.println("    Shard iterator: " + currentShardIter.substring(380));

                    // Use the shard iterator to read the stream records

                    GetRecordsResult getRecordsResult = streamsClient.getRecords(new GetRecordsRequest()
                            .withShardIterator(currentShardIter));
                    List<Record> records = getRecordsResult.getRecords();
                    for (Record record : records) {
                        StreamRecord streamRecord = record.getDynamodb();

                        Map<String, AttributeValue> keys = streamRecord.getKeys();
                        ddbProxy.putItem("test", keys);
                        //TODO: write to DSE via dynamo proxy
                        System.out.println("        " + record.getDynamodb());
                    }
                    processedRecordCount += records.size();
                    currentShardIter = getRecordsResult.getNextShardIterator();
                }
            }

            // If LastEvaluatedShardId is set, then there is
            // at least one more page of shard IDs to retrieve
            lastEvaluatedShardId = describeStreamResult.getStreamDescription().getLastEvaluatedShardId();

        } while (lastEvaluatedShardId != null);
    }
}
