package com.datastax.powertools.dcp.managed.ddbstreams;

/*
 *
 * @author Sebastián Estévez on 12/26/18.
 *
 */


import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClientBuilder;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.datastax.powertools.dcp.DCProxyConfiguration;
import io.dropwizard.lifecycle.Managed;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.dynamodbv2.streamsadapter.StreamsWorkerFactory;


import java.util.List;
import java.util.Map;
import java.util.Properties;

public class DynamoStreamsManager implements Managed {
    private final AmazonDynamoDB ddbProxy;
    private String dynamodbEndpoint;
    private String streamsEndpoint;
    private String signinRegion;
    private String accessKey;
    private String secretKey;
    private AmazonDynamoDBStreams streamsClient;
    private AmazonDynamoDB realDDB;
    private String streamArn;

    //private StreamSpecification streamSpec;

    //TODO: support multiple streams from multiple tables
    private AmazonDynamoDBStreamsAdapterClient adapterClient;
    private StreamsRecordProcessorFactory recordProcessorFactory;
    private KinesisClientLibConfiguration workerConfig;

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

        //TODO make table name dynamic
        String tableName = "test";

        this.dynamodbEndpoint = config.getAwsDynamodbEndpoint();
        this.streamsEndpoint = config.getStreamsEndpoint();
        this.signinRegion = config.getDynamoRegion();
        this.accessKey = config.getDynamoAccessKey();
        this.secretKey = config.getDynamoSecretKey();

        Properties props = System.getProperties();
        props.setProperty("aws.accessKeyId", accessKey);
        props.setProperty("aws.secretKey", secretKey);

        AwsClientBuilder.EndpointConfiguration endpointConfiguration =
                new AwsClientBuilder.EndpointConfiguration(streamsEndpoint, signinRegion);
        SystemPropertiesCredentialsProvider spcp = new SystemPropertiesCredentialsProvider();

        realDDB = AmazonDynamoDBClientBuilder.standard().
                withRegion(Regions.US_EAST_2).
                //withEndpointConfiguration(endpointConfiguration).
                withCredentials(spcp).build();

        DescribeTableResult tableResult = realDDB.describeTable(tableName);
        streamArn = tableResult.getTable().getLatestStreamArn();
        //streamSpec = tableResult.getTable().getStreamSpecification();
        streamsClient = AmazonDynamoDBStreamsClientBuilder.standard().withEndpointConfiguration(endpointConfiguration).build();

        adapterClient = new AmazonDynamoDBStreamsAdapterClient(streamsClient);

        recordProcessorFactory = new StreamsRecordProcessorFactory(ddbProxy, tableName);

        workerConfig = new KinesisClientLibConfiguration("test-app",
                streamArn,
                spcp,
                "streams-worker")
                .withMaxRecords(1000)
                .withIdleTimeBetweenReadsInMillis(500)
                .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);
        AmazonCloudWatch cloudWatchClient;
        cloudWatchClient = AmazonCloudWatchClientBuilder.standard()
        .withRegion(signinRegion)
        .build();

        System.out.println("Creating worker for stream: " + streamArn);

        /*
        DescribeStreamRequest request = new DescribeStreamRequest();
        DescribeStreamRequestAdapter describeStreamResult = new DescribeStreamRequestAdapter(request);
        String id = describeStreamResult.getExclusiveStartShardId();
        String id2 = describeStreamResult.withStreamArn(streamArn).getExclusiveStartShardId();
        */

        Worker worker = StreamsWorkerFactory.createDynamoDbStreamsWorker(
                recordProcessorFactory,
                workerConfig,
                adapterClient,
                realDDB,
                cloudWatchClient
        );

        System.out.println("Starting worker...");
        Thread t = new Thread(worker);
        t.start();
    }

    /*
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
                //while (currentShardIter != null && processedRecordCount < maxItemCount) {
                while (currentShardIter != null ) {
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
    */
}
