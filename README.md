# dynamo-cassandra-proxy

`dynamo-cassandra-proxy` consists of a scalable proxy layer that sits between your app and Apache Cassandra.

It provides compatibility with the DynamoDB SDK which allows existing DynamoDB applications to read and write data to Cassandra without application changes.

It also supports the ability to sync DynamoDB tables with cassandra via DynamoDB Streams.

## Config

Create your yaml based on the template:

    cp conf/dynamo-cassandra-proxy.yaml.template conf/dynamo-cassandra-proxy.yaml

The following are the options supported by the proxy:

| Option | Description|
| ------ | ---------- |
|streamsEnabled| When set to true it enables the proxy to pull live data from an existing dynamodb table| 
|dynamoRegion| Only needed when streaming is enabled, region your dynamodb table is in |
|dyanmoAccessKey| Only needed when streaming is enabled, used to connect to dynamodb streams|
|dyanmoSecretKey| Only needed when streaming is enabled, used to connect to dynamodb streams|
|awsDynamodbEndpoint| Only needed when streaming is enabled, used to connect to dynamodb streams|
|contactPoints| Contact points to connect to Apache Cassandra(TM) cluster. If you are using the docker option just leave localhost|
|dockerCassandra| When set to true it will stand up Cassandra in your local docker. Ensure the docker deamon is installed and running and your user has access to run `docker ps`|


## To run locally

Clone:

    git clone git@github.com:datastax/dynamo-cassandra-proxy.git

Run:

    java -Ddw.contactPoints="$contactPoints" -cp target/dynamodb-cassandra-proxy-0.1.0.jar com.datastax.powertools.dcp.DCProxyApplication server conf/dynamo-cassandra-proxy.yaml


## Contributing

A good place to start might be fleshing out your own Translator.
For details on translators see Translators.md in /docs

## MVP Roadmap:

Check means currently completed:

 - [x] CreateTable - Done in json_blob
 - [x] DeleteItem - Done in json_blob
 - [ ] DeleteTable
 - [x] GetItem - Done in json_blob
 - [x] PutItem - Done in json_blob
 - [ ] Query - Simple Case done in json_blob
 - [ ] Scan
 - [x] Hybrid functionality - DDB to Cassandra
 - [ ] Hybrid functionality - Cassandra to DDB

**Other features** not yet implemented:

- UpdateItem
- BatchGetItem
- BatchWriteItem
- DescribeStream
- DescribeTable
- DescribeLimits
- DescribeTimeToLive
- GetRecords
- GetShardIterator
- ListStreams
- ListTables
- ListTagsOfResource
- TagResource
- UntagResource
- UpdateTable
- UpdateTimeToLive

## License
This project is licensed under the Apache Public License 2.0