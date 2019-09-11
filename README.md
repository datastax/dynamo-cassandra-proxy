# dynamo-cassandra-proxy

dynamo-cassandra-proxy consists of a scalable proxy layer that sits between your app and Apache Cassandra.

It provides compatibility with the DynamoDB SDK which allows existing DynamoDB applications to read and write data to Cassandra without application changes.

It also supports the ability to sync DynamoDB tables with cassandra via DynamoDB Streams.

## To run locally

    java -Ddw.contactPoints="$contactPoints" -cp target/dynamodb-cassandra-proxy-0.1.0.jar com.datastax.powertools.dcp.DCProxyApplication server conf/dynamo-cassandra-proxy.yaml

## To run on k8s



## Contributing



## MVP Roadmap:

 - [x] CreateTable - Done in json_blob
 - [x] DeleteItem - Done in json_blob
 - [ ] DeleteTable
 - [x] GetItem - Done in json_blob
 - [x] PutItem - Done in json_blob
 - [ ] Query - Simple Case done in json_blob
 - [ ] Scan
 - [x] Hybrid functionality - DDB to Cassandra
 - [ ] Hybrid functionality - Cassandra to DDB

## Other features:

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
