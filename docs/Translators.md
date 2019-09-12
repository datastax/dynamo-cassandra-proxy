## Translators

The `dynamo-db-proxy` uses Translators to go from dynamodb requests to CQL data models.

DDB supports a single partition key and a single sort key. Everything else is JSON. The query API allows users to select individual fields within the JSON payload at query time but only within a partition with the exception of global indexes.

Some Translators can support partial DynamoDB functionality and throw UnimplementedExceptions for other functionality.

A translator class must extend `DynamoDSETranslator`.

## DSE Data Models:

### json_blob

json_blob - currently implemented

```
CREATE TABLE dynamoks.test (
    hash_key double,
    sort_key double,
    json_blob text,
    PRIMARY KEY (hash_key, sort_key)
) WITH CLUSTERING ORDER BY (sort_key ASC)
```

This is the simplest implementation. The downside is that it cannot feasibly support the DDB UpdateItem API because it would require a read before write on every query. Update queries will return a not supported exception.
```
cqlsh> select * from dynamoks.test ;

 hash_key | sort_key | json_blob
----------+----------+----------------------------------------------------------------------------------------------------------------------------------
        1 |        2 | {"favorites":{"SS":["puppies","kittens","other cute animals"]},"hash_key":{"S":"1.0"},"city":{"S":"NYC"},"sort_key":{"S":"2.0"}}

```
### multi-version
An LSM inspired data model could support the DDB UpdateItem API by versioning updates and using client implemented LWW semantics on read and potentially a cleanup (compaction-like) background job. 
```
CREATE TABLE dynamoks.test (
    hash_key double,
    sort_key double,
    version timeuuid,
    json_blob text,
    PRIMARY KEY (hash_key, sort_key, version)
) WITH CLUSTERING ORDER BY (sort_key ASC)
```

Example data:
```
cqlsh> select * from dynamoks.test ;

 hash_key | sort_key | version |json_blob
----------+----------+---------------------
        1 |        2 |       1 |{"favorites":{"SS":["puppies","kittens","other cute animals"]},"hash_key":{"S":"1.0"},"sort_key":{"S":"2.0"}}
        1 |        2 |       2 |{"favorites":{"SS":["kittens","other cute animals"]},"hash_key":{"S":"1.0"},"city":{"S":"NYC"},"sort_key":{"S":"2.0"}}
```

For queries, the proxy would read the whole partition, merge and return a merged response:
```
{"favorites":{"SS":["puppies","kittens","other cute animals"]},"hash_key":{"S":"1.0"},"city":{"S":"NYC"},"sort_key":{"S":"2.0"}}
```



### fully-denormalized
For better multi-model and cql support, the following data model would denormalize the dynamodb item into multiple records. 
```
CREATE TABLE dynamoks.test (
    hash_key double,
    sort_key double,
    column_name text,
    value_numeric double,
    value_string text,
    value_date date,
    PRIMARY KEY (hash_key, sort_key, column_name)
) WITH CLUSTERING ORDER BY (sort_key ASC)
```

Example data:
```
cqlsh> select * from dynamoks.test ;

 hash_key | sort_key | column_name | value_numeric | value_string | value_date
----------+----------+----------------------------------------------------
        1 |        2 |   'puppies' |  3            |               | 
        1 |        2 |   'City'    |               | 'NYC'         | 
```


For queries, the proxy would read the whole partition, merge and return a merged response:
```
{"favorites":{"N":["puppies",3]},"hash_key":{"S":"1.0"},"city":{"S":"NYC"},"sort_key":{"S":"2.0"}}
```

A mix is also possible (Fully denormalized / multi-version)
