## Translator

`dynamo-db-proxy` は、dynamodb のリクエストから CQL のデータモデルへの変換のために Translator を使用します。

dynamodb は、単独のパーティションキーと、単独のソートキーを使用します。その他はすべて JSON です。クエリー API は、クエリー時に JSON ペイロード内の個別フィールドの選択を可能にしますが、グローバルインデックスを除き、その選択範囲は、パーティション内に限られます。
DDB supports a single partition key and a single sort key. Everything else is JSON. The query API allows users to select individual fields within the JSON payload at query time but only within a partition with the exception of global indexes.

Translator は、DynamoDB の機能を部分的にサポートしたり、それ以外のみ実装の機能については UnimplementedExceptions を throw することができます。

Translator クラスは、`DynamoDSETranslator` を extend しなければなりません。

## Cassandra / DSE のデータモデル

### json_blob

json_blob - 現在の実装

```
CREATE TABLE dynamoks.test (
    hash_key double,
    sort_key double,
    json_blob text,
    PRIMARY KEY (hash_key, sort_key)
) WITH CLUSTERING ORDER BY (sort_key ASC)
```

最も単純な実装となっています。制限としては、DynamoDB の UpdateItem API は、クエリーのたびに write の前に read が必要となるため、対応が容易ではないということです。そのため、更新クエリーは、サポートされていない旨の例外を返します。
```
cqlsh> select * from dynamoks.test ;

 hash_key | sort_key | json_blob
----------+----------+----------------------------------------------------------------------------------------------------------------------------------
        1 |        2 | {"favorites":{"SS":["puppies","kittens","other cute animals"]},"hash_key":{"S":"1.0"},"city":{"S":"NYC"},"sort_key":{"S":"2.0"}}

```
### マルチバージョン

LSM の考えを取り入れたデータモデルであれば、更新をバージョニングし、read 時に LWW のセマンティクスをクライアント側で実装して使用し、（コンパクションのような）バックグラウドで動作するクリーンアップジョブを用意すれば、DynamoDB の UpdateItem API をサポートできるかもしれません。

```
CREATE TABLE dynamoks.test (
    hash_key double,
    sort_key double,
    version timeuuid,
    json_blob text,
    PRIMARY KEY (hash_key, sort_key, version)
) WITH CLUSTERING ORDER BY (sort_key ASC)
```

データ例:
```
cqlsh> select * from dynamoks.test ;

 hash_key | sort_key | version |json_blob
----------+----------+---------------------
        1 |        2 |       1 |{"favorites":{"SS":["puppies","kittens","other cute animals"]},"hash_key":{"S":"1.0"},"sort_key":{"S":"2.0"}}
        1 |        2 |       2 |{"favorites":{"SS":["kittens","other cute animals"]},"hash_key":{"S":"1.0"},"city":{"S":"NYC"},"sort_key":{"S":"2.0"}}
```

クエリー時には、プロキシがパーティション全体を読み取り、マージを行い、マージ結果を返すという方法が考えられます。

```
{"favorites":{"SS":["puppies","kittens","other cute animals"]},"hash_key":{"S":"1.0"},"city":{"S":"NYC"},"sort_key":{"S":"2.0"}}
```



### 完全非正規化

より良いマルチモデルと CQL への対応を考えた場合、以下のようなデータモデルならば、dynamodb 項目を非正規化して複数のレコードにできます。

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

データ例:
```
cqlsh> select * from dynamoks.test ;

 hash_key | sort_key | column_name | value_numeric | value_string | value_date
----------+----------+----------------------------------------------------
        1 |        2 |   'puppies' |  3            |               | 
        1 |        2 |   'City'    |               | 'NYC'         | 
```


クエリー時には、プロキシがパーティション全体を読み取り、マージを行い、マージ結果を返すということになります。

```
{"favorites":{"N":["puppies",3]},"hash_key":{"S":"1.0"},"city":{"S":"NYC"},"sort_key":{"S":"2.0"}}
```

上記の混合も考えられます（完全非正規化とマルチバージョンの混合）