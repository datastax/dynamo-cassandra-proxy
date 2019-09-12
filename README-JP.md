# dynamo-cassandra-proxy

`dynamo-cassandra-proxy` は、アプリケーションと Apache Cassandra の間に座る、スケーラブルなプロキシレイヤーとして構成されます。

このプロキシレイヤーは、DynamoDB SDK との互換性を提供します。そうすることで、既存の DynamoDB アプリケーションに変更を加えることなく Cassandra を対象としたデータの読み書きが可能になります。

また、DynamoDB Streams 経由で、DynamoDB のテーブルを Cassandra 側と同期させる機能もサポートします。

## 構成設定

以下のように、テンプレートに基づいて yaml を作成します。

    cp conf/dynamo-cassandra-proxy.yaml.template conf/dynamo-cassandra-proxy.yaml

以下は、このプロキシがサポートするオプションです。

| オプション | 説明 |
| -------- | ---------- |
|streamsEnabled| true に設定すると、プロキシは既存の DynamoDB テーブルからのライブデータの取り込みを有効します| 
|dynamoRegion| 上記のストリーミングを有効にした場合にのみ必要。DynamoDB テーブルが格納さているリージョン|
|dyanmoAccessKey| 上記のストリーミングを有効にした場合にのみ必要。DynamoDB Streams への接続に使用|
|dyanmoSecretKey| 上記のストリーミングを有効にした場合にのみ必要。DynamoDB Streams への接続に使用|
|awsDynamodbEndpoint| 上記のストリーミングを有効にした場合にのみ必要。DynamoDB Streams への接続に使用|
|contactPoints| Apache Cassandra(TM) クラスターへの接続に使用するコンタクトポイント。下記の docker オプションを使用する場合は、localhost のままにします|
|dockerCassandra| true に設定すると、ローカルの docker で Cassandra を起動します。docker デーモンがインストールされ、実行されていることと、使用するユーザーが `docker ps` の実行権限を持っていることを確認してください|


## ローカルでの実行

クローン:

    git clone git@github.com:datastax/dynamo-cassandra-proxy.git

ビルド:

    mvn package

プロキシの実行: プロキシを自分で用意した Cassandra クラスターに接続するにしろ、yaml ファルの中で cassandraDocker オプションを利用してプロキシに Cassandra ノードを立ち上げてもらうにしろ、ローカルでコードを実行するには、以下のようします。

    java -Ddw.contactPoints="$contactPoints" -cp target/dynamodb-cassandra-proxy-0.1.0.jar com.datastax.powertools.dcp.DCProxyApplication server conf/dynamo-cassandra-proxy.yaml

プロキシは起動すると、8080 番のポートをリッスンします。そうしたら、DynamoDB アプリケーションを SDK の中で `<ホスト名>:8080` に向けます。接続文字列の参考例を以下に示します（Java の場合）。

            ClientConfiguration config = new ClientConfiguration();
            config.setMaxConnections(dynamodbMaxConnections);;
            String dynamodbEndpoint = "localhost:8080"
            String signinRegion = "dummy"
            AwsClientBuilder.EndpointConfiguration endpointConfiguration = new AwsClientBuilder
                    .EndpointConfiguration(protocol + "://" + dynamodbEndpoint, signinRegion);
            ddbBuilder = AmazonDynamoDBClientBuilder.standard()
                    .withClientConfiguration(config)
                    .withEndpointConfiguration(endpointConfiguration);

メモ： `MaxConnections` は、AWS SDK をごく基本レベルの性能を超えて動作するようにする主要な鍵となります。参考までに、この値を最大 50 まで上げていきながらテストしましたが、ミディアムサイズの環境で、ほぼリニアにスケールすることを確認しています。ベンチマークなどのために Cassandra クラスターを飽和させることを目的としている場合には、この値を大きくして見てください。


## docker-compose を利用した実行

以下のコマンドでビルドします。

    mvn package
    
docker コンテナをビルドして実行します。
    
    docker-compose up


## ローカルの Kubernetes での実行

Cassandra 用の config map の設定:

    kubectl create configmap cassandra-config \
--from-file=common/cassandra/conf-dir/resources/cassandra/conf 

k8s yaml の適用:

    kubectl apply -f k8s-local/proxy-suite.yaml 

この時点で、pod は以下のようになっているはずです。

```
$ kubectl get pods                                                                                     [2:34:13]
NAME                  READY   STATUS              RESTARTS   AGE
cassandra-0           1/1     Running             0          2m35s
cassandra-1           1/1     Running             0          168s
cassandra-3           1/1     Running             0          123s
dynamo-cass-proxy-0   1/1     Running             4          63s
```

実行したデプロイを終了するには、以下のようにします。

    kubectl delete -f k8s-local/proxy-suite.yaml 


## コントリビューション

Translator を用意するのも手始めとしては良いかもしれません。
Translator の詳細については、[docs 内の Translators](docs/Translators-JP.md) をご覧ください。

## MVP（実用最小限の実装）のロードマップ:

印が入っているところは現時点の実装済機能:

 - [x] CreateTable - json_blob で実装
 - [x] DeleteItem - json_blob で実装
 - [ ] DeleteTable
 - [x] GetItem - json_blob で実装
 - [x] PutItem - json_blob で実装
 - [ ] Query - json_blob で単純なケースを実装
 - [ ] Scan
 - [x] ハイブリッド機能 - DynamoDB から Cassandra へ
 - [ ] ハイブリッド機能 - Cassandra から DynamoDB へ

未実装の**その他の機能**

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

## ライセンス
本プロジェクトは、Apache Public License 2.0 のもとにライセンスします
