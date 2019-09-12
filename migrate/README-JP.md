# DynamodbMigrate
 
 この migrate ディレクトリの目的は、AWS DynamoDB データベースから DSE / Cassandra へのワンタイムにマイグレーションをサポートすることです。このコードは、AWS SDK を利用して、DynamoDB のレコードをスキャンして dataframe に入れる Travis Crawford の github を利用しています。本リポジトリのコードは、この dataframe を利用し、構造に修正を加えて、Cassandra のテーブルに書き出します。
 
 このデモを実行するには、以下をローカルシステムにインストール済みで利用可能であることが前提となります。
 
   1. Datastax Enterprise 6.7.x
   2. Spark
   3. git
   4. mvn
   5. Travis Crawford のリポジトリ
 
 
 # ローカルの DSE / Cassandra の起動
 
 
 ### ローカルの OSX または Linux マシンで起動します。DSE tarball の例を以下に示します（Search 機能には -s、Analytics 機能には -k を指定）
 
   * `dse cassandra -k` 
   
 ## デモの用意と実行
 
   
 
 ### 必要な dependency のビルド
 
   このコードは、DynamoDB のレコードをスキャンして dataframe に入れる Travis Crawford の github を利用しています。
 
 ### これは、fork した Travis Crawford の dependency です
 
   * コードを保存するディレクトリに移動します。
   * 次のコマンドを実行します。
 	`https://github.com/jphaugla/spark-dynamodb.git`
   * このコードをビルドします。
 ```bash
 mvn package
 ```
   * この dependency をインストールします。
 ```bash
 mvn install -Dgpg.skip
 ```
   * 詳細については、readme を参照してください。
 https://github.com/jphaugla/spark-dynamodb/blob/master/README.md
 
 
 ### このデモを実行するには、GitHub からソースコードをダウンロードする必要があります。
 
   * ダウンロードしたプロジェクトの migrate ディレクトリに移ります。
   * 次のコマンドを実行します。
        `git clone git@github.com:datastax/dynamo-cassandra-proxy.git`
   * コードをビルドします。
 	`cd dynamodb-cassandra-proxy/migrate && mvn package`
   
 
 ### 実行方法
 
   * プロジェクトの migrate ディレクトリで、プロデューサーアプリを開始します。
   
 	`./runit.sh`
     
   
 ####  mvn の問題
 Spark と DSE の dependency に関わる jar ファイルを削除する必要がありました。
 
 	rm -rf ~/.ivy2
 	rm -rf ~/.m2
 
 ## 今後について
 
    * run.sh スクリプトの一部として、テーブル名の他に、hash_key と sort_key を渡す必要がある。できれば、テーブル名だけを渡し、AWS SDK を使って hash_key と sort_key を取得したい。
    * DSE Analytics を使用した実行。本リポジトリと spark-dynamodb GitHub では、DSE Analytics 6.7.3 の Spark のバージョンに対応した pom.xml ファイルを作成している。しかし、それらのバージョンで動作させることができなかったので、現状は放棄。