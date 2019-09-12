FROM openjdk:11

COPY target/dynamodb-cassandra-proxy-0.1.0.jar /opt/dynamo-cassandra-proxy/dynamodb-cassandra-proxy-0.1.0.jar
COPY conf/dynamo-cassandra-proxy.yaml.template /opt/dynamo-cassandra-proxy/dynamo-cassandra-proxy.yaml

CMD java -jar /opt/dynamo-cassandra-proxy/dynamodb-cassandra-proxy-0.1.0.jar server /opt/dynamo-cassandra-proxy/dynamo-cassandra-proxy.yaml
