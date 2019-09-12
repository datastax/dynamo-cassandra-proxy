FROM openjdk:11
MAINTAINER Sebastian Estevez estevezsebastian@gmail.com

# Install all apt-get utils and required repos
RUN apt-get update && \
    apt-get upgrade -y && \
    # Install add-apt-repository
    apt-get install -y \
        software-properties-common && \
    apt-get update && \
    # Install
    apt-get install -y \
	git maven

COPY target/ /opt/dynamo-cassandra-proxy

COPY conf/dynamo-cassandra-proxy.yaml.template /opt/dynamo-cassandra-proxy/dynamo-cassandra-proxy.yaml

CMD java -cp /opt/dynamo-cassandra-proxy/dynamodb-cassandra-proxy-0.1.0.jar com.datastax.powertools.dcp.DCProxyApplication server /opt/dynamo-cassandra-proxy/dynamo-cassandra-proxy.yaml
