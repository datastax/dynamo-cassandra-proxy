/bin/bash

contactPoints="localhost"

if [ -z "$1" ]; then
  contactPoints="$1"
fi

nohup java -Ddw.contactPoints="$contactPoints" -cp target/dynamodb-cassandra-proxy-0.1.0.jar com.datastax.powertools.dcp.DCProxyApplication server conf/dynamo-cassandra-proxy.yaml &
