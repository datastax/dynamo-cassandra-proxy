/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.powertools.dcp.managed.dse;


import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.powertools.dcp.DCProxyConfiguration;
import com.google.common.collect.Maps;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.stream.Collectors;

public class CassandraManager implements Managed {
    private final static Logger logger = LoggerFactory.getLogger(CassandraManager.class);

    public CqlSession getSession() {
        return session;
    }

    private DCProxyConfiguration config;
    private String keyspaceName;
    private CqlSession session;
    private CassandraStatements.Prepared stmts;
    private final Map<String, TableDef> tableDefs = Maps.newConcurrentMap();

    public void configure(DCProxyConfiguration config) {
        this.config = config;
        this.keyspaceName = config.getKeyspaceName();
    }

    public void start() {
        logger.info("Contact points {}", config.getContactPoints());

        CqlSessionBuilder builder = CqlSession.builder()
                .addContactPoints(Arrays.stream(config.getContactPoints().split(","))
                        .map(s -> new InetSocketAddress(s, config.getCqlPort()))
                        .collect(Collectors.toList()))
                .withLocalDatacenter(config.getLocalDC());

        if (config.getCqlUserName() != null)
            builder.withAuthCredentials(config.getCqlUserName(), config.getCqlPassword());

        if (config.isDockerCassandra()) {
            logger.info("Docker cassandra enabled in the yaml.");
            logger.info("Attempting to stand up container.");
            DockerHelper dh = new DockerHelper();
            dh.startDSE();

            //TODO
            /*
            try {
                Thread.sleep(60000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            */

        }

        session = builder.build();


        logger.info("Preparing statements for " + CassandraManager.class.getSimpleName());
        stmts = new CassandraStatements.Prepared(session, config.getKeyspaceName(), config.getReplicationStrategy());

        refreshSchema();
    }

    public void refreshSchema() {

        KeyspaceMetadata ksm = session.getMetadata().getKeyspace(keyspaceName).orElseThrow(() -> new RuntimeException("Keyspace missing: " + keyspaceName));

        for (Map.Entry<CqlIdentifier, TableMetadata> e : ksm.getTables().entrySet())
        {
            TableMetadata m = e.getValue();
            TableDef tableDef = new TableDef();
            String tableName = e.getKey().asInternal();

            tableDef.setKeyspaceName(keyspaceName);
            tableDef.setTableName("\""+tableName + "\"");
            tableDef.setSession(session);

            PreparedStatement jsonPutStatement = stmts.prepare(String.format("INSERT INTO %s.\"%s\" JSON ?", keyspaceName, tableName));
            tableDef.setJsonPutStatement(jsonPutStatement);

            List<ColumnMetadata> keys = m.getPartitionKey();
            if (keys.size() != 1)
                throw new IllegalStateException("Dynamo like tables can only contain one Partition Key: " + tableName);

            Map<ColumnMetadata, ClusteringOrder> clustering = m.getClusteringColumns();
            if (clustering.size() > 1)
                throw new IllegalStateException("Dynamo like tables can only contain upto one Clustering Key: " + tableName);

            ColumnMetadata partitionKey = keys.get(0);
            tableDef.setPartitionKey(partitionKey);
            String partitionKeyName = partitionKey.getName().asInternal();

            PreparedStatement jsonQueryStatement = stmts.prepare(
                    String.format("SELECT * from %s.\"%s\" where \"%s\" = ?", keyspaceName, tableName, partitionKeyName)
            );
            tableDef.setJsonQueryPartitionStatement(jsonQueryStatement);

            if (clustering.isEmpty())
            {
                PreparedStatement deleteStatement = stmts.prepare(
                        String.format("DELETE from %s.\"%s\" where \"%s\" = ?", keyspaceName, tableName, partitionKeyName)
                );
                tableDef.setDeleteStatement(deleteStatement);

                PreparedStatement queryRowStatement = stmts.prepare(
                        String.format("select * from %s.\"%s\" where \"%s\" = ?", keyspaceName, tableName, partitionKeyName)
                );
                tableDef.setQueryRowStatement(queryRowStatement);
            }
            else
            {
                ColumnMetadata clusteringKey = clustering.keySet().iterator().next();
                tableDef.setClusteringKey(clusteringKey);
                String clusteringKeyName = clusteringKey.getName().asInternal();

                PreparedStatement deleteStatement = stmts.prepare(
                        String.format("DELETE from %s.\"%s\" where \"%s\" = ? and \"%s\" = ?", keyspaceName, tableName, partitionKeyName, clusteringKeyName)
                );
                tableDef.setDeleteStatement(deleteStatement);

                PreparedStatement queryRowStatement = stmts.prepare(
                        String.format("select * from %s.\"%s\" where \"%s\" = ? and \"%s\" = ?", keyspaceName, tableName, partitionKeyName, clusteringKeyName)
                );
                tableDef.setQueryRowStatement(queryRowStatement);
            }

            tableDefs.put(tableName, tableDef);
        }
    }

    public void stop() throws Exception {
        session.close();

        if (config.isDockerCassandra()) {
            DockerHelper dh = new DockerHelper();
            dh.stopDSE();
        }
    }

    public String getKeyspaceName() {
        return keyspaceName;
    }

    public PreparedStatement getPutStatement(String tableName) {
        TableDef tableDef = tableDefs.get(tableName);
        if (tableDef == null){
            logger.error(String.format("Table %s does not exist", tableName));
            return null;
        }
        return tableDefs.get(tableName).getJsonPutStatement();
    }

    public boolean hasTable(String tableName)
    {
        return tableDefs.containsKey(tableName);
    }

    public TableDef getTableDef(String tableName) {
        TableDef tableDef = tableDefs.get(tableName);
        if (tableDef == null)
            throw new MissingResourceException("Table not found " + tableName, tableName, tableName);

        return tableDefs.get(tableName);
    }
}
