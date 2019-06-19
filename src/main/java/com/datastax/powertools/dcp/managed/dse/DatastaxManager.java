package com.datastax.powertools.dcp.managed.dse;

/*
 *
 * @author Sebastián Estévez on 12/6/18.
 *
 */


import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.datastax.powertools.dcp.DCProxyConfiguration;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class DatastaxManager implements Managed {
    private final static Logger logger = LoggerFactory.getLogger(DatastaxManager.class);

    private int cqlPort = 9042;
    private String contactPoints = new String();
    private DseCluster cluster;

    public DseSession getSession() {
        return session;
    }

    private DseSession session;
    private String username;
    private String password;
    private String keyspaceName;
    private DSEStmts.Prepared stmts;
    private String replicationStrategy;
    private Map<String, TableDef> tableDefs;

    public String getComponentName() {
        return "dse";
    }

    public void configure(DCProxyConfiguration config) {
        contactPoints = config.getContactPoints();
        cqlPort = config.getCqlPort();
        username = config.getCqlUserName();
        password = config.getCqlPassword();
        keyspaceName = config.getKeyspaceName();
        replicationStrategy= config.getReplicationStrategy();
    }

    public void start() {
        DseCluster.Builder builder = DseCluster.builder().
                addContactPoints(contactPoints.split(",")).
                withPort(cqlPort).
                withCredentials(username, password).
                withoutJMXReporting();

        password = null; // defensive

        cluster = builder.build();
        try {
            session = this.cluster.connect();
        }catch (NoHostAvailableException e){
            logger.warn("Cluster not found, standing up container.");
            DockerHelper dh = new DockerHelper();
            dh.startDSE();
            cluster = builder.build();
            session = this.cluster.connect();
        }
        logger.info("Preparing statements for " + DatastaxManager.class.getSimpleName());
        stmts = new DSEStmts.Prepared(session, keyspaceName, replicationStrategy);

        refreshSchema();
    }

    public void refreshSchema() {

        ResultSet rows = session.execute(stmts.get_columns.bind());

        tableDefs = new HashMap<>();
        for (Row row: rows){
            String table = row.getString("table_name");
            TableDef  tableRepresentation;

            if (tableDefs.containsKey(table)) {
                tableRepresentation = tableDefs.get(table);
            }else {
                tableRepresentation = new TableDef();
                PreparedStatement jsonPutStatement = stmts.prepare(String.format("INSERT INTO %s.%s JSON ?", keyspaceName, table));
                tableRepresentation.setJsonPutStatement(jsonPutStatement);
            }

            String kind = row.getString("kind");
            String colName= row.getString("column_name");
            DataType.Name type = DataType.Name.valueOf(row.getString("type").toUpperCase());


            if (kind.equals("partition_key")){
                tableRepresentation.addPK(colName, type);

                //TODO: handle compound partition keys
                PreparedStatement jsonQueryStatement = stmts.prepare(String.format("SELECT * from %s.%s where %s = ?", keyspaceName, table, colName));
                tableRepresentation.setJsonQueryPartitionStatement(jsonQueryStatement);
            }else if (kind.equals("clustering")){
                tableRepresentation.addClusteringColumn(colName, type);

                Map<String, DataType.Name> partitionKeyMap = tableRepresentation.getPartitionKeyMap();
                String pk = "";
                //note this works because hash comes before sort in the alphabet
                for (String partitionKey : partitionKeyMap.keySet()) {
                    pk = partitionKey;
                    continue;
                }

                //TODO: handle compound clustering columns
                PreparedStatement deleteStatement = stmts.prepare(
                        String.format(
                                "DELETE from %s.%s where %s = ? and %s = ?",
                                keyspaceName,
                                table,
                                colName,
                                pk
                                ));
                tableRepresentation.setDeleteStatement(deleteStatement);

                PreparedStatement queryRowStatement = stmts.prepare(
                        String.format(
                                "select * from %s.%s where %s = ? and %s = ?",
                                keyspaceName,
                                table,
                                colName,
                                pk
                                ));
                tableRepresentation.setQueryRowStatement(queryRowStatement);
            }

            if (tableDefs.containsKey(table)) {
                tableDefs.replace(table, tableRepresentation);
            }else{
                tableDefs.put(table, tableRepresentation);
            }
        }
    }

    public void stop() throws Exception {
        session.close();
        cluster.close();
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

    public PreparedStatement getDeleteStatement(String tableName) {
        return tableDefs.get(tableName).getDeleteStatement();
    }

    public PreparedStatement getQueryStatement(String tableName) {
        return tableDefs.get(tableName).getJsonQueryPartitionStatement();
    }

    public PreparedStatement getQueryRowStatement(String tableName) {
        return tableDefs.get(tableName).getQueryRowStatement();
    }

    public Map<String, DataType.Name> getPartitionKeys(String tableName) {
        return tableDefs.get(tableName).getPartitionKeyMap();
    }

    public Map<String, DataType.Name> getClusteringColumns(String tableName) {
        return tableDefs.get(tableName).getClusteringColumnMap();
    }

    public boolean tableInSchema(String tableName) {
        return tableDefs.containsKey(tableName);
    }

    public TableDef getTableDef(String tableName) {
        return tableDefs.get(tableName);
    }
}
