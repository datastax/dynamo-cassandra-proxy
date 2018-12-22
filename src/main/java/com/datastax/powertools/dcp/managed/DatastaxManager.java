package com.datastax.powertools.dcp.managed;

/*
 *
 * @author Sebastián Estévez on 12/6/18.
 *
 */


import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.datastax.powertools.dcp.DCProxyConfiguration;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class DatastaxManager implements Managed {
    private final static Logger logger = LoggerFactory.getLogger(DatastaxManager.class);

    private int cqlPort = 9042;
    private String[] contactPoints = new String[]{"localhost"};
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

    public void start() throws Exception {
        DseCluster.Builder builder = DseCluster.builder().
                addContactPoints(contactPoints).
                withPort(cqlPort).
                withCredentials(username, password).
                withoutJMXReporting();

        password = null; // defensive

        cluster = builder.build();
        session = this.cluster.connect();
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


            if (kind.equals("partition_key")){
                tableRepresentation.addPK(colName);
                PreparedStatement jsonQueryStatement = stmts.prepare(String.format("SELECT * from %s.%s where %s = ?", keyspaceName, table, colName));
                tableRepresentation.setJsonQueryStatement(jsonQueryStatement);
            }else if (kind.equals("clustering")){
                tableRepresentation.addClusteringColumn(colName);
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
        return tableDefs.get(tableName).getJsonPutStatement();
    }

    public PreparedStatement getQueryStatement(String tableName) {
        return tableDefs.get(tableName).getJsonQueryStatement();
    }

    public List<String> getPartitionKeys(String tableName) {
        return tableDefs.get(tableName).getPartitionKeys();
    }

    public List<String> getClusteringColumns(String tableName) {
        return tableDefs.get(tableName).getClusteringColumns();
    }
}
