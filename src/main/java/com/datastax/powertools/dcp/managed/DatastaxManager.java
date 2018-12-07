package com.datastax.powertools.dcp.managed;

/*
 *
 * @author Sebastián Estévez on 12/6/18.
 *
 */


import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.datastax.driver.dse.graph.GraphOptions;
import com.datastax.powertools.dcp.DCProxyConfiguration;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DatastaxManager implements Managed {
    private final static Logger logger = LoggerFactory.getLogger(DatastaxManager.class);

    private int cqlPort = 9042;
    private String[] contactPoints = new String[]{"localhost"};
    private DseCluster cluster;
    private DseSession session;
    private String username;
    private String password;
    private String keyspaceName;
    private DSEStmts.Prepared stmts;
    private String replicationStrategy;
    private Set<String> tables;

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

        //TODO schema
        ResultSet rows = session.execute(stmts.get_tables.bind());

        tables = new HashSet<>();
        for (Row row: rows){
            String table = row.getString("table_name");
            tables.add(table);
        }
    }

    public void stop() throws Exception {
        session.close();
        cluster.close();
    }
}
