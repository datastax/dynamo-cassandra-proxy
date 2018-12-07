package com.datastax.powertools.dcp.managed;

/*
 *
 * @author Sebastián Estévez on 12/6/18.
 *
 */


import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public class DSEStmts {


    private static final String KEYSPACE_PATTERN = ";;;KEYSPACE;;;";
    private static final String REPLICATION_STRATEGY_PATTERN = ";;;REPLICATION_STRATEGY;;;";

    private static String STMT_create_keyspace = String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = %s ;", KEYSPACE_PATTERN, REPLICATION_STRATEGY_PATTERN);
    private static String STMT_get_tables = String.format("SELECT * FROM %s.%s", "system_schema", "tables");

    public static class Prepared {
        private final String keyspace;
        private final Session session;
        private final String replicationStrategy;

        final PreparedStatement get_tables;
        final PreparedStatement create_keyspace;

        public Prepared(Session session, String keyspace, String replicationStrategy) {
            this.keyspace = keyspace;
            this.session = session;
            this.replicationStrategy = replicationStrategy;

            create_keyspace = prepare(STMT_create_keyspace);

            //Ensure the dynamo keyspace exists
            session.execute(create_keyspace.bind());

            get_tables = prepare(STMT_get_tables);
        }

        private PreparedStatement prepare(String stmt) {
            String withKeyspace
                    = stmt.replaceAll(KEYSPACE_PATTERN, this.keyspace);
            String withReplication
                    = withKeyspace.replaceAll(REPLICATION_STRATEGY_PATTERN, this.replicationStrategy);

            PreparedStatement prepared = session.prepare(withReplication);

            if (stmt.contains("solr_query")) {
                prepared.setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
            } else {
                prepared.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
            }
            return prepared;
        }
    }
}
