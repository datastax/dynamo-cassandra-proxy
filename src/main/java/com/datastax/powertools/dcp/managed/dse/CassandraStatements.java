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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

public class CassandraStatements {
    private static final String KEYSPACE_PATTERN = ";;;KEYSPACE;;;";
    private static final String REPLICATION_STRATEGY_PATTERN = ";;;REPLICATION_STRATEGY;;;";

    private static String STMT_create_keyspace = String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = %s ;", KEYSPACE_PATTERN, REPLICATION_STRATEGY_PATTERN);
    private static String STMT_get_columns = String.format("SELECT * FROM %s.%s WHERE keyspace_name = '%s' ;", "system_schema", "columns", KEYSPACE_PATTERN);

    public static class Prepared {
        private final String keyspace;
        private final CqlSession session;
        private final String replicationStrategy;

        final PreparedStatement get_columns;
        final PreparedStatement create_keyspace;

        public Prepared(CqlSession session, String keyspace, String replicationStrategy) {
            this.keyspace = keyspace;
            this.session = session;
            this.replicationStrategy = replicationStrategy;

            create_keyspace = prepare(STMT_create_keyspace);

            //Ensure the dynamo keyspaceName exists
            session.execute(create_keyspace.bind());

            get_columns = prepare(STMT_get_columns);
        }

        public PreparedStatement prepare(String stmt) {
            String withKeyspace = stmt.replaceAll(KEYSPACE_PATTERN, this.keyspace);
            String withReplication = withKeyspace.replaceAll(REPLICATION_STRATEGY_PATTERN, this.replicationStrategy);
            PreparedStatement prepared = session.prepare(withReplication);

            return prepared;
        }
    }
}
