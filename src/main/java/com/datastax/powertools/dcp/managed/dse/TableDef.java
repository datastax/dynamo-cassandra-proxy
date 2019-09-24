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

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.ASCII;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.BIGINT;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.BLOB;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.BOOLEAN;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.COUNTER;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.DATE;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.DECIMAL;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.DOUBLE;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.FLOAT;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.INET;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.INT;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.SMALLINT;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.TIME;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.TIMEUUID;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.TINYINT;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.UUID;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.VARCHAR;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.VARINT;

public class TableDef {
    private static final Logger logger = LoggerFactory.getLogger(TableDef.class);

    private AttributeDefinition partitionKey;
    private Optional<AttributeDefinition> clusteringKey = Optional.empty();

    private PreparedStatement jsonPutStatement;
    private PreparedStatement jsonQueryPartitionStatement;
    private PreparedStatement jsonQueryRowStatement;
    private PreparedStatement deleteStatement;
    private PreparedStatement queryRowStatement;
    private PreparedStatement jsonQueryPartitionAndClusteringStatement;
    private Map<ComparisonOperator, PreparedStatement> jsonQueryRangeStatementMap = new HashMap<>();
    private String keyspaceName;
    private CqlSession session;

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    private String tableName;

    public PreparedStatement getQueryRowStatement() {
        return queryRowStatement;
    }

    public PreparedStatement getJsonQueryRowStatement() {
        return jsonQueryRowStatement;
    }

    public void setJsonQueryRowStatement(PreparedStatement jsonQueryRowStatement) {
        this.jsonQueryRowStatement = jsonQueryRowStatement;
    }

    public AttributeDefinition getPartitionKey() {
        return partitionKey;
    }

    public Optional<AttributeDefinition> getClusteringKey() {
        return clusteringKey;
    }


    public PreparedStatement getJsonPutStatement() {
        return jsonPutStatement;
    }

    public void setJsonPutStatement(PreparedStatement jsonPutStatement) {
        this.jsonPutStatement = jsonPutStatement;
    }

    public void setPartitionKey(ColumnMetadata pk) {
        this.partitionKey = convertToAttribute(pk);
    }

    public void setClusteringKey(ColumnMetadata column) {
        this.clusteringKey = Optional.of(convertToAttribute(column));
    }

    private AttributeDefinition convertToAttribute(ColumnMetadata column)
    {
        AttributeDefinition ad = new AttributeDefinition();
        ad.setAttributeName(column.getName().asInternal());

        switch (column.getType().getProtocolCode())
        {
            case BLOB:
                ad.setAttributeType(ScalarAttributeType.B);
                break;
            case BIGINT:
            case BOOLEAN:
            case COUNTER:
            case DECIMAL:
            case DOUBLE:
            case FLOAT:
            case INT:
            case VARINT:
            case TINYINT:
            case SMALLINT:
                ad.setAttributeType(ScalarAttributeType.N);
                break;
            case TIMEUUID:
            case UUID:
            case INET:
            case DATE:
            case VARCHAR:
            case ASCII:
            case TIME:
                ad.setAttributeType(ScalarAttributeType.S);
                break;
            default:
                throw new IllegalArgumentException("Type not supported: " + column.getName().asInternal() + " " + column.getType());
        }

        return ad;
    }



    public PreparedStatement getJsonQueryPartitionStatement() {
        return jsonQueryPartitionStatement;
    }

    public void setJsonQueryPartitionStatement(PreparedStatement jsonQueryPartitionStatement) {
        this.jsonQueryPartitionStatement = jsonQueryPartitionStatement;
    }

    public PreparedStatement getDeleteStatement() {
        return deleteStatement;
    }

    public void setDeleteStatement(PreparedStatement deleteStatement) {
        this.deleteStatement = deleteStatement;
    }

    public void setQueryRowStatement(PreparedStatement queryRowStatement) {
        this.queryRowStatement = queryRowStatement;
    }

    public PreparedStatement getLazyJsonQueryPartitionAndClusteringStatement(ComparisonOperator comparisonOperator) {

        PreparedStatement preparedStatment = null;

        if (jsonQueryRangeStatementMap.containsKey(comparisonOperator)){
            preparedStatment = jsonQueryRangeStatementMap.get(comparisonOperator);
        }
        else{
            String clustering = "\"" + clusteringKey.get().getAttributeName() + "\"";
            String partition= "\"" + partitionKey.getAttributeName() + "\"";
            Select select = selectFrom(keyspaceName, tableName).all().whereColumn(partition).isEqualTo(bindMarker());
            switch (comparisonOperator) {
                case EQ:
                    preparedStatment = session.prepare(select.whereColumn(clustering).isEqualTo(bindMarker()).build());
                    break;
                case NE:
                    preparedStatment = session.prepare(select.whereColumn(clustering).isNotEqualTo(bindMarker()).build());
                    break;
                case IN:
                    preparedStatment = session.prepare(select.whereColumn(clustering).in(bindMarker()).build());
                    break;
                case LE:
                    preparedStatment = session.prepare(select.whereColumn(clustering).isLessThanOrEqualTo(bindMarker()).build());
                    break;
                case LT:
                    preparedStatment = session.prepare(select.whereColumn(clustering).isLessThan(bindMarker()).build());
                    break;
                case GE:
                    preparedStatment = session.prepare(select.whereColumn(clustering).isGreaterThanOrEqualTo(bindMarker()).build());
                    break;
                case GT:
                    preparedStatment = session.prepare(select.whereColumn(clustering).isGreaterThan(bindMarker()).build());
                    break;
                case BETWEEN:
                    preparedStatment = session.prepare(select
                            .whereColumn(clustering).isGreaterThanOrEqualTo(bindMarker())
                            .whereColumn(clustering).isLessThanOrEqualTo(bindMarker())
                            .build()
                    );
                    break;
                case NOT_NULL:
                    preparedStatment = session.prepare(select
                            .whereColumn(clustering).isNotNull()
                            .build()
                    );
                    break;
                case NULL:
                    throw new UnsupportedOperationException("CQL does not support null clustering columns");
                case CONTAINS:
                    throw new UnsupportedOperationException("Contains - feature unsupported");
                case NOT_CONTAINS:
                    throw new UnsupportedOperationException("Not Contains - feature unsupported");
                case BEGINS_WITH:
                    throw new UnsupportedOperationException("Begins With - feature unsupported");
            }
        }
        return preparedStatment;
    }

    public void setKeyspaceName(String keyspaceName) {
        this.keyspaceName = keyspaceName;
    }

    public void setSession(CqlSession session) {
        this.session = session;
    }
}
