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
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.*;

public class TableDef {
    private static final Logger logger = LoggerFactory.getLogger(TableDef.class);

    private AttributeDefinition partitionKey;
    private Optional<AttributeDefinition> clusteringKey = Optional.empty();

    private PreparedStatement jsonPutStatement;
    private PreparedStatement jsonQueryPartitionStatement;
    private PreparedStatement jsonQueryRowStatement;
    private PreparedStatement deleteStatement;
    private PreparedStatement queryRowStatement;

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
}
