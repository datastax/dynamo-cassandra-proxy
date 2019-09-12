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

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;

import java.util.HashMap;
import java.util.Map;

public class TableDef {
    private Map<String, DataType.Name> partitionKeyMap = new HashMap<String, DataType.Name>();
    private Map<String, DataType.Name> clusteringColumnMap = new HashMap<String, DataType.Name>();
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

    public Map<String, DataType.Name> getPartitionKeyMap() {
        return partitionKeyMap;
    }

    public void setPartitionKeyMap(Map<String, DataType.Name> partitionKeyMap) {
        this.partitionKeyMap = partitionKeyMap;
    }

    public Map<String, DataType.Name> getClusteringColumnMap() {
        return clusteringColumnMap;
    }

    public void setClusteringColumnMap(Map<String, DataType.Name> clusteringColumnMap) {
        this.clusteringColumnMap = clusteringColumnMap;
    }

    public PreparedStatement getJsonPutStatement() {
        return jsonPutStatement;
    }

    public void setJsonPutStatement(PreparedStatement jsonPutStatement) {
        this.jsonPutStatement = jsonPutStatement;
    }

    public void addPK(String colName, DataType.Name type) {
        this.partitionKeyMap.put(colName, type);
    }

    public void addClusteringColumn(String colName, DataType.Name type) {
        this.clusteringColumnMap.put(colName, type);
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
