package com.datastax.powertools.dcp.managed.dse;

/*
 *
 * @author Sebastián Estévez on 12/17/18.
 *
 */


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
