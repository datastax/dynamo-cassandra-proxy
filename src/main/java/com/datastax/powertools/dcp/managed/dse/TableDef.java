package com.datastax.powertools.dcp.managed.dse;

/*
 *
 * @author Sebastián Estévez on 12/17/18.
 *
 */


import com.datastax.driver.core.PreparedStatement;

import java.util.ArrayList;
import java.util.List;

public class TableDef {
    private List<String> partitionKeys = new ArrayList<>();
    private List<String> clusteringColumns = new ArrayList<>();
    private PreparedStatement jsonPutStatement;
    private PreparedStatement jsonQueryStatement;

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public void setPartitionKeys(List<String> partitionKeys) {
        this.partitionKeys = partitionKeys;
    }

    public List<String> getClusteringColumns() {
        return clusteringColumns;
    }

    public void setClusteringColumns(List<String> clusteringColumns) {
        this.clusteringColumns = clusteringColumns;
    }

    public PreparedStatement getJsonPutStatement() {
        return jsonPutStatement;
    }

    public void setJsonPutStatement(PreparedStatement jsonPutStatement) {
        this.jsonPutStatement = jsonPutStatement;
    }

    public void addPK(String colName) {
        this.partitionKeys.add(colName);
    }

    public void addClusteringColumn(String colName) {
        this.clusteringColumns.add(colName);
    }

    public PreparedStatement getJsonQueryStatement() {
        return jsonQueryStatement;
    }

    public void setJsonQueryStatement(PreparedStatement jsonQueryStatement) {
        this.jsonQueryStatement = jsonQueryStatement;
    }
}
