package com.datastax.powertools.dcp.api;

/*
 *
 * @author Sebastián Estévez on 1/24/19.
 *
 */


import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class NicerGlobalSecondaryIndexDescription {
    public NicerGlobalSecondaryIndexDescription() {
    }

    @JsonProperty("IndexName")
    private String indexName;
    @JsonProperty("KeySchemaElement")
    private List<NicerKeySchemaElement> keySchema;
    @JsonProperty("Projection")
    private NicerProjection projection;
    @JsonProperty("IndexStatus")
    private String indexStatus;
    @JsonProperty("Backfilling")
    private Boolean backfilling;
    @JsonProperty("ProvisionedThroughputDescription")
    private NicerProvisionedThroughputDescription provisionedThroughput;
    @JsonProperty("IndexSizeBytes")
    private Long indexSizeBytes;
    @JsonProperty("ItemCount")
    private Long itemCount;
    @JsonProperty("IndexArn")
    private String indexArn;

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public List<NicerKeySchemaElement> getKeySchema() {
        return keySchema;
    }

    public void setKeySchema(List<NicerKeySchemaElement> keySchema) {
        this.keySchema = keySchema;
    }

    public NicerProjection getProjection() {
        return projection;
    }

    public void setProjection(NicerProjection projection) {
        this.projection = projection;
    }

    public String getIndexStatus() {
        return indexStatus;
    }

    public void setIndexStatus(String indexStatus) {
        this.indexStatus = indexStatus;
    }

    public Boolean getBackfilling() {
        return backfilling;
    }

    public void setBackfilling(Boolean backfilling) {
        this.backfilling = backfilling;
    }

    public NicerProvisionedThroughputDescription getProvisionedThroughput() {
        return provisionedThroughput;
    }

    public void setProvisionedThroughput(NicerProvisionedThroughputDescription provisionedThroughput) {
        this.provisionedThroughput = provisionedThroughput;
    }

    public Long getIndexSizeBytes() {
        return indexSizeBytes;
    }

    public void setIndexSizeBytes(Long indexSizeBytes) {
        this.indexSizeBytes = indexSizeBytes;
    }

    public Long getItemCount() {
        return itemCount;
    }

    public void setItemCount(Long itemCount) {
        this.itemCount = itemCount;
    }

    public String getIndexArn() {
        return indexArn;
    }

    public void setIndexArn(String indexArn) {
        this.indexArn = indexArn;
    }
}
