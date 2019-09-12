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
package com.datastax.powertools.dcp.api;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;
import java.util.List;

public class NicerTableDescription extends AttributeDefinition {

    public NicerTableDescription() {
    }

    @JsonProperty("AttributeDefinition")
    private List<NicerAttributeDefinition> attributeDefinitions;

    @JsonProperty("TableName")
    private String tableName;

    @JsonProperty("KeySchemaElement")
    private List<NicerKeySchemaElement> keySchema;

    @JsonProperty("TableStatus")
    private String tableStatus;

    @JsonProperty("CreationDateTime")
    private Date creationDateTime;

    @JsonProperty("ProvisionedThroughputDescription")
    private NicerProvisionedThroughputDescription provisionedThroughput;

    @JsonProperty("TableSizeBytes")
    private Long tableSizeBytes;

    @JsonProperty("ItemCount")
    private Long itemCount;

    @JsonProperty("TableArn")
    private String tableArn;

    @JsonProperty("TableId")
    private String tableId;

    @JsonProperty("LocalSecondaryIndexDescription")
    private List<NicerLocalSecondaryIndexDescription> localSecondaryIndexes;

    @JsonProperty("GlobalSecondaryIndexDescription")
    private List<NicerGlobalSecondaryIndexDescription> globalSecondaryIndexes;

    @JsonProperty("StreamSpecification")
    private NicerStreamSpecification streamSpecification;

    @JsonProperty("LatestStreamLabel")
    private String latestStreamLabel;

    @JsonProperty("LatestStreamArn")
    private String latestStreamArn;

    @JsonProperty("RestoreSummary")
    private NicerRestoreSummary restoreSummary;

    @JsonProperty("SSEDescription")
    private NicerSSEDescription sSEDescription;

    public NicerTableDescription(TableDescription tableDescription) {
        setLatestStreamArn(tableDescription.getLatestStreamArn());
        setTableArn(tableDescription.getTableArn());
    }

    public List<NicerAttributeDefinition> getAttributeDefinitions() {
        return attributeDefinitions;
    }

    public void setAttributeDefinitions(List<NicerAttributeDefinition> attributeDefinitions) {
        this.attributeDefinitions = attributeDefinitions;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<NicerKeySchemaElement> getKeySchema() {
        return keySchema;
    }

    public void setKeySchema(List<NicerKeySchemaElement> keySchema) {
        this.keySchema = keySchema;
    }

    public String getTableStatus() {
        return tableStatus;
    }

    public void setTableStatus(String tableStatus) {
        this.tableStatus = tableStatus;
    }

    public Date getCreationDateTime() {
        return creationDateTime;
    }

    public void setCreationDateTime(Date creationDateTime) {
        this.creationDateTime = creationDateTime;
    }

    public NicerProvisionedThroughputDescription getProvisionedThroughput() {
        return provisionedThroughput;
    }

    public void setProvisionedThroughput(NicerProvisionedThroughputDescription provisionedThroughput) {
        this.provisionedThroughput = provisionedThroughput;
    }

    public Long getTableSizeBytes() {
        return tableSizeBytes;
    }

    public void setTableSizeBytes(Long tableSizeBytes) {
        this.tableSizeBytes = tableSizeBytes;
    }

    public Long getItemCount() {
        return itemCount;
    }

    public void setItemCount(Long itemCount) {
        this.itemCount = itemCount;
    }

    public String getTableArn() {
        return tableArn;
    }

    public void setTableArn(String tableArn) {
        this.tableArn = tableArn;
    }

    public String getTableId() {
        return tableId;
    }

    public void setTableId(String tableId) {
        this.tableId = tableId;
    }

    public List<NicerLocalSecondaryIndexDescription> getLocalSecondaryIndexes() {
        return localSecondaryIndexes;
    }

    public void setLocalSecondaryIndexes(List<NicerLocalSecondaryIndexDescription> localSecondaryIndexes) {
        this.localSecondaryIndexes = localSecondaryIndexes;
    }

    public List<NicerGlobalSecondaryIndexDescription> getGlobalSecondaryIndexes() {
        return globalSecondaryIndexes;
    }

    public void setGlobalSecondaryIndexes(List<NicerGlobalSecondaryIndexDescription> globalSecondaryIndexes) {
        this.globalSecondaryIndexes = globalSecondaryIndexes;
    }

    public NicerStreamSpecification getStreamSpecification() {
        return streamSpecification;
    }

    public void setStreamSpecification(NicerStreamSpecification streamSpecification) {
        this.streamSpecification = streamSpecification;
    }

    public String getLatestStreamLabel() {
        return latestStreamLabel;
    }

    public void setLatestStreamLabel(String latestStreamLabel) {
        this.latestStreamLabel = latestStreamLabel;
    }

    public String getLatestStreamArn() {
        return latestStreamArn;
    }

    public void setLatestStreamArn(String latestStreamArn) {
        this.latestStreamArn = latestStreamArn;
    }

    public NicerRestoreSummary getRestoreSummary() {
        return restoreSummary;
    }

    public void setRestoreSummary(NicerRestoreSummary restoreSummary) {
        this.restoreSummary = restoreSummary;
    }

    public NicerSSEDescription getsSEDescription() {
        return sSEDescription;
    }

    public void setsSEDescription(NicerSSEDescription sSEDescription) {
        this.sSEDescription = sSEDescription;
    }
}
