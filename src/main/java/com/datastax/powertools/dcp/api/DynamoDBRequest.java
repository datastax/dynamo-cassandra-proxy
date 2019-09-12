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
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.KeyType.RANGE;

public class DynamoDBRequest {
    private String tableName;

    private JsonNode item;
    private List<NicerAttributeDefinition> attributeDefinitions;
    private List<NicerKeySchemaElement> keySchema;
    private ProvisionedThroughput provisionedThroughput;
    private String keyConditionExpression;
    private JsonNode keyConditions;
    private JsonNode expressionAttributeValues;
    private Logger logger = LoggerFactory.getLogger(DynamoDBRequest.class);

    public DynamoDBRequest() {
    }

    @JsonProperty("TableName")
    public String getTableName() {
        return tableName;
    }

    @JsonProperty("TableName")
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @JsonProperty("Item")
    public JsonNode getItem() {
        return item;
    }

    @JsonProperty("Item")
    public void setItem(JsonNode item) {
        this.item = item;
    }

    @JsonProperty("AttributeDefinitions")
    public List<NicerAttributeDefinition> getAttributeDefinitions() {
        return attributeDefinitions;
    }

    /*
    public Map<String, AttributeValue> getAttributeMap() {
        Map<String, AttributeValue> attributeMap = new HashMap<>();
        for (NicerAttributeDefinition attributeDefinition : attributeDefinitions) {
            String key = attributeDefinition.getAttributeName();
            AttributeDefinition value = attributeDefinition.getAttributeDefinition();
            attributeMap.put(key, value);
        }
        return attributeMap;
    }
    */

    public List<AttributeDefinition> getAWSAttributeDefinitions() {
        List<AttributeDefinition> awsAttributeDefinitions = attributeDefinitions.stream().map((x) -> x.getAttributeDefinition()).collect(Collectors.toList());
        return awsAttributeDefinitions;
    }

    @JsonProperty("AttributeDefinitions")
    public void setAttributeDefinitions(List<NicerAttributeDefinition> attributeDefinitions) {
        this.attributeDefinitions = attributeDefinitions;
    }

    @JsonProperty("KeySchema")
    public List<NicerKeySchemaElement> getKeySchema() {
        return keySchema;
    }

    @JsonProperty("KeySchema")
    public void setKeySchema(List<NicerKeySchemaElement> keySchema) {
        this.keySchema = keySchema;
    }

    @JsonProperty("ProvisionedThroughput")
    public ProvisionedThroughput getProvisionedThroughput() {
        return provisionedThroughput;
    }

    @JsonProperty("ProvisionedThroughput")
    public void setProvisionedThroughput(ProvisionedThroughput provisionedThroughput) {
        this.provisionedThroughput = provisionedThroughput;
    }

    @JsonProperty("KeyConditions")
    public JsonNode getKeyConditions() {
        return keyConditions;
    }

    @JsonProperty("KeyConditions")
    public void setKeyConditions(JsonNode keyConditions) {
        this.keyConditions = keyConditions;
    }

    @JsonProperty("KeyConditionExpression")
    public String getKeyConditionExpression() {
        return keyConditionExpression;
    }

    @JsonProperty("KeyConditionExpression")
    public void setKeyConditionExpression(String keyConditions) {
        this.keyConditionExpression = keyConditions;
    }

    public String getColumnPairs() {
        String columnPairs = attributeDefinitions.stream().map(this::attributeToPairs).collect(Collectors.joining(","));
        return columnPairs;

    }

    private String attributeToPairs(AttributeDefinition attributeDefinition) {
        String pair = "";
        switch(attributeDefinition.getAttributeType()) {
            case "N": pair = attributeDefinition.getAttributeName() + " double";
                break;
            case "S": pair = attributeDefinition.getAttributeName() + " text";
                break;
            case "BOOL": pair = attributeDefinition.getAttributeName() + " boolean";
                break;
            case "B": pair = attributeDefinition.getAttributeName() + " blob";
                break;
            default: logger.error("Type not supported");
                break;

        }
        return pair;
    }

    public String getPrimaryKey() {
        String partitionKey= "";
        String clusteringColumns= "";
        for (NicerKeySchemaElement keySchemaElement: keySchema) {
            String type = keySchemaElement.getKeyType();
            String name = keySchemaElement.getAttributeName();
            if (type.equals(HASH.toString())){
                partitionKey += " " + name + ",";
            }
            if (type.equals(RANGE.toString())) {
                clusteringColumns += " " + name + ",";
            }
        }
        partitionKey = partitionKey.substring(0, partitionKey.length()-1);
        clusteringColumns= clusteringColumns.substring(0, clusteringColumns.length()-1);

        String primaryKey = "";
        if (clusteringColumns.length() >0) {
            primaryKey = String.format("((%s), %s)", partitionKey, clusteringColumns);
        }else{
            primaryKey = String.format("((%s))", partitionKey);
        }
        return primaryKey;
    }

    @JsonProperty("ExpressionAttributeValues")
    public JsonNode getExpressionAttributeValues() {
        return expressionAttributeValues;
    }

    @JsonProperty("ExpressionAttributeValues")
    public void setExpressionAttributeValues(JsonNode expressionAttributeValues) {
        this.expressionAttributeValues = expressionAttributeValues;
    }

    public List<KeySchemaElement> getAWSKeySchema() {
        List<KeySchemaElement> awsKeySchema = this.keySchema.stream().map(x -> x.getAWSKeySchema()).collect(Collectors.toList());
        return awsKeySchema;
    }
}
