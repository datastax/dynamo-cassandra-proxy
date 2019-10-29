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
package com.datastax.powertools.dcp;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.powertools.dcp.api.DynamoDBResponse;
import com.datastax.powertools.dcp.managed.dse.CassandraManager;
import com.datastax.powertools.dcp.managed.dse.TableDef;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperFieldModel.DynamoDBAttributeType;
import static com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperFieldModel.DynamoDBAttributeType.valueOf;
import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.KeyType.RANGE;
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

/**
 * DynamoDb translator which encodes the document in a simple C* schema of (partition key, sort key, JSON string)
 */
public class DynamoDSETranslatorJSONBlob extends DynamoDSETranslator {
    private final static Logger logger = LoggerFactory.getLogger(DynamoDSETranslatorJSONBlob.class);
    //private static final Pattern queryPattern = Pattern.compile(".*(:\\S+)");
    private static final Pattern queryPattern = Pattern.compile("(\\S+)\\s?([?:=|<|>])\\s?:\\S+(?:\\s?\\S+\\s?(\\S+)\\s?(=|<|>)\\s?:\\S+)?");

    private static final Map<String, ComparisonOperator> operatorConverter = new HashMap()
    {{
            put("=", ComparisonOperator.EQ);
            put("!=", ComparisonOperator.NE);
            put("<=", ComparisonOperator.LE);
            put("<", ComparisonOperator.LT);
            put(">=", ComparisonOperator.GE);
            put(">", ComparisonOperator.GT);
    }};

    public DynamoDSETranslatorJSONBlob(CassandraManager cassandraManager) {
        super(cassandraManager);
    }

    @Override
    public DynamoDBResponse query(QueryRequest payload)
    {
        logger.debug("query against JSON table");
        ResultSet resultSet;

        if (payload.getKeyConditionExpression() != null)
            resultSet = queryByKeyExpression(payload);
        else if (payload.getKeyConditions() != null)
            resultSet = queryByKeyCondition(payload);
        else
            throw new UnsupportedOperationException("un-supported query type");

       try
        {
            Collection<Map<String, AttributeValue>> items = new HashSet<Map<String, AttributeValue>>();
            for (Row row : resultSet)
            {
                AttributeValue item;
                ColumnDefinitions colDefs = row.getColumnDefinitions();
                Map<String, AttributeValue> keysSet = new HashMap<>();
                for (ColumnDefinition colDef : colDefs)
                {
                    if (colDef.getName().asInternal().equals("json_blob"))
                        continue;

                    item = rowToAV(colDef, row);
                    keysSet.put(colDef.getName().asInternal(), item);
                }

                Map<String, AttributeValue> itemSet = blobToItemSet(row.getString("json_blob"));
                itemSet.putAll(keysSet);

                if (payload.getFilterExpression() != null){
                    if(!matchesFilterExpression(itemSet, payload)){
                        continue;
                    }
                }
                items.add(itemSet);
            }

            QueryResult queryResult = new QueryResult();
            queryResult.setItems(items);

            return new DynamoDBResponse(queryResult, 200);
        } catch (Throwable e) {
            logger.warn("Query error", e);

            DynamoDBResponse ddbResponse = new DynamoDBResponse(null, 500);
            String msg= String.format("query failed with error: %s", e.getMessage());
            ddbResponse.setError(msg);
            return ddbResponse;
        }
    }

    private boolean matchesFilterExpression(Map<String, AttributeValue> itemSet, QueryRequest payload) {

        String filterExpression = payload.getFilterExpression();
        Matcher matcher = queryPattern.matcher(filterExpression);
                if (matcher.find())
        {
            BoundStatement boundStatement = null;

            for (int i =0; i < matcher.groupCount();i = i+2) {
                Map<String, AttributeValue> expressionAtributeValues = payload.getExpressionAttributeValues();

                String attributeName = matcher.group(i + 1);
                AttributeValue itemAttributeValue = itemSet.get(attributeName);

                JsonNode valueJson = awsRequestMapper.valueToTree(itemAttributeValue);
                Comparable<Object> itemValue = (Comparable<Object>) getObjectFromJsonLeaf(valueJson.fields().next());

                AttributeValue expressionAttributeValue = expressionAtributeValues.get(attributeName);

                valueJson = awsRequestMapper.valueToTree(expressionAttributeValue);
                Comparable<Object> expressionValue = (Comparable<Object>) getObjectFromJsonLeaf(valueJson.fields().next());

                String operator = matcher.group(i + 2);

                switch (operator) {
                    case "=":
                        return itemValue.equals(expressionValue);
                    case "!=":
                        return !itemValue.equals(expressionValue);
                    case "<=":
                        return itemValue.compareTo(expressionValue) <= 0;
                    case "<":
                        return itemValue.compareTo(expressionValue) < 0;
                    case ">=":
                        return itemValue.compareTo(expressionValue) >= 0;
                    case ">":
                        return itemValue.compareTo(expressionValue) > 0;
                }
            }
        }
        throw new UnsupportedOperationException("Error parsing filter expression: " + filterExpression);

    }

    private ResultSet queryByKeyCondition(QueryRequest payload) {
        TableDef tableDef = cassandraManager.getTableDef(payload.getTableName());
        BoundStatement boundStatement = null;

        if (payload.getKeyConditions().size() ==1){
            for (Map.Entry<String, Condition> c : payload.getKeyConditions().entrySet()) {
                if (c.getKey().equals(tableDef.getPartitionKey().getAttributeName())) {
                    PreparedStatement jsonPartitionStatement = tableDef.getJsonQueryPartitionStatement();
                    if (!c.getValue().getComparisonOperator().equals(ComparisonOperator.EQ.name()))
                        throw new UnsupportedOperationException("Hash Key lookups only support equality conditions");


                    List<AttributeValue> v = c.getValue().getAttributeValueList();
                    JsonNode valueJson = awsRequestMapper.valueToTree(v.iterator().next());

                    Object value = getObjectFromJsonLeaf(valueJson.fields().next());

                    boundStatement = jsonPartitionStatement.bind(value);
                }
            }
        }
        if (payload.getKeyConditions().size() ==2) {

            PreparedStatement jsonPartitionAndClusteringStatement = null;

            Object partitionValue = null;
            Object clusteringValue = null;

            for (Map.Entry<String, Condition> c : payload.getKeyConditions().entrySet()) {
                if (c.getKey().equals(tableDef.getPartitionKey().getAttributeName())) {
                    PreparedStatement jsonPartitionStatement = tableDef.getJsonQueryPartitionStatement();
                    if (!c.getValue().getComparisonOperator().equals(ComparisonOperator.EQ.name()))
                        throw new UnsupportedOperationException("Hash Key lookups only support equality conditions");


                    List<AttributeValue> v = c.getValue().getAttributeValueList();
                    JsonNode valueJson = awsRequestMapper.valueToTree(v.iterator().next());

                    partitionValue = getObjectFromJsonLeaf(valueJson.fields().next());

                }

                if (c.getKey().equals(tableDef.getClusteringKey().get().getAttributeName())) {
                    if (c.getValue().getComparisonOperator() == null) {
                        throw new UnsupportedOperationException("null Comparison Operator not allowed");
                    }

                    List<AttributeValue> v = c.getValue().getAttributeValueList();
                    JsonNode valueJson = awsRequestMapper.valueToTree(v.iterator().next());

                    clusteringValue = getObjectFromJsonLeaf(valueJson.fields().next());

                    jsonPartitionAndClusteringStatement = tableDef.getLazyJsonQueryPartitionAndClusteringStatement(ComparisonOperator.valueOf(c.getValue().getComparisonOperator()));
                }
            }
            boundStatement = jsonPartitionAndClusteringStatement.bind(partitionValue, clusteringValue);
        }

        return session().execute(boundStatement);
    }

    private ResultSet queryByKeyExpression(QueryRequest payload) {

        TableDef tableDef = cassandraManager.getTableDef(payload.getTableName());
        PreparedStatement jsonStatement = tableDef.getJsonQueryPartitionStatement();

        Matcher matcher = queryPattern.matcher(payload.getKeyConditionExpression());
        if (matcher.find())
        {
            Map<String, AttributeValue> expressionAttributeValues = payload.getExpressionAttributeValues();
            BoundStatement boundStatement = null;

            //Partition Key
            if (matcher.groupCount() == 2) {
                for (Map.Entry<String, AttributeValue> stringAttributeValueEntry : expressionAttributeValues.entrySet()) {
                    if (!stringAttributeValueEntry.getKey().equals(tableDef.getPartitionKey().getAttributeName()))
                        continue;

                    JsonNode valueJson = awsRequestMapper.valueToTree(stringAttributeValueEntry.getValue());
                    Object value = getObjectFromJsonLeaf(valueJson.fields().next());

                    boundStatement = jsonStatement.bind(value);
                    break;
                }
            }
            //Partition Key and Clustering Columns
            if (matcher.groupCount() == 4) {
                PreparedStatement jsonPartitionAndClusteringStatement = null;

                Object partitionValue = null;
                Object clusteringValue = null;

                AttributeValue v = expressionAttributeValues.get(tableDef.getPartitionKey().getAttributeName());
                JsonNode valueJson = awsRequestMapper.valueToTree(v);
                partitionValue = getObjectFromJsonLeaf(valueJson.fields().next());

                v = expressionAttributeValues.get(tableDef.getClusteringKey().get().getAttributeName());
                valueJson = awsRequestMapper.valueToTree(v);
                clusteringValue = getObjectFromJsonLeaf(valueJson.fields().next());

                ComparisonOperator comparisonOperator;
                if (tableDef.getClusteringKey().get().getAttributeName().equals(matcher.group(1))) {
                    comparisonOperator = operatorConverter.get(matcher.group(2));
                } else if (tableDef.getClusteringKey().get().getAttributeName().equals(matcher.group(3))) {
                    comparisonOperator = operatorConverter.get(matcher.group(4));
                } else{
                    throw new UnsupportedOperationException("Invalid Expression Values");
                }
                jsonPartitionAndClusteringStatement = tableDef.getLazyJsonQueryPartitionAndClusteringStatement(comparisonOperator);
                boundStatement = jsonPartitionAndClusteringStatement.bind(partitionValue, clusteringValue);

            }
            return session().execute(boundStatement);
        }

        throw new UnsupportedOperationException("Error parsing expression: " + payload.getKeyConditionExpression());
    }


    private Map<String, AttributeValue> blobToItemSet(String json_blob) throws IOException
    {
        JsonNode items = awsRequestMapper.readTree(json_blob);

        Map<String, AttributeValue> itemSet = new HashMap<>();

        List<String> dynamoTypes = Arrays.asList("N", "S", "BOOL", "B", "S", "SS");
        Iterator<Map.Entry<String, JsonNode>> fieldIterator = items.fields();

        for (Iterator<Map.Entry<String, JsonNode>> it = fieldIterator; it.hasNext(); ) {
            Map.Entry<String, JsonNode> item = it.next();
            Iterator<Map.Entry<String, JsonNode>> itemFieldIterator = item.getValue().fields();
            for (Iterator<Map.Entry<String, JsonNode>> it2 = itemFieldIterator; it2.hasNext(); ) {
                Map.Entry<String, JsonNode> pair = it2.next();
                if (!dynamoTypes.contains(pair.getKey())) {
                    throw new UnsupportedOperationException("Nested not implemented");
                } else {
                    itemSet.put(item.getKey(), getAttributeFromJsonLeaf(item.getValue(), pair.getKey()));
                }
            }
        }
        return itemSet;
    }

    private AttributeValue getAttributeFromJsonLeaf(JsonNode values, String attributeType) {
        AttributeValue av = new AttributeValue();

        Iterator<Map.Entry<String, JsonNode>> it;
        for (it = values.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> leaf = it.next();
            ;
            JsonNodeType type = leaf.getValue().getNodeType();
            av = new AttributeValue();
            JsonNode value = leaf.getValue();

            //TODO: nested switch is not maintainable, do something prettier
            switch (type) {
                case ARRAY: {
                    Set set = new HashSet();
                    for (JsonNode jsonNode : value) {
                        jsonNode.getNodeType();
                        if (jsonNode.getNodeType().equals(JsonNodeType.STRING)) {
                            set.add(jsonNode.asText());
                        } else {
                            try {
                                set.add(jsonNode.binaryValue());
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    av.setSS(set);
                }
                break;
                case BINARY:
                    try {
                        av.setB(ByteBuffer.wrap(leaf.getValue().binaryValue()));
                    } catch (IOException e) {
                        throw new IOError(e);
                    }
                    break;
                case BOOLEAN:
                    av.setBOOL(value.asBoolean());
                    break;
                case MISSING:
                    break;
                case NULL:
                    break;
                case NUMBER:
                    av.setN(String.valueOf(value.asDouble()));
                    break;
                case OBJECT:
                    break;
                case POJO:
                    break;
                case STRING:
                    switch (ScalarAttributeType.fromValue(attributeType)) {
                        case S:
                            av.setS(value.asText());
                            break;
                        case N:
                            av.setN(value.asText());
                            break;
                       case B:
                            try {
                                av.setB(ByteBuffer.wrap(leaf.getValue().binaryValue()));
                            } catch (IOException e) {
                                throw new IOError(e);
                            }
                            break;
                    }
                    break;
            }
            ;
        }
        return av;

    }


    private AttributeValue rowToAV(ColumnDefinition columnDefinition, Row row) {
        AttributeValue av = new AttributeValue();
        CqlIdentifier name = columnDefinition.getName();
        switch (columnDefinition.getType().getProtocolCode())
        {
            case BLOB:
                av.setB(row.getByteBuffer(name));
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
                String v = String.valueOf(row.getDouble(name));
                if (v.endsWith(".0")) //Keep non doubles looking like non-doubles
                    v = v.substring(0, v.length() - 2);
                av.setN(v);
                break;
            case TIMEUUID:
            case UUID:
            case INET:
            case DATE:
            case VARCHAR:
            case ASCII:
            case TIME:
                av.setS(row.getString(name));
                break;
            default:
                throw new IllegalArgumentException("Type not supported: " + name.asInternal() + " " + columnDefinition.getType());
        }
        return av;
    }



    private Object getObjectFromJsonLeaf(Map.Entry<String, JsonNode> leaf) {
        DynamoDBAttributeType key = valueOf(leaf.getKey());
        JsonNode value = leaf.getValue();
        try {
            switch (key) {
                case N:
                    return value.asDouble();
                case S:
                    return value.asText();
                case BOOL:
                    return value.asBoolean();
                case B:
                    return value.binaryValue();
                default:
                    logger.error("Type not supported");
                    break;
            }
            return null;
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            return null;
        }

    }

    @Override
    public DynamoDBResponse deleteTable(DeleteTableRequest deleteTableRequest) {
        logger.info("deleting JSON table");

        String keyspace = keyspaceName;
        String table = deleteTableRequest.getTableName();
        String statement = String.format("DROP TABLE %s.\"%s\";\n", keyspace, table);
        ResultSet result = session().execute(statement);
        if (result.wasApplied()) {

            logger.info("deleted table " + table);
            cassandraManager.refreshSchema();

            TableDescription newTableDesc = this.getTableDescription(table, null,null);
            DeleteTableResult createResult = (new DeleteTableResult()).withTableDescription(newTableDesc);
            return new DynamoDBResponse(createResult, 200);
        }
        return null;
    }

    @Override
    public DynamoDBResponse createTable(CreateTableRequest createTableRequest) throws IOException {

        logger.info("creating JSON table");

        String columnPairs = createTableRequest.getAttributeDefinitions().stream().map(this::attributeToPairs).collect(Collectors.joining(", "));
        columnPairs += ",json_blob text";
        String keyspace = keyspaceName;
        String table = createTableRequest.getTableName();
        String primaryKey = getPrimaryKey(createTableRequest.getKeySchema());
        String statement = String.format("CREATE TABLE IF NOT EXISTS %s.\"%s\" ( %s, PRIMARY KEY %s);\n", keyspace, table, columnPairs, primaryKey);
        ResultSet result = session().execute(statement);
        if (result.wasApplied()) {

            logger.info("created {} as {}", table, statement);

            cassandraManager.refreshSchema();

            TableDescription newTableDesc = this.getTableDescription(table, createTableRequest.getAttributeDefinitions(), createTableRequest.getKeySchema());
            CreateTableResult createResult = (new CreateTableResult()).withTableDescription(newTableDesc);
            return new DynamoDBResponse(createResult, 200);
        }
        return null;
    }

    public String getPrimaryKey(List<KeySchemaElement> keySchema){
        String partitionKey = null;
        String clusteringColumn = null;

        for (KeySchemaElement keySchemaElement : keySchema) {
            String type = keySchemaElement.getKeyType();
            String name = keySchemaElement.getAttributeName();
            if (type.equals(HASH.toString()))
                partitionKey = name;

            else if (type.equals(RANGE.toString()))
                clusteringColumn = name;
        }
        String primaryKey;
        if(clusteringColumn != null)
            primaryKey = String.format("((\"%s\"), \"%s\")", partitionKey, clusteringColumn);
        else
            primaryKey = String.format("(\"%s\")", partitionKey);

        return primaryKey;
    }

    private String attributeToPairs(AttributeDefinition attributeDefinition) {
        String name = "\"" + attributeDefinition.getAttributeName() + "\"";
        DynamoDBAttributeType type = valueOf(attributeDefinition.getAttributeType());
        switch(type) {
            case N:
                return name + " double";
            case S:
                return name + " text";
            case BOOL:
                return name + " boolean";
            case B:
                return name + " blob";
            default:
                throw new RuntimeException("Type not supported");
        }
    }

    @Override
    public DynamoDBResponse describeTable(DescribeTableRequest describeTableRequest) {
        String tableName = describeTableRequest.getTableName();
        if (cassandraManager.hasTable(tableName)){
            TableDef tableDef = cassandraManager.getTableDef(tableName);

            AttributeDefinition partitionKey = tableDef.getPartitionKey();
            Optional<AttributeDefinition> maybeClusteringKey = tableDef.getClusteringKey();

            TableDescription tableDescription = new TableDescription()
                    .withTableName(tableName)
                    .withTableStatus(TableStatus.ACTIVE)
                    .withCreationDateTime(new Date())
                    .withTableArn(tableName);

            if (maybeClusteringKey.isPresent()) {
                AttributeDefinition clusteringKey = maybeClusteringKey.get();
                tableDescription.setAttributeDefinitions(ImmutableList.of(partitionKey, clusteringKey));
                tableDescription.setKeySchema(
                        ImmutableList.of(
                                new KeySchemaElement(partitionKey.getAttributeName(), KeyType.HASH),
                                new KeySchemaElement(clusteringKey.getAttributeName(), KeyType.RANGE))
                );
            } else {
                tableDescription.setAttributeDefinitions(ImmutableList.of(partitionKey));
                tableDescription.setKeySchema(
                        ImmutableList.of(new KeySchemaElement(partitionKey.getAttributeName(), KeyType.HASH))
                );
            }

            DescribeTableResult dtr = new DescribeTableResult().withTable(tableDescription);
            DynamoDBResponse ddbr = new DynamoDBResponse(dtr, 200);

            return ddbr;

        }else{
            DynamoDBResponse ddbr = new DynamoDBResponse(null, 500);
            ddbr.setError("Table not found");
            return ddbr;
        }
    }

    private TableDescription getTableDescription(String tableName, Collection<AttributeDefinition> attributeDefinitions, Collection<KeySchemaElement> keySchema) {
        TableDescription tableDescription = (new TableDescription())
                .withTableName(tableName)
                .withAttributeDefinitions(attributeDefinitions)
                .withKeySchema(keySchema)
                .withTableStatus(TableStatus.ACTIVE)
                .withCreationDateTime(new Date())
                .withTableArn(tableName);

        return tableDescription;
    }

    @Override
    public DynamoDBResponse deleteItem(DeleteItemRequest dir) {
        logger.debug("delete item into JSON table");
        String tableName = dir.getTableName();
        TableDef tableDef = cassandraManager.getTableDef(tableName);

        PreparedStatement deleteStatement = tableDef.getDeleteStatement();

        AttributeDefinition partitionKeyAttr = tableDef.getPartitionKey();
        Optional<AttributeDefinition> maybeCusteringKeyAttr = tableDef.getClusteringKey();

        Map<String, AttributeValue> keys = dir.getKey();

        Object partitionKeyValue = getAttributeObject(
                ScalarAttributeType.fromValue(partitionKeyAttr.getAttributeType()),
                keys.get(partitionKeyAttr.getAttributeName())
        );

        BoundStatement boundStatement;

        if (maybeCusteringKeyAttr.isPresent())
        {
            Object clusteringKeyValue = getAttributeObject(
                    ScalarAttributeType.fromValue(maybeCusteringKeyAttr.get().getAttributeType()),
                    keys.get(maybeCusteringKeyAttr.get().getAttributeName())
            );

            boundStatement = deleteStatement.bind(partitionKeyValue, clusteringKeyValue);
        }
        else
        {
            boundStatement = deleteStatement.bind(partitionKeyValue);
        }

        ResultSet result = session().execute(boundStatement);

        if (result.wasApplied()){
            DeleteItemResult dres = new DeleteItemResult();
            return new DynamoDBResponse(dres, 200);
        }
        else return null;

    }

    private Object getAttributeObject(ScalarAttributeType type, AttributeValue value) {
        //Note: only string, number, and binary are allowed for primary keys in dynamodb
        switch (type) {
            case N:
                return Double.parseDouble(value.getN());
            case S:
                return value.getS();
            case B:
                return value.getB();
            default:
                throw new IllegalStateException("Unknown dynamo scalar type: " + type);
        }
    }

    @Override
    public DynamoDBResponse putItem(PutItemRequest putItemRequest) {
        PutItemResult pir = new PutItemResult();
        logger.debug("put item into JSON table");

        TableDef tableDef = cassandraManager.getTableDef(putItemRequest.getTableName());
        PreparedStatement jsonStatement = tableDef.getJsonPutStatement();

        if (jsonStatement == null) {
            String msg= String.format("Requested resource not found: Table: %s not found", putItemRequest.getTableName());
            DynamoDBResponse ddbResponse = new DynamoDBResponse(pir, 400);
            ddbResponse.setError(msg);
            logger.error(msg);
            return ddbResponse;
        }

        Map<String, AttributeValue> items = putItemRequest.getItem();
        //TODO: there may be a cleaner way to to this using Marshal
        JsonNode itemsJson = awsRequestMapper.valueToTree(items);
        itemsJson = organizeColumns(itemsJson, tableDef.getPartitionKey(), tableDef.getClusteringKey());

        String jsonColumns = stringify(itemsJson);

        BoundStatement boundStatement = jsonStatement.bind(jsonColumns);

        try {
            ResultSet result = session().execute(boundStatement);
            if (result.wasApplied()){
                DynamoDBResponse ddbResponse = new DynamoDBResponse(pir, 200);
                return ddbResponse;
            }
            else {
                DynamoDBResponse ddbResponse = new DynamoDBResponse(pir, 500);
                String msg = String.format("PutItem not applied", putItemRequest.getTableName());
                ddbResponse.setError(msg);
                return ddbResponse;
            }
        }catch (Exception e){
            DynamoDBResponse ddbResponse = new DynamoDBResponse(pir, 500);
            String msg= String.format("PutItem write failed with error: %s",e.getMessage());
            ddbResponse.setError(msg);
            return ddbResponse;
        }
    }

    @Override
    public DynamoDBResponse getItem(GetItemRequest getItemRequest) {
        logger.debug("get item from JSON table");

        String tableName = getItemRequest.getTableName();
        TableDef tableDef = cassandraManager.getTableDef(tableName);
        PreparedStatement selectStatement = tableDef.getQueryRowStatement();

        AttributeDefinition partitionKeyDef = tableDef.getPartitionKey();
        Optional<AttributeDefinition> clusteringKeyDef = tableDef.getClusteringKey();

        Map<String, AttributeValue> keys = getItemRequest.getKey();

        AttributeValue partitionKey = keys.get(partitionKeyDef.getAttributeName());
        AttributeValue clusteringKey = clusteringKeyDef.isPresent() ?
                keys.get(clusteringKeyDef.get().getAttributeName()) : null;

        ScalarAttributeType partitionKeyType = ScalarAttributeType.valueOf(partitionKeyDef.getAttributeType());
        ScalarAttributeType clusteringKeyType = clusteringKeyDef.isPresent() ?
                ScalarAttributeType.valueOf(clusteringKeyDef.get().getAttributeType()) : null;

        BoundStatement boundStatement = clusteringKey == null ?
                selectStatement.bind(getAttributeObject(partitionKeyType, partitionKey)) :
                selectStatement.bind(getAttributeObject(partitionKeyType, partitionKey),
                        getAttributeObject(clusteringKeyType, clusteringKey));

        ResultSet result = session().execute(boundStatement);

        GetItemResult gir = new GetItemResult();
        Map<String, AttributeValue> item = new HashMap<>();
        ColumnDefinitions colDefs = result.getColumnDefinitions();

        Row row = result.one();

        //Case that nothing is found
        if (row == null)
            return new DynamoDBResponse(null, 200);

        Map<String, AttributeValue> keysSet = new HashMap<>();
        for (ColumnDefinition colDef : colDefs)
        {
            if (colDef.getName().asInternal().equals("json_blob"))
                continue;

            keysSet.put(colDef.getName().asInternal(), rowToAV(colDef, row));
        }

        try
        {
            item = blobToItemSet(row.getString("json_blob"));
            item.putAll(keysSet);

            gir.withItem(item);
            return new DynamoDBResponse(gir, 200);
        } catch (IOException e) {
            DynamoDBResponse ddbResponse = new DynamoDBResponse(gir, 500);
            String msg = String.format("GetItem failed", getItemRequest.getTableName());
            ddbResponse.setError(msg);
            return ddbResponse;
        }
    }

    private CqlSession session() {
        return cassandraManager.getSession();
    }

    private String stringify(JsonNode items) {
        ObjectNode itemsClone =  (ObjectNode) items.deepCopy();
        String jsonString = items.get("json_blob").toString();
        itemsClone.remove("json_blob");
        itemsClone.put("json_blob", jsonString);
        return itemsClone.toString();
    }

    private ObjectNode organizeColumns(JsonNode items, AttributeDefinition partitionKey, Optional<AttributeDefinition> clusteringKey) {

        ObjectNode itemsClone = new ObjectNode(JsonNodeFactory.instance);
        ObjectNode jsonBlobNode = new ObjectNode(JsonNodeFactory.instance);
        for (Iterator<Map.Entry<String, JsonNode>> it = items.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> item = it.next();
            String itemKey = item.getKey();
            if ( partitionKey.getAttributeName().equals(itemKey) ||
                    (clusteringKey.isPresent() && clusteringKey.get().getAttributeName().equals(itemKey)))
                itemsClone.put("\"" + itemKey + "\"", stripDynamoTypes(item.getValue()));
            else
                jsonBlobNode.put(itemKey, item.getValue());

        }

        itemsClone.put("json_blob", items);

        return itemsClone;
    }




    public JsonNode stripDynamoTypes(JsonNode items) {
        JsonNodeType type = items.getNodeType();
        Set<String> dynamoTypes = Sets.newHashSet("N", "S", "BOOL", "B", "SS", "NS");
        switch (type) {
            case OBJECT: {
                Iterator<Map.Entry<String, JsonNode>> fieldIterator = items.fields();

                ObjectNode jsonObjectNode = new ObjectNode(JsonNodeFactory.instance);
                for (Iterator<Map.Entry<String, JsonNode>> it = fieldIterator; it.hasNext(); ) {
                    Map.Entry<String, JsonNode> item = it.next();
                    if (!dynamoTypes.contains(item.getKey())) {
                        jsonObjectNode.put(item.getKey(), stripDynamoTypes(item.getValue()));
                    }else {
                        JsonNode coerced = coerceDynamoTypes(item.getValue(), item.getKey());
                        return coerced;
                    }
                }
                return jsonObjectNode;
            }
            case ARRAY: {
                ArrayNode item = (ArrayNode) items;
                ArrayNode jsonArrayNode = new ArrayNode(JsonNodeFactory.instance);
                for (JsonNode jsonNode : item) {
                    jsonArrayNode.add(stripDynamoTypes(jsonNode));
                }
                return jsonArrayNode;
            }
            default:
                return items;
        }
    }

    private JsonNode coerceDynamoTypes(JsonNode value, String key) {
        try {
            DynamoDBAttributeType keyType = valueOf(key);
            switch (keyType) {
                case N:
                    return awsRequestMapper.valueToTree(value.asDouble());
                case S:
                    return awsRequestMapper.valueToTree(value.asText());
                case BOOL:
                    return awsRequestMapper.valueToTree(value.asBoolean());
                case B:
                    return awsRequestMapper.valueToTree(value.binaryValue());
                case SS:
                    return stripDynamoTypes(value);
                default:
                    logger.error("Type not supported");
                    break;
            }
            return null;
        }catch(Exception e){
            e.printStackTrace();
            logger.error(e.getMessage());
            return null;
        }
    }
}
