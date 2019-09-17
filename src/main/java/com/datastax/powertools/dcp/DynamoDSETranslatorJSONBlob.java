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
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.powertools.dcp.api.DynamoDBResponse;
import com.datastax.powertools.dcp.managed.dse.DatastaxManager;
import com.datastax.powertools.dcp.managed.dse.TableDef;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;
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
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperFieldModel.DynamoDBAttributeType;
import static com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperFieldModel.DynamoDBAttributeType.valueOf;
import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.KeyType.RANGE;

public class DynamoDSETranslatorJSONBlob extends DynamoDSETranslator {
    private final static Logger logger = LoggerFactory.getLogger(DynamoDSETranslatorJSONBlob.class);
    private final String keyspaceName;
    private final DatastaxManager datastaxManager;
    private final ObjectMapper mapper;
    private Session session;

    public DynamoDSETranslatorJSONBlob(DatastaxManager datastaxManager) {
        super(datastaxManager);
        this.keyspaceName = super.getKeyspaceName();
        this.datastaxManager = datastaxManager;
        mapper = new ObjectMapper()
            .setPropertyNamingStrategy(PropertyNamingStrategy.UPPER_CAMEL_CASE)
            .setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    @Override
    public DynamoDBResponse query(QueryRequest payload) {
        Pattern pattern = Pattern.compile(".*(:\\S+)");
        logger.debug("query against JSON table");

        session = cacheAndOrGetCachedSession();

        PreparedStatement jsonStatement = datastaxManager.getQueryStatement(payload.getTableName());

        // TODO: also handle KeyConditions and other query options
        Matcher matcher = pattern.matcher(payload.getKeyConditionExpression());

        if (matcher.find()) {
            String keyAlias = matcher.group(1);
            try {
                Map<String, AttributeValue> expressionAttributeValues = payload.getExpressionAttributeValues();
                BoundStatement boundStatement = null;
                for (Map.Entry<String, AttributeValue> stringAttributeValueEntry : expressionAttributeValues.entrySet()) {

                    JsonNode valueJson = mapper.valueToTree(stringAttributeValueEntry.getValue());
                    Object value = getObjectFromJsonLeaf(valueJson.fields().next());

                    boundStatement = jsonStatement.bind(value);
                }
                ResultSet result = session.execute(boundStatement);

                Collection<Map<String, AttributeValue>> items = new HashSet<Map<String, AttributeValue>>();
                for (Row row : result) {
                    AttributeValue item;
                    ColumnDefinitions colDefs = row.getColumnDefinitions();
                    Map<String, AttributeValue> itemSet = new HashMap<>();
                    for (ColumnDefinitions.Definition colDef : colDefs) {
                        String name = colDef.getName();
                        item = colToAttributeValue(colDef, row);
                        itemSet.put(name, item);
                    }
                    itemSet = blobToItemSet(row.getString("json_blob"));
                    items.add(itemSet);
                }
                QueryResult queryResult = new QueryResult();
                queryResult.setItems(items);
                return new DynamoDBResponse(queryResult, 200);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }
        return null;
    }

    private Map<String, AttributeValue> blobToItemSet(String json_blob) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode items = mapper.readTree(json_blob);

        Map<String, AttributeValue> itemSet = new HashMap<>();

        List<String> dynamoTypes = Arrays.asList("N", "S", "BOOL", "B", "S", "SS");
        Iterator<Map.Entry<String, JsonNode>> fieldIterator = items.fields();

        for (Iterator<Map.Entry<String, JsonNode>> it = fieldIterator; it.hasNext(); ) {
            Map.Entry<String, JsonNode> item = it.next();
            Iterator<Map.Entry<String, JsonNode>> itemFieldIterator = item.getValue().fields();
            for (Iterator<Map.Entry<String, JsonNode>> it2 = itemFieldIterator; it2.hasNext(); ) {
                Map.Entry<String, JsonNode> pair = it2.next();
                if (!dynamoTypes.contains(pair.getKey())) {
                    throw new Exception("Nested not implemented");
                } else {
                    itemSet.put(item.getKey(), getAttributeFromJsonLeaf(item.getValue()));
                }
            }
        }
        return itemSet;
    }

    private AttributeValue getAttributeFromJsonLeaf(JsonNode values) {
        AttributeValue av = new AttributeValue();

        Iterator<Map.Entry<String, JsonNode>> it;
        for (it = values.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> leaf = it.next();
            ;
            JsonNodeType type = leaf.getValue().getNodeType();
            av = new AttributeValue();
            JsonNode value = leaf.getValue();
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
                        e.printStackTrace();
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
                    av.setS(value.asText());
                    break;
            }
            ;
        }
        return av;

    }

    private AttributeValue colToAttributeValue(ColumnDefinitions.Definition colDef, Row row) {
        DataType.Name type = colDef.getType().getName();
        String name = colDef.getName();
        AttributeValue av = rowToAV(name, type, row);
        return av;
    }

    private AttributeValue rowToAV(String name, DataType.Name type, Row row) {
        AttributeValue av = new AttributeValue();
        switch (type) {
            case CUSTOM:
                break;
            case ASCII:
                break;
            case BIGINT:
                av.setN(String.valueOf(row.getInt(name)));
                break;
            case BLOB:
                av.setB(row.getBytes(name));
                break;
            case BOOLEAN:
                av.setBOOL(row.getBool(name));
                break;
            case COUNTER:
                break;
            case DECIMAL:
                break;
            case DOUBLE:
                av.setN(String.valueOf(row.getDouble(name)));
                break;
            case FLOAT:
                av.setN(String.valueOf(row.getFloat(name)));
                break;
            case INT:
                av.setN(String.valueOf(row.getInt(name)));
                break;
            case TEXT:
                av.setS(row.getString(name));
                break;
            case TIMESTAMP:
                av.setS(String.valueOf(row.getTimestamp(name)));
                break;
            case UUID:
                break;
            case VARCHAR:
                av.setS(String.valueOf(row.getString(name)));
                break;
            case VARINT:
                av.setN(String.valueOf(row.getVarint(name)));
                break;
            case TIMEUUID:
                av.setS(String.valueOf(row.getUUID(name)));
                break;
            case INET:
                break;
            case DATE:
                av.setS(String.valueOf(row.getDate(name)));
                break;
            case TIME:
                av.setS(String.valueOf(row.getTime(name)));
                break;
            case SMALLINT:
                av.setN(String.valueOf(row.getInt(name)));
                break;
            case TINYINT:
                av.setN(String.valueOf(row.getInt(name)));
                break;
            case DURATION:
                break;
            case LIST:
                break;
            case MAP:
                break;
            case SET:
                break;
            case UDT:
                break;
            case TUPLE:
                break;
            default:
                logger.error("Type not supported");
                break;
        }
        if (av == null) {
            logger.error("Type not supported");
            return null;
        }
        return av;
    }

    private AttributeDefinition cqlToAD(String name, DataType.Name type) {
        if (name == null | type == null) {
            throw new WebApplicationException("Invalid table");
        }
        AttributeDefinition ad = new AttributeDefinition();
        ad.setAttributeName(name);
        switch (type) {
            case CUSTOM:
                break;
            case ASCII:
                break;
            case BIGINT:
                ad.setAttributeType(ScalarAttributeType.N);
                break;
            case BLOB:
                ad.setAttributeType(ScalarAttributeType.B);
                break;
            case BOOLEAN:
                ad.setAttributeType(ScalarAttributeType.N);
                break;
            case COUNTER:
                break;
            case DECIMAL:
                break;
            case DOUBLE:
                ad.setAttributeType(ScalarAttributeType.N);
                break;
            case FLOAT:
                ad.setAttributeType(ScalarAttributeType.N);
                break;
            case INT:
                ad.setAttributeType(ScalarAttributeType.N);
                break;
            case TEXT:
                ad.setAttributeType(ScalarAttributeType.N);
                break;
            case TIMESTAMP:
                ad.setAttributeType(ScalarAttributeType.N);
                break;
            case UUID:
                break;
            case VARCHAR:
                ad.setAttributeType(ScalarAttributeType.S);
                break;
            case VARINT:
                ad.setAttributeType(ScalarAttributeType.N);
                break;
            case TIMEUUID:
                ad.setAttributeType(ScalarAttributeType.S);
                break;
            case INET:
                break;
            case DATE:
                ad.setAttributeType(ScalarAttributeType.S);
                break;
            case TIME:
                ad.setAttributeType(ScalarAttributeType.S);
                break;
            case SMALLINT:
                ad.setAttributeType(ScalarAttributeType.N);
                break;
            case TINYINT:
                ad.setAttributeType(ScalarAttributeType.N);
                break;
            case DURATION:
                break;
            case LIST:
                break;
            case MAP:
                break;
            case SET:
                break;
            case UDT:
                break;
            case TUPLE:
                break;
            default:
                logger.error("Type not supported");
                return null;
        }
        if (ad == null) {
            logger.error("Type not supported");
            return null;
        }
        return ad;
    }

    private Object getKeyFromExpression(String keyAlias, ObjectNode expressionAttributeValues) throws Exception {
        for (Iterator<Map.Entry<String, JsonNode>> it = expressionAttributeValues.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> item = it.next();
            if (item.getKey().equals(keyAlias)) {
                JsonNode leaves = item.getValue();
                for (Iterator<Map.Entry<String, JsonNode>> it2 = leaves.fields(); it2.hasNext(); ) {
                    Map.Entry<String, JsonNode> leaf = it2.next();
                    return getObjectFromJsonLeaf(leaf);
                }
            }
        }
        throw new Exception("Invalid ExpressionAttributes");
    }

    private Object getObjectFromJsonLeaf(Map.Entry<String, JsonNode> leaf) {
        String key = leaf.getKey();
        JsonNode value = leaf.getValue();
        try {
            switch (key) {
                case "N":
                    return value.asDouble();
                case "S":
                    return value.asText();
                case "BOOL":
                    return value.asBoolean();
                case "B":
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

        session = cacheAndOrGetCachedSession();

        String keyspace = keyspaceName;
        String table = deleteTableRequest.getTableName();
        String statement = String.format("DROP TABLE %s.%s;\n", keyspace, table);
        ResultSet result = this.session.execute(statement);
        if (result.wasApplied()) {

            logger.info("deleted table " + table);

            datastaxManager.refreshSchema();

            TableDescription newTableDesc = this.getTableDescription(table, null,null);
            DeleteTableResult createResult = (new DeleteTableResult()).withTableDescription(newTableDesc);
            return new DynamoDBResponse(createResult, 200);
        }
        return null;
    }

    @Override
    public DynamoDBResponse createTable(CreateTableRequest createTableRequest) throws IOException {

        logger.info("creating JSON table");

        session = cacheAndOrGetCachedSession();


        String columnPairs = createTableRequest.getAttributeDefinitions().stream().map(this::attributeToPairs).collect(Collectors.joining(","));
        columnPairs += ",json_blob text";
        String keyspace = keyspaceName;
        String table = createTableRequest.getTableName();
        String primaryKey = getPrimaryKey(createTableRequest.getKeySchema());
        String statement = String.format("CREATE TABLE IF NOT EXISTS %s.%s ( %s, PRIMARY KEY %s);\n", keyspace, table, columnPairs, primaryKey);
        ResultSet result = this.session.execute(statement);
        if (result.wasApplied()) {

            logger.info("created table " + table);

            datastaxManager.refreshSchema();

            TableDescription newTableDesc = this.getTableDescription(table, createTableRequest.getAttributeDefinitions(), createTableRequest.getKeySchema());
            CreateTableResult createResult = (new CreateTableResult()).withTableDescription(newTableDesc);
            return new DynamoDBResponse(createResult, 200);
        }
        return null;
    }

    public String getPrimaryKey(List<KeySchemaElement> keySchema){
    String partitionKey = "";
        String clusteringColumns = "";

        for (KeySchemaElement keySchemaElement : keySchema) {
            String type = keySchemaElement.getKeyType();
            String name = keySchemaElement.getAttributeName();
            if (type.equals(HASH.toString())) {
                partitionKey += " " + name + ",";
            }
            if (type.equals(RANGE.toString())) {
                clusteringColumns += " " + name + ",";
            }
        }

        partitionKey =partitionKey.substring(0,partitionKey.length()-1);
        clusteringColumns=clusteringColumns.substring(0,clusteringColumns.length()-1);

        String primaryKey = "";
        if(clusteringColumns.length()>0)

        {
            primaryKey = String.format("((%s), %s)", partitionKey, clusteringColumns);
        }else

        {
            primaryKey = String.format("((%s))", partitionKey);
        }

        return primaryKey;
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

    @Override
    public DynamoDBResponse describeTable(DescribeTableRequest describeTableRequest) {
        String tableName= describeTableRequest.getTableName();
        if (datastaxManager.tableInSchema(tableName)){
            TableDef dseJsonTableDef = datastaxManager.getTableDef(tableName);

            Map<String, DataType.Name> hashKeyMap = dseJsonTableDef.getPartitionKeyMap();
            Map<String, DataType.Name> sortKeyMap = dseJsonTableDef.getClusteringColumnMap();

            String sortKey = null;
            DataType.Name sortKeyType = null;
            for (Map.Entry<String, DataType.Name> sortKeyKV : sortKeyMap.entrySet()) {
                sortKey = sortKeyKV.getKey();
                sortKeyType = sortKeyKV.getValue();
            }
            String hashKey = null;
            DataType.Name hashKeyType = null;
            for (Map.Entry<String, DataType.Name> hashKeyKV : hashKeyMap.entrySet()) {
                hashKey = hashKeyKV.getKey();
                hashKeyType = hashKeyKV.getValue();
            }


            TableDescription tableDescription = new TableDescription()
                    .withTableName(tableName)
                    .withAttributeDefinitions(
                            cqlToAD(hashKey, hashKeyType),
                            cqlToAD(sortKey, sortKeyType)
                    )
                    .withKeySchema(
                            new KeySchemaElement(hashKey, HASH),
                            new KeySchemaElement(sortKey, KeyType.RANGE))
                    .withTableStatus(TableStatus.ACTIVE)
                    .withCreationDateTime(new Date())
                    .withTableArn(tableName);

            DescribeTableResult dtr = new DescribeTableResult().withTable(tableDescription);

            DynamoDBResponse ddbr = new DynamoDBResponse(dtr, 200);

            return ddbr;

        }else{
            DynamoDBResponse ddbr = new DynamoDBResponse(null, 400);
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

        PreparedStatement deleteStatement = datastaxManager.getDeleteStatement(tableName);

        Map<String, DataType.Name> partitionKeys = datastaxManager.getPartitionKeys(tableName);
        Map<String, DataType.Name> clusteringColumns = datastaxManager.getClusteringColumns(tableName);

        Map<String, AttributeValue> keys = dir.getKey();

        Object partitionKey = null;
        Object clusteringKey = null;
        for (Map.Entry<String, AttributeValue> pair : keys.entrySet()) {
            if (partitionKeys.containsKey(pair.getKey())){
                partitionKey = getAttributeObject(pair.getValue());
            }else if (clusteringColumns.containsKey(pair.getKey())){
                clusteringKey = getAttributeObject(pair.getValue());
            }
        }

        BoundStatement boundStatement = deleteStatement.bind(clusteringKey, partitionKey);

        session = cacheAndOrGetCachedSession();
        ResultSet result = session.execute(boundStatement);

        if (result.wasApplied()){
            DeleteItemResult dres = new DeleteItemResult();
            return new DynamoDBResponse(dres, 200);
        }
        else return null;

    }

    private Object getAttributeObject(AttributeValue value) {
        //Note: only string, number, and binary are allowed for primary keys in dynamodb

        if (value.getN() != null){
            return Double.parseDouble(value.getN());
        }
        if (value.getB() != null){
            return value.getB();
        }
        if (value.getS() != null){
            return value.getS();
        }else {
            logger.error("type unsupported");
            return null;
        }
    }

    @Override
    public DynamoDBResponse putItem(PutItemRequest putItemRequest) throws IOException {
        PutItemResult pir = new PutItemResult();
        logger.debug("put item into JSON table");
        PreparedStatement jsonStatement = datastaxManager.getPutStatement(putItemRequest.getTableName());
        if (jsonStatement == null){
            String msg= String.format("Requested resource not found: Table: %s not found",putItemRequest.getTableName());
            DynamoDBResponse ddbResponse = new DynamoDBResponse(pir, 400);
            ddbResponse.setError(msg);
            logger.error(msg);
            return ddbResponse;
        }
        String tableName = putItemRequest.getTableName();
        Map<String, DataType.Name> partionKeys = datastaxManager.getPartitionKeys(tableName);
        Map<String, DataType.Name> clusteringColumns = datastaxManager.getClusteringColumns(tableName);

        Map<String, AttributeValue> items = putItemRequest.getItem();

        //TODO: there may be a cleaner way to to this using Marshal
        JsonNode itemsJson = mapper.valueToTree(items);
        itemsJson = organizeColumns(itemsJson, partionKeys, clusteringColumns);

        String jsonColumns = stringify(itemsJson);

        BoundStatement boundStatement = jsonStatement.bind(jsonColumns);

        session = cacheAndOrGetCachedSession();
        try {
            ResultSet result = session.execute(boundStatement);
            if (result.wasApplied()){
                DynamoDBResponse ddbResponse = new DynamoDBResponse(pir, 200);
                return ddbResponse;
            }
            else {
                DynamoDBResponse ddbResponse = new DynamoDBResponse(pir, 400);
                String msg = String.format("PutItem not applied",putItemRequest.getTableName());
                ddbResponse.setError(msg);
                return ddbResponse;
            }
        }catch (Exception e){
            DynamoDBResponse ddbResponse = new DynamoDBResponse(pir, 400);
            String msg= String.format("PutItem write failed with error: %s",e.getMessage());
            ddbResponse.setError(msg);
            return ddbResponse;
        }
    }

    @Override
    public DynamoDBResponse getItem(GetItemRequest getItemRequest) {
        logger.debug("get item from JSON table");

        String tableName = getItemRequest.getTableName();

        PreparedStatement selectStatement = datastaxManager.getQueryRowStatement(tableName);

        Map<String, DataType.Name> partitionKeys = datastaxManager.getPartitionKeys(tableName);
        Map<String, DataType.Name> clusteringColumns = datastaxManager.getClusteringColumns(tableName);

        Map<String, AttributeValue> keys = getItemRequest.getKey();

        Object partitionKey = null;
        Object clusteringKey = null;
        for (Map.Entry<String, AttributeValue> pair : keys.entrySet()) {
            if (partitionKeys.containsKey(pair.getKey())){
                partitionKey = getAttributeObject(pair.getValue());
            }else if (clusteringColumns.containsKey(pair.getKey())){
                clusteringKey = getAttributeObject(pair.getValue());
            }
        }

        BoundStatement boundStatement = selectStatement.bind(clusteringKey, partitionKey);

        session = cacheAndOrGetCachedSession();
        ResultSet result = session.execute(boundStatement);

        GetItemResult gir = new GetItemResult();
        Map<String, AttributeValue> item = new HashMap<>();
        ColumnDefinitions colDef = result.getColumnDefinitions();

        for (Row row : result) {
            for (ColumnDefinitions.Definition definition : colDef) {
                AttributeValue av = colToAttributeValue(definition, row);
                item.put(definition.getName(), av);
            }
        }

        gir.withItem(item);
        return new DynamoDBResponse(gir, 200);
    }

    private Session cacheAndOrGetCachedSession() {
        if (this.session == null){
            this.session = datastaxManager.getSession();
            return session;
        }
        else {
            return session;
        }
    }

    private String stringify(JsonNode items) {
        ObjectNode itemsClone =  (ObjectNode) items.deepCopy();
        String jsonString = items.get("json_blob").toString();
        itemsClone.remove("json_blob");
        itemsClone.put("json_blob", jsonString);
        return itemsClone.toString();
    }

    private ObjectNode organizeColumns(JsonNode items, Map<String, DataType.Name> partionKeys, Map<String, DataType.Name> clusteringColumns) {

        ObjectNode itemsClone =  (ObjectNode) items.deepCopy();
        Set<String> removeMe = new HashSet<String>();

        ObjectNode jsonBlobNode = new ObjectNode(JsonNodeFactory.instance);
        for (Iterator<Map.Entry<String, JsonNode>> it = itemsClone.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> item = it.next();
            String itemKey = item.getKey();
            if (!partionKeys.containsKey(itemKey) && !clusteringColumns.containsKey(itemKey)){
                removeMe.add(itemKey);
                jsonBlobNode.put(itemKey,item.getValue());
            }
        }

        itemsClone.remove(removeMe);
        itemsClone = (ObjectNode) stripDynamoTypes((JsonNode)itemsClone);
        itemsClone.put("json_blob", items);
        return itemsClone;
    }




    public JsonNode stripDynamoTypes(JsonNode items) {
        JsonNodeType type = items.getNodeType();
        List<String> dynamoTypes = Arrays.asList("N", "S", "BOOL", "B", "S", "SS");
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
            ObjectMapper mapper = new ObjectMapper();
            DynamoDBAttributeType keyType = valueOf(key);
            switch (keyType) {
                case N:
                    return mapper.valueToTree(value.asDouble());
                case S:
                    return mapper.valueToTree(value.asText());
                case BOOL:
                    return mapper.valueToTree(value.asBoolean());
                case B:
                    return mapper.valueToTree(value.binaryValue());
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


        /*
    private static final com.amazonaws.protocol.json.SdkJsonProtocolFactory protocolFactory = new com.amazonaws.protocol.json.SdkJsonProtocolFactory(
            new JsonClientMetadata()
                    .withProtocolVersion("1.0")
                    .withSupportsCbor(false)
                    .withSupportsIon(false)
                    .addErrorMetadata(
                            new JsonErrorShapeMetadata().withErrorCode("ResourceInUseException").withModeledClass(
                                    com.amazonaws.services.dynamodbv2.model.ResourceInUseException.class))
                    .addErrorMetadata(
                            new JsonErrorShapeMetadata().withErrorCode("TableAlreadyExistsException").withModeledClass(
                                    com.amazonaws.services.dynamodbv2.model.TableAlreadyExistsException.class))
                    .addErrorMetadata(
                            new JsonErrorShapeMetadata().withErrorCode("GlobalTableAlreadyExistsException").withModeledClass(
                                    com.amazonaws.services.dynamodbv2.model.GlobalTableAlreadyExistsException.class))
                    .addErrorMetadata(
                            new JsonErrorShapeMetadata().withErrorCode("InvalidRestoreTimeException").withModeledClass(
                                    com.amazonaws.services.dynamodbv2.model.InvalidRestoreTimeException.class))
                    .addErrorMetadata(
                            new JsonErrorShapeMetadata().withErrorCode("ReplicaAlreadyExistsException").withModeledClass(
                                    com.amazonaws.services.dynamodbv2.model.ReplicaAlreadyExistsException.class))
                    .addErrorMetadata(
                            new JsonErrorShapeMetadata().withErrorCode("ConditionalCheckFailedException").withModeledClass(
                                    com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException.class))
                    .addErrorMetadata(
                            new JsonErrorShapeMetadata().withErrorCode("BackupNotFoundException").withModeledClass(
                                    com.amazonaws.services.dynamodbv2.model.BackupNotFoundException.class))
                    .addErrorMetadata(
                            new JsonErrorShapeMetadata().withErrorCode("IndexNotFoundException").withModeledClass(
                                    com.amazonaws.services.dynamodbv2.model.IndexNotFoundException.class))
                    .addErrorMetadata(
                            new JsonErrorShapeMetadata().withErrorCode("LimitExceededException").withModeledClass(
                                    com.amazonaws.services.dynamodbv2.model.LimitExceededException.class))
                    .addErrorMetadata(
                            new JsonErrorShapeMetadata().withErrorCode("GlobalTableNotFoundException").withModeledClass(
                                    com.amazonaws.services.dynamodbv2.model.GlobalTableNotFoundException.class))
                    .addErrorMetadata(
                            new JsonErrorShapeMetadata().withErrorCode("ItemCollectionSizeLimitExceededException").withModeledClass(
                                    com.amazonaws.services.dynamodbv2.model.ItemCollectionSizeLimitExceededException.class))
                    .addErrorMetadata(
                            new JsonErrorShapeMetadata().withErrorCode("ReplicaNotFoundException").withModeledClass(
                                    com.amazonaws.services.dynamodbv2.model.ReplicaNotFoundException.class))
                    .addErrorMetadata(
                            new JsonErrorShapeMetadata().withErrorCode("TableNotFoundException").withModeledClass(
                                    com.amazonaws.services.dynamodbv2.model.TableNotFoundException.class))
                    .addErrorMetadata(
                            new JsonErrorShapeMetadata().withErrorCode("BackupInUseException").withModeledClass(
                                    com.amazonaws.services.dynamodbv2.model.BackupInUseException.class))
                    .addErrorMetadata(
                            new JsonErrorShapeMetadata().withErrorCode("ResourceNotFoundException").withModeledClass(
                                    com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException.class))
                    .addErrorMetadata(
                            new JsonErrorShapeMetadata().withErrorCode("ContinuousBackupsUnavailableException").withModeledClass(
                                    com.amazonaws.services.dynamodbv2.model.ContinuousBackupsUnavailableException.class))
                    .addErrorMetadata(
                            new JsonErrorShapeMetadata().withErrorCode("TableInUseException").withModeledClass(
                                    com.amazonaws.services.dynamodbv2.model.TableInUseException.class))
                    .addErrorMetadata(
                            new JsonErrorShapeMetadata().withErrorCode("ProvisionedThroughputExceededException").withModeledClass(
                                    com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException.class))
                    .addErrorMetadata(
                            new JsonErrorShapeMetadata().withErrorCode("PointInTimeRecoveryUnavailableException").withModeledClass(
                                    com.amazonaws.services.dynamodbv2.model.PointInTimeRecoveryUnavailableException.class))
                    .addErrorMetadata(
                            new JsonErrorShapeMetadata().withErrorCode("InternalServerError").withModeledClass(
                                    com.amazonaws.services.dynamodbv2.model.InternalServerErrorException.class))
                    .withBaseServiceExceptionClass(com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException.class));

    public Map stripDynamoTypes(Map<String, AttributeValue> items) {
        for (Map.Entry<String, AttributeValue> item : items.entrySet()) {
            String contentType = ContentType.APPLICATION_JSON.toString();
            JsonFactory jsonFactory = new JsonFactory();
            StructuredJsonGenerator jsonGenerator = new SdkJsonGenerator(jsonFactory, contentType);
            OperationInfo operationInfo= OperationInfo.builder().protocol(Protocol.AWS_JSON).requestUri("/")
                   // .httpMethodName(HttpMethodName.POST).hasExplicitPayloadMember(false).hasPayloadMembers(true)
                   // .operationIdentifier("DynamoDB_20120810.GetItem")
                   // .serviceName("AmazonDynamoDBv2")
                    .build();
            Object originalRequest = null;
            MarshallerRegistry.Builder marshallerRegistryOverrides = MarshallerRegistry.builder();
            item.getValue().marshall(new JsonProtocolMarshaller(jsonGenerator, contentType, operationInfo, originalRequest, marshallerRegistryOverrides));
        }
        return null;
    }
    */

}
