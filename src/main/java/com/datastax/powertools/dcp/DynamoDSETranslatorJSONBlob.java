package com.datastax.powertools.dcp;

/*
 *
 * @author Sebastián Estévez on 12/15/18.
 *
 */


import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.datastax.driver.core.*;
import com.datastax.driver.dse.DseSession;
import com.datastax.powertools.dcp.api.DynamoDBRequest;
import com.datastax.powertools.dcp.managed.DatastaxManager;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.sql.JDBCType.ARRAY;

public class DynamoDSETranslatorJSONBlob extends DynamoDSETranslator {
    private final static Logger logger = LoggerFactory.getLogger(DynamoDSETranslatorJSONBlob.class);
    private final String keyspaceName;
    private final DatastaxManager datastaxManager;
    private DseSession session;

    public DynamoDSETranslatorJSONBlob(DatastaxManager datastaxManager) {
        super(datastaxManager);
        this.keyspaceName = super.getKeyspaceName();
        this.datastaxManager = datastaxManager;
    }

    @Override
    public QueryResult query(DynamoDBRequest payload) {
        Pattern pattern = Pattern.compile(".*(:\\S+)");
        logger.info("query against JSON table");

        session = cacheAndOrGetCachedSession();

        PreparedStatement jsonStatement = datastaxManager.getQueryStatement(payload.getTableName());

        // TODO: also handle KeyConditions
        Matcher matcher = pattern.matcher(payload.getKeyConditionExpression());

        if(matcher.find()) {
            String keyAlias = matcher.group(1);
            try {
                Object value = getKeyFromExpression(keyAlias, (ObjectNode) payload.getExpressionAttributeValues());
                BoundStatement boundStatement = jsonStatement.bind(value);
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
                return queryResult;
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
            for (Iterator<Map.Entry<String, JsonNode>> it2 = itemFieldIterator; it2.hasNext();){
                Map.Entry<String, JsonNode> pair = it2.next();
                if (!dynamoTypes.contains(pair .getKey())) {
                    throw new Exception("Nested not implemented");
                }else {
                    itemSet.put(item.getKey(), getAttributeFromJsonLeaf(item.getValue()));
                }
            }
        }
        return itemSet;
    }

    private AttributeValue getAttributeFromJsonLeaf(JsonNode values) {
        AttributeValue av = new AttributeValue();

        Iterator<Map.Entry<String, JsonNode>> it;
        for (it = values.fields(); it.hasNext();){
            Map.Entry<String, JsonNode> leaf = it.next();;
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
                        }else{
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
                case BOOLEAN: av.setBOOL(value.asBoolean());
                    break;
                case MISSING:
                    break;
                case NULL:
                    break;
                case NUMBER: av.setN(String.valueOf(value.asDouble()));
                    break;
                case OBJECT:
                    break;
                case POJO:
                    break;
                case STRING: av.setS(value.asText());
                    break;
            };
        }
        return av;

    }

    private AttributeValue colToAttributeValue(ColumnDefinitions.Definition colDef, Row row) {
        AttributeValue av = new AttributeValue();

        DataType.Name type = colDef.getType().getName();
        String name = colDef.getName();
        switch (type) {
            case CUSTOM:
                break;
            case ASCII:
                break;
            case BIGINT: av.setN(String.valueOf(row.getInt(name)));
                break;
            case BLOB: av.setB(row.getBytes(name));
                break;
            case BOOLEAN: av.setBOOL(row.getBool(name));
                break;
            case COUNTER:
                break;
            case DECIMAL:
                break;
            case DOUBLE: av.setN(String.valueOf(row.getDouble(name)));
                break;
            case FLOAT: av.setN(String.valueOf(row.getFloat(name)));
                break;
            case INT: av.setN(String.valueOf(row.getInt(name)));
                break;
            case TEXT: av.setS(row.getString(name));
                break;
            case TIMESTAMP: av.setS(String.valueOf(row.getTimestamp(name)));
                break;
            case UUID:
                break;
            case VARCHAR: av.setS(String.valueOf(row.getString(name)));
                break;
            case VARINT: av.setN(String.valueOf(row.getVarint(name)));
                break;
            case TIMEUUID: av.setS(String.valueOf(row.getUUID(name)));
                break;
            case INET:
                break;
            case DATE: av.setS(String.valueOf(row.getDate(name)));
                break;
            case TIME: av.setS(String.valueOf(row.getTime(name)));
                break;
            case SMALLINT: av.setN(String.valueOf(row.getInt(name)));
                break;
            case TINYINT: av.setN(String.valueOf(row.getInt(name)));
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
        if (av == null){
            logger.error("Type not supported");
            return null;
        }
        return av;
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
        }catch(Exception e){
            e.printStackTrace();
            logger.error(e.getMessage());
            return null;
        }

    }

    @Override
    public String createTable(DynamoDBRequest payload) {
        logger.info("creating JSON table");

        session = cacheAndOrGetCachedSession();

        String columnPairs = payload.getColumnPairs();
        columnPairs += ",json_blob text";
        String keyspace = keyspaceName;
        String table = payload.getTableName();
        String primaryKey = payload.getPrimaryKey();
        String statement = String.format("CREATE TABLE IF NOT EXISTS %s.%s ( %s, PRIMARY KEY %s);\n", keyspace, table, columnPairs, primaryKey);
        ResultSet result = this.session.execute(statement);
        if (result.wasApplied()) {
            logger.info("created table " + table);
            datastaxManager.refreshSchema();
            return "true";
        }
        return "false";
    }

    @Override
    public String putItem(DynamoDBRequest payload) {
        logger.info("put item into JSON table");
        PreparedStatement jsonStatement = datastaxManager.getPutStatement(payload.getTableName());
        List<String> partionKeys = datastaxManager.getPartitionKeys(payload.getTableName());
        List<String> clusteringColumns = datastaxManager.getClusteringColumns(payload.getTableName());

        JsonNode items = payload.getItem();

        items = organizeColumns(items, partionKeys, clusteringColumns);
        //items = stripDynamoTypes(items);

        String jsonColumns = stringify(items);
        BoundStatement boundStatement = jsonStatement.bind(jsonColumns);

        session = cacheAndOrGetCachedSession();
        ResultSet result = session.execute(boundStatement);

        if (result.wasApplied()){
            return "true";
        }
        else return "false";
    }

    private DseSession cacheAndOrGetCachedSession() {
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

    private ObjectNode organizeColumns(JsonNode items, List<String> partionKeys, List<String> clusteringColumns) {

        ObjectNode itemsClone =  (ObjectNode) items.deepCopy();
        Set<String> removeMe = new HashSet<String>();

        ObjectNode jsonBlobNode = new ObjectNode(JsonNodeFactory.instance);
        for (Iterator<Map.Entry<String, JsonNode>> it = itemsClone.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> item = it.next();
            String itemKey = item.getKey();
            if (!partionKeys.contains(itemKey) && !clusteringColumns.contains(itemKey)){
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
            switch (key) {
                case "N":
                    return mapper.valueToTree(value.asDouble());
                case "S":
                    return mapper.valueToTree(value.asText());
                case "BOOL":
                    return mapper.valueToTree(value.asBoolean());
                case "B":
                    return mapper.valueToTree(value.binaryValue());
                case "SS":
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
