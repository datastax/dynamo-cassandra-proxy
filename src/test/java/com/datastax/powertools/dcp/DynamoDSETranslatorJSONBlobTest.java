package com.datastax.powertools.dcp;

import com.datastax.powertools.dcp.managed.dse.DatastaxManager;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.Test;

import java.io.IOException;


/*
 *
 * @author Sebastián Estévez on 12/17/18.
 *
 */


public class DynamoDSETranslatorJSONBlobTest {
    private String stringJson = "{\"hash_key\":{\"S\":\"hash_value\"},\"json_blob\":{\"favorites\":{\"SS\":[\"puppies\",\"kittens\",\"other cute animals\"]},\"city\":{\"S\":\"NYC\"}}}";
    //private String stringJson2 = "{\"favorites\":{\"SS\":[\"puppies\",\"kittens\",\"other cute animals\"]}}";
    private String stringJson2 = "{\"SS\":[\"puppies\",\"kittens\",\"other cute animals\"]}";
    //private String stringJson2 = "[\"puppies\",\"kittens\",\"other cute animals\"]";


    @Test
    public void testStripJson() throws IOException {
        DatastaxManager dm = new DatastaxManager();
        DynamoDSETranslatorJSONBlob jsonBlobTranslator = new DynamoDSETranslatorJSONBlob(dm);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonObj= mapper.readTree(stringJson);

        JsonNode result = jsonBlobTranslator.stripDynamoTypes(jsonObj);
        System.out.println(jsonObj.toString());
        System.out.println(result.toString());
    }

    @Test
    public void testStripJsonArray() throws IOException {
        DatastaxManager dm = new DatastaxManager();
        DynamoDSETranslatorJSONBlob jsonBlobTranslator = new DynamoDSETranslatorJSONBlob(dm);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonObj= mapper.readTree(stringJson2);

        JsonNode result = jsonBlobTranslator.stripDynamoTypes(jsonObj);
        System.out.println(jsonObj.toString());
        System.out.println(result.toString());
    }
}