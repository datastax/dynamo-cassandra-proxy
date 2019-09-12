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

import com.datastax.powertools.dcp.managed.dse.DatastaxManager;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.Test;

import java.io.IOException;

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