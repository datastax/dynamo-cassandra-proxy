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

import com.amazonaws.services.dynamodbv2.model.*;
import com.datastax.powertools.dcp.api.DynamoDBResponse;
import com.datastax.powertools.dcp.managed.dse.CassandraManager;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;

import java.io.IOException;

public abstract class DynamoDSETranslator {
    protected final CassandraManager cassandraManager;
    protected String keyspaceName;

    public static final ObjectMapper awsRequestMapper = new ObjectMapper()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .setPropertyNamingStrategy(
            //Map to AWS api style
             new PropertyNamingStrategy.UpperCamelCaseStrategy() {
                @Override
                public String translate(String input) {
                    String output = super.translate(input);

                    if (output != null && output.length() >= 2) {
                        switch (output) {
                            case "Ss": return "SS";
                            case "Bool": return "BOOL";
                            case "Ns": return "NS";
                            default:
                                break;
                        }
                    }

                    return output;
                }
            });

    public DynamoDSETranslator(CassandraManager cassandraManager) {
        this.cassandraManager = cassandraManager;
        this.keyspaceName = cassandraManager.getKeyspaceName();
    }

    public abstract DynamoDBResponse createTable(CreateTableRequest payload) throws IOException;
    public abstract DynamoDBResponse putItem(PutItemRequest payload) throws IOException;
    public abstract DynamoDBResponse getItem(GetItemRequest payload);
    public abstract DynamoDBResponse query(QueryRequest payload);
    public abstract DynamoDBResponse deleteItem(DeleteItemRequest dir);
    public abstract DynamoDBResponse describeTable(DescribeTableRequest dtr);
    public abstract DynamoDBResponse deleteTable(DeleteTableRequest dbr);

    protected String getKeyspaceName() {
        return keyspaceName;
    }

}
