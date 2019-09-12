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
import com.datastax.powertools.dcp.api.DynamoDBRequest;
import com.datastax.powertools.dcp.api.DynamoDBResponse;
import com.datastax.powertools.dcp.managed.dse.DatastaxManager;

public abstract class DynamoDSETranslator {
    private final DatastaxManager datastaxManager;
    protected String keyspaceName;

    public DynamoDSETranslator(DatastaxManager datastaxManager) {
        this.datastaxManager = datastaxManager;
        this.keyspaceName = datastaxManager.getKeyspaceName();
    }

    public abstract DynamoDBResponse createTable(DynamoDBRequest payload);
    public abstract DynamoDBResponse putItem(DynamoDBRequest payload);
    public abstract DynamoDBResponse getItem(GetItemRequest payload);
    public abstract DynamoDBResponse query(DynamoDBRequest payload);
    public abstract DynamoDBResponse deleteItem(DeleteItemRequest dir);
    public abstract DynamoDBResponse describeTable(DynamoDBRequest dtr);

    protected String getKeyspaceName() {
        return keyspaceName;
    }
}
