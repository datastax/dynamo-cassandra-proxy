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
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class NicerAttributeDefinition extends AttributeDefinition {

    public NicerAttributeDefinition() {
    }

    @JsonProperty("AttributeName")
    @Override
    public void setAttributeName(String attributeName) {
        super.setAttributeName(attributeName);
    }

    @JsonProperty("AttributeName")
    @Override
    public String getAttributeName() {
        return super.getAttributeName();
    }

    @JsonProperty("AttributeType")
    @Override
    public void setAttributeType(String attributeType) {
        super.setAttributeType(attributeType);
    }

    @JsonProperty("AttributeType")
    @Override
    public String getAttributeType() {
        return super.getAttributeType();
    }

    @JsonIgnore
    public AttributeDefinition getAttributeDefinition(){
        return new AttributeDefinition(getAttributeName(), getAttributeType());
    }
}
