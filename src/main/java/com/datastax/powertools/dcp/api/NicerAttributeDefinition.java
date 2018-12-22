package com.datastax.powertools.dcp.api;

/*
 *
 * @author Sebastián Estévez on 12/17/18.
 *
 */


import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
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
}
