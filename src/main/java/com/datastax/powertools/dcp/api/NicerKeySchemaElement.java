package com.datastax.powertools.dcp.api;

/*
 *
 * @author Sebastián Estévez on 12/17/18.
 *
 */


import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class NicerKeySchemaElement extends KeySchemaElement {
    public NicerKeySchemaElement() {
        super();
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

    @JsonProperty("KeyType")
    @Override
    public void setKeyType(String keyType) {
        super.setKeyType(keyType);
    }

    @JsonProperty("KeyType")
    @Override
    public String getKeyType() {
        return super.getKeyType();
    }

    @JsonIgnore
    public KeySchemaElement getAWSKeySchema() {
        return new KeySchemaElement(getAttributeName(), getKeyType());
    }
}
