package com.datastax.powertools.dcp.api;

/*
 *
 * @author Sebastián Estévez on 1/24/19.
 *
 */


import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class NicerProjection {

    @JsonProperty("ProjectionType")
    private String projectionType;
    @JsonProperty("NonKeyAttributes")
    private List<String> nonKeyAttributes;

    public NicerProjection() {
    }

    public String getProjectionType() {
        return projectionType;
    }

    public void setProjectionType(String projectionType) {
        this.projectionType = projectionType;
    }

    public List<String> getNonKeyAttributes() {
        return nonKeyAttributes;
    }

    public void setNonKeyAttributes(List<String> nonKeyAttributes) {
        this.nonKeyAttributes = nonKeyAttributes;
    }

}
