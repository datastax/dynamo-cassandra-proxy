package com.datastax.powertools.dcp.api;

/*
 *
 * @author Sebastián Estévez on 1/24/19.
 *
 */


import com.fasterxml.jackson.annotation.JsonProperty;

public class NicerStreamSpecification {
    public NicerStreamSpecification() {
    }

    @JsonProperty("StreamEnabled")
    private Boolean streamEnabled;
    @JsonProperty("StreamViewType")
    private String streamViewType;

    public Boolean getStreamEnabled() {
        return streamEnabled;
    }

    public void setStreamEnabled(Boolean streamEnabled) {
        this.streamEnabled = streamEnabled;
    }

    public String getStreamViewType() {
        return streamViewType;
    }

    public void setStreamViewType(String streamViewType) {
        this.streamViewType = streamViewType;
    }
}
