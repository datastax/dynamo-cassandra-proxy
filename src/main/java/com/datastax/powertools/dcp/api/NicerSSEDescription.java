package com.datastax.powertools.dcp.api;

/*
 *
 * @author Sebastián Estévez on 1/24/19.
 *
 */


import com.fasterxml.jackson.annotation.JsonProperty;

public class NicerSSEDescription {
    public NicerSSEDescription() {
    }

    @JsonProperty("Status")
    private String status;
    @JsonProperty("SSEType")
    private String sSEType;
    @JsonProperty("KMSMasterKeyArn")
    private String kMSMasterKeyArn;

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getsSEType() {
        return sSEType;
    }

    public void setsSEType(String sSEType) {
        this.sSEType = sSEType;
    }

    public String getkMSMasterKeyArn() {
        return kMSMasterKeyArn;
    }

    public void setkMSMasterKeyArn(String kMSMasterKeyArn) {
        this.kMSMasterKeyArn = kMSMasterKeyArn;
    }

}
