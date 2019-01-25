package com.datastax.powertools.dcp.api;

/*
 *
 * @author Sebastián Estévez on 1/24/19.
 *
 */


import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

public class NicerRestoreSummary {
    public NicerRestoreSummary() {
    }

    @JsonProperty("SourceBackupArn")
    private String sourceBackupArn;
    @JsonProperty("SourceTableArn")
    private String sourceTableArn;
    @JsonProperty("RestoreDateTime")
    private Date restoreDateTime;
    @JsonProperty("RestoreInProgress")
    private Boolean restoreInProgress;

    public String getSourceBackupArn() {
        return sourceBackupArn;
    }

    public void setSourceBackupArn(String sourceBackupArn) {
        this.sourceBackupArn = sourceBackupArn;
    }

    public String getSourceTableArn() {
        return sourceTableArn;
    }

    public void setSourceTableArn(String sourceTableArn) {
        this.sourceTableArn = sourceTableArn;
    }

    public Date getRestoreDateTime() {
        return restoreDateTime;
    }

    public void setRestoreDateTime(Date restoreDateTime) {
        this.restoreDateTime = restoreDateTime;
    }

    public Boolean getRestoreInProgress() {
        return restoreInProgress;
    }

    public void setRestoreInProgress(Boolean restoreInProgress) {
        this.restoreInProgress = restoreInProgress;
    }
}
