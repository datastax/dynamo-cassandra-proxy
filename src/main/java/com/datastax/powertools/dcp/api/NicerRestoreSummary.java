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
