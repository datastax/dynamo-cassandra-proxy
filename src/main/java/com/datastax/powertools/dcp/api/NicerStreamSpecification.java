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
