// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.job.extensions.cdc.state;

import com.google.gson.annotations.SerializedName;

import java.util.Map;

public class SnapshotSplit extends AbstractSourceSplit {
    @SerializedName("tid")
    private String tableId;
    @SerializedName("sk")
    private String splitKey;
    @SerializedName("ss")
    private String splitStart;
    @SerializedName("se")
    private String splitEnd;
    @SerializedName("hw")
    private Map<String, String> highWatermark;

    public SnapshotSplit() {
        super();
    }

    public SnapshotSplit(String splitId, String tableId, String splitKey, String splitStart, String splitEnd,
            Map<String, String> highWatermark) {
        super(splitId);
        this.tableId = tableId;
        this.splitKey = splitKey;
        this.splitStart = splitStart;
        this.splitEnd = splitEnd;
        this.highWatermark = highWatermark;
    }

    public String getTableId() {
        return tableId;
    }

    public void setTableId(String tableId) {
        this.tableId = tableId;
    }

    public String getSplitKey() {
        return splitKey;
    }

    public void setSplitKey(String splitKey) {
        this.splitKey = splitKey;
    }

    public String getSplitStart() {
        return splitStart;
    }

    public void setSplitStart(String splitStart) {
        this.splitStart = splitStart;
    }

    public String getSplitEnd() {
        return splitEnd;
    }

    public void setSplitEnd(String splitEnd) {
        this.splitEnd = splitEnd;
    }

    public Map<String, String> getHighWatermark() {
        return highWatermark;
    }

    public void setHighWatermark(Map<String, String> highWatermark) {
        this.highWatermark = highWatermark;
    }

}
