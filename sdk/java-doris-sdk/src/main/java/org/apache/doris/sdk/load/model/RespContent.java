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

package org.apache.doris.sdk.load.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Doris stream load HTTP response body JSON mapping POJO.
 * Field names match the Doris API response exactly.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RespContent {

    @JsonProperty("TxnId")      private long txnId;
    @JsonProperty("Label")      private String label;
    @JsonProperty("Status")     private String status;
    @JsonProperty("TwoPhaseCommit")    private String twoPhaseCommit;
    @JsonProperty("ExistingJobStatus") private String existingJobStatus;
    @JsonProperty("Message")    private String message;
    @JsonProperty("NumberTotalRows")      private long numberTotalRows;
    @JsonProperty("NumberLoadedRows")     private long numberLoadedRows;
    @JsonProperty("NumberFilteredRows")   private int numberFilteredRows;
    @JsonProperty("NumberUnselectedRows") private int numberUnselectedRows;
    @JsonProperty("LoadBytes")            private long loadBytes;
    @JsonProperty("LoadTimeMs")           private int loadTimeMs;
    @JsonProperty("BeginTxnTimeMs")       private int beginTxnTimeMs;
    @JsonProperty("StreamLoadPutTimeMs")  private int streamLoadPutTimeMs;
    @JsonProperty("ReadDataTimeMs")       private int readDataTimeMs;
    @JsonProperty("WriteDataTimeMs")      private int writeDataTimeMs;
    @JsonProperty("CommitAndPublishTimeMs") private int commitAndPublishTimeMs;
    @JsonProperty("ErrorURL")   private String errorUrl;

    public long getTxnId() { return txnId; }
    public void setTxnId(long txnId) { this.txnId = txnId; }
    public String getLabel() { return label; }
    public void setLabel(String label) { this.label = label; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public String getTwoPhaseCommit() { return twoPhaseCommit; }
    public void setTwoPhaseCommit(String v) { this.twoPhaseCommit = v; }
    public String getExistingJobStatus() { return existingJobStatus; }
    public void setExistingJobStatus(String v) { this.existingJobStatus = v; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    public long getNumberTotalRows() { return numberTotalRows; }
    public void setNumberTotalRows(long v) { this.numberTotalRows = v; }
    public long getNumberLoadedRows() { return numberLoadedRows; }
    public void setNumberLoadedRows(long v) { this.numberLoadedRows = v; }
    public int getNumberFilteredRows() { return numberFilteredRows; }
    public void setNumberFilteredRows(int v) { this.numberFilteredRows = v; }
    public int getNumberUnselectedRows() { return numberUnselectedRows; }
    public void setNumberUnselectedRows(int v) { this.numberUnselectedRows = v; }
    public long getLoadBytes() { return loadBytes; }
    public void setLoadBytes(long v) { this.loadBytes = v; }
    public int getLoadTimeMs() { return loadTimeMs; }
    public void setLoadTimeMs(int v) { this.loadTimeMs = v; }
    public int getBeginTxnTimeMs() { return beginTxnTimeMs; }
    public void setBeginTxnTimeMs(int v) { this.beginTxnTimeMs = v; }
    public int getStreamLoadPutTimeMs() { return streamLoadPutTimeMs; }
    public void setStreamLoadPutTimeMs(int v) { this.streamLoadPutTimeMs = v; }
    public int getReadDataTimeMs() { return readDataTimeMs; }
    public void setReadDataTimeMs(int v) { this.readDataTimeMs = v; }
    public int getWriteDataTimeMs() { return writeDataTimeMs; }
    public void setWriteDataTimeMs(int v) { this.writeDataTimeMs = v; }
    public int getCommitAndPublishTimeMs() { return commitAndPublishTimeMs; }
    public void setCommitAndPublishTimeMs(int v) { this.commitAndPublishTimeMs = v; }
    public String getErrorUrl() { return errorUrl; }
    public void setErrorUrl(String errorUrl) { this.errorUrl = errorUrl; }

    @Override
    public String toString() {
        return "RespContent{label='" + label + "', status='" + status
                + "', loadedRows=" + numberLoadedRows + ", loadBytes=" + loadBytes
                + ", loadTimeMs=" + loadTimeMs + "}";
    }
}
