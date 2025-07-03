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

package org.pentaho.di.trans.steps.dorisstreamloader.load;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RespContent {

    @JsonProperty(value = "TxnId")
    private Long txnId;

    @JsonProperty(value = "Label")
    private String label;

    @JsonProperty(value = "Status")
    private String status;

    @JsonProperty(value = "TwoPhaseCommit")
    private String twoPhaseCommit;

    @JsonProperty(value = "ExistingJobStatus")
    private String existingJobStatus;

    @JsonProperty(value = "Message")
    private String message;

    @JsonProperty(value = "NumberTotalRows")
    private Long numberTotalRows;

    @JsonProperty(value = "NumberLoadedRows")
    private Long numberLoadedRows;

    @JsonProperty(value = "NumberFilteredRows")
    private Integer numberFilteredRows;

    @JsonProperty(value = "NumberUnselectedRows")
    private Integer numberUnselectedRows;

    @JsonProperty(value = "LoadBytes")
    private Long loadBytes;

    @JsonProperty(value = "LoadTimeMs")
    private Integer loadTimeMs;

    @JsonProperty(value = "BeginTxnTimeMs")
    private Integer beginTxnTimeMs;

    @JsonProperty(value = "StreamLoadPutTimeMs")
    private Integer streamLoadPutTimeMs;

    @JsonProperty(value = "ReadDataTimeMs")
    private Integer readDataTimeMs;

    @JsonProperty(value = "WriteDataTimeMs")
    private Integer writeDataTimeMs;

    @JsonProperty(value = "CommitAndPublishTimeMs")
    private Integer commitAndPublishTimeMs;

    @JsonProperty(value = "ErrorURL")
    private String errorURL;

    public Long getTxnId() {
        return txnId;
    }

    public String getStatus() {
        return status;
    }

    public String getTwoPhaseCommit() {
        return twoPhaseCommit;
    }

    public String getMessage() {
        return message;
    }

    public String getExistingJobStatus() {
        return existingJobStatus;
    }

    public Long getNumberTotalRows() {
        return numberTotalRows;
    }

    public Long getNumberLoadedRows() {
        return numberLoadedRows;
    }

    public Integer getNumberFilteredRows() {
        return numberFilteredRows;
    }

    public Integer getNumberUnselectedRows() {
        return numberUnselectedRows;
    }

    public Long getLoadBytes() {
        return loadBytes;
    }

    public Integer getLoadTimeMs() {
        return loadTimeMs;
    }

    public Integer getBeginTxnTimeMs() {
        return beginTxnTimeMs;
    }

    public Integer getStreamLoadPutTimeMs() {
        return streamLoadPutTimeMs;
    }

    public Integer getReadDataTimeMs() {
        return readDataTimeMs;
    }

    public Integer getWriteDataTimeMs() {
        return writeDataTimeMs;
    }

    public Integer getCommitAndPublishTimeMs() {
        return commitAndPublishTimeMs;
    }

    public String getLabel() {
        return label;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            return "";
        }
    }

    public String getErrorURL() {
        return errorURL;
    }
}
