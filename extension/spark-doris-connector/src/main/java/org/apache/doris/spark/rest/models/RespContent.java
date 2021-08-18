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
package org.apache.doris.spark.rest.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RespContent {

    @JsonProperty(value = "TxnId")
    private int TxnId;

    @JsonProperty(value = "Label")
    private String Label;

    @JsonProperty(value = "Status")
    private String Status;

    @JsonProperty(value = "ExistingJobStatus")
    private String ExistingJobStatus;

    @JsonProperty(value = "Message")
    private String Message;

    @JsonProperty(value = "NumberTotalRows")
    private long NumberTotalRows;

    @JsonProperty(value = "NumberLoadedRows")
    private long NumberLoadedRows;

    @JsonProperty(value = "NumberFilteredRows")
    private int NumberFilteredRows;

    @JsonProperty(value = "NumberUnselectedRows")
    private int NumberUnselectedRows;

    @JsonProperty(value = "LoadBytes")
    private long LoadBytes;

    @JsonProperty(value = "LoadTimeMs")
    private int LoadTimeMs;

    @JsonProperty(value = "BeginTxnTimeMs")
    private int BeginTxnTimeMs;

    @JsonProperty(value = "StreamLoadPutTimeMs")
    private int StreamLoadPutTimeMs;

    @JsonProperty(value = "ReadDataTimeMs")
    private int ReadDataTimeMs;

    @JsonProperty(value = "WriteDataTimeMs")
    private int WriteDataTimeMs;

    @JsonProperty(value = "CommitAndPublishTimeMs")
    private int CommitAndPublishTimeMs;

    @JsonProperty(value = "ErrorURL")
    private String ErrorURL;

    public String getStatus() {
        return Status;
    }

    public String getMessage() {
        return Message;
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
}
