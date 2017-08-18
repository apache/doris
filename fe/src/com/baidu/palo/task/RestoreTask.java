// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.task;

import com.baidu.palo.thrift.TResourceInfo;
import com.baidu.palo.thrift.TRestoreReq;
import com.baidu.palo.thrift.TTaskType;

import java.util.Map;

public class RestoreTask extends AgentTask {

    private long jobId;
    private String remoteFilePath;
    private int schemaHash;
    private Map<String, String> remoteProperties;

    public RestoreTask(TResourceInfo resourceInfo, long backendId, long jobId, long dbId, long tableId,
                       long partitionId, long indexId, long tabletId, int schemaHash,
                       String remoteFilePath, Map<String, String> remoteProperties) {
        super(resourceInfo, backendId, TTaskType.RESTORE, dbId, tableId, partitionId, indexId, tabletId);

        this.jobId = jobId;
        this.remoteFilePath = remoteFilePath;
        this.schemaHash = schemaHash;
        this.remoteProperties = remoteProperties;
    }

    public long getJobId() {
        return jobId;
    }

    public String getRemoteFilePath() {
        return remoteFilePath;
    }

    public int getSchemaHash() {
        return schemaHash;
    }

    public Map<String, String> getRemoteProperties() {
        return remoteProperties;
    }

    public TRestoreReq toThrift() {
        TRestoreReq req = new TRestoreReq(tabletId, schemaHash, remoteFilePath, remoteProperties);
        return req;
    }
}
