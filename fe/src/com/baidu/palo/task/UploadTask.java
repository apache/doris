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
import com.baidu.palo.thrift.TTaskType;
import com.baidu.palo.thrift.TUploadReq;

import java.util.Map;

public class UploadTask extends AgentTask {

    private long jobId;
    private String src;
    private String dest;

    private Map<String, String> remoteSourceProperties;

    public UploadTask(TResourceInfo resourceInfo, long backendId, long jobId, long dbId, long tableId,
                      long partitionId, long indexId, long tabletId, String src, String dest,
                      Map<String, String> remoteSourceProperties) {
        super(resourceInfo, backendId, TTaskType.UPLOAD, dbId, tableId, partitionId, indexId, tabletId);
        this.jobId = jobId;
        this.src = src;
        this.dest = dest;
        this.remoteSourceProperties = remoteSourceProperties;
    }

    public long getJobId() {
        return jobId;
    }

    public String getSrc() {
        return src;
    }

    public String getDest() {
        return dest;
    }

    public Map<String, String> getRemoteSourceProperties() {
        return remoteSourceProperties;
    }

    public TUploadReq toThrift() {
        TUploadReq request = new TUploadReq(src, dest, remoteSourceProperties);
        request.setTablet_id(tabletId);
        return request;
    }
}
