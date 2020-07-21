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

package org.apache.doris.task;

import org.apache.doris.thrift.TMoveDirReq;
import org.apache.doris.thrift.TResourceInfo;
import org.apache.doris.thrift.TTaskType;

public class DirMoveTask extends AgentTask {

    private long jobId;
    private String src;
    private int schemaHash;
    private boolean overwrite;

    public DirMoveTask(TResourceInfo resourceInfo, long backendId, long signature, long jobId, long dbId,
            long tableId, long partitionId, long indexId, long tabletId, String src, int schemaHash,
            boolean overwrite) {
        super(resourceInfo, backendId, TTaskType.MOVE, dbId, tableId, partitionId, indexId, tabletId, signature);
        this.jobId = jobId;
        this.src = src;
        this.schemaHash = schemaHash;
        this.overwrite = overwrite;
    }

    public long getJobId() {
        return jobId;
    }

    public String getSrc() {
        return src;
    }

    public int getSchemaHash() {
        return schemaHash;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public TMoveDirReq toThrift() {
        TMoveDirReq req = new TMoveDirReq(tabletId, schemaHash, src, jobId, overwrite);
        return req;
    }

}
