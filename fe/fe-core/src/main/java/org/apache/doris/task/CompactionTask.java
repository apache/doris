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

import org.apache.doris.thrift.TCompactionReq;
import org.apache.doris.thrift.TTaskType;


public class CompactionTask extends AgentTask {


    private int schemaHash;
    private String type;

    public CompactionTask(long backendId, long dbId, long tableId, long partitionId, long indexId,
                          long tabletId, int schemaHash, String type) {
        super(null, backendId, TTaskType.COMPACTION, dbId, tableId, partitionId, indexId, tabletId);
        this.schemaHash = schemaHash;
        this.type = type;
    }

    public int getSchemaHash() {
        return schemaHash;
    }

    public TCompactionReq toThrift() {
        TCompactionReq request = new TCompactionReq();
        request.setTabletId(tabletId);
        request.setSchemaHash(schemaHash);
        request.setType(type);

        return request;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("tablet id: ").append(tabletId).append(", schema hash: ").append(schemaHash);
        sb.append(", backend: ").append(backendId).append(", type: ").append(type);
        return sb.toString();
    }
}
