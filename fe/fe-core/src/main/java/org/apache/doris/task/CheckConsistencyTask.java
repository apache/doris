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

import org.apache.doris.thrift.TCheckConsistencyReq;
import org.apache.doris.thrift.TResourceInfo;
import org.apache.doris.thrift.TTaskType;

public class CheckConsistencyTask extends AgentTask {

    private int schemaHash;
    private long version;

    public CheckConsistencyTask(TResourceInfo resourceInfo, long backendId, long dbId,
                                long tableId, long partitionId, long indexId, long tabletId,
                                int schemaHash, long version) {
        super(resourceInfo, backendId, TTaskType.CHECK_CONSISTENCY, dbId, tableId, partitionId, indexId, tabletId);

        this.schemaHash = schemaHash;
        this.version = version;
    }

    public int getSchemaHash() {
        return schemaHash;
    }

    public long getVersion() {
        return version;
    }

    public TCheckConsistencyReq toThrift() {
        TCheckConsistencyReq request = new TCheckConsistencyReq(tabletId, schemaHash, version, 0L);
        return request;
    }
}
