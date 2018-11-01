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

import org.apache.doris.thrift.TCancelDeleteDataReq;
import org.apache.doris.thrift.TTaskType;

public class CancelDeleteTask extends AgentTask {
    private int schemaHash;
    private long version;
    private long versionHash;

    public CancelDeleteTask(long backendId, long dbId, long tableId, long partitionId, long indexId,
                            long tabletId, int schemaHash, long version, long versionHash) {
        super(null, backendId, TTaskType.CANCEL_DELETE, dbId, tableId, partitionId, indexId, tabletId);

        this.schemaHash = schemaHash;
        this.version = version;
        this.versionHash = versionHash;
    }

    public TCancelDeleteDataReq toThrift() {
        TCancelDeleteDataReq request = new TCancelDeleteDataReq(tabletId, schemaHash, version, versionHash);
        return request;
    }

    public int getSchemaHash() {
        return schemaHash;
    }

    public long getVersion() {
        return version;
    }

    public long getVersionHash() {
        return versionHash;
    }
}
