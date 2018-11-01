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

import org.apache.doris.thrift.TRecoverTabletReq;
import org.apache.doris.thrift.TTaskType;

public class RecoverTabletTask extends AgentTask {

    private long version;
    private long versionHash;
    private int schemaHash;

    public RecoverTabletTask(long backendId, long tabletId, long version, long versionHash, int schemaHash) {
        super(null, backendId, TTaskType.RECOVER_TABLET, -1L, -1L, -1L, -1L, tabletId, tabletId);
        this.version = version;
        this.versionHash = versionHash;
        this.schemaHash = schemaHash;
    }
    
    public TRecoverTabletReq toThrift() {
        TRecoverTabletReq recoverTabletReq = new TRecoverTabletReq();
        recoverTabletReq.setTablet_id(tabletId);
        recoverTabletReq.setVersion(version);
        recoverTabletReq.setVersion_hash(versionHash);
        recoverTabletReq.setSchema_hash(schemaHash);
        return recoverTabletReq;
    }
}