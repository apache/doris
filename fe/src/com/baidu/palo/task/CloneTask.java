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

import com.baidu.palo.thrift.TBackend;
import com.baidu.palo.thrift.TCloneReq;
import com.baidu.palo.thrift.TStorageMedium;
import com.baidu.palo.thrift.TTaskType;

import java.util.List;

public class CloneTask extends AgentTask {

    private int schemaHash;
    private List<TBackend> srcBackends;
    private TStorageMedium storageMedium;

    public CloneTask(long backendId, long dbId, long tableId, long partitionId, long indexId,
                     long tabletId, int schemaHash, List<TBackend> srcBackends, TStorageMedium storageMedium) {
        super(null, backendId, TTaskType.CLONE, dbId, tableId, partitionId, indexId, tabletId);
        this.schemaHash = schemaHash;
        this.srcBackends = srcBackends;
        this.storageMedium = storageMedium;
    }

    public TCloneReq toThrift() {
        TCloneReq request = new TCloneReq(tabletId, schemaHash, srcBackends);
        request.setStorage_medium(storageMedium);
        return request;
    }

    public int getSchemaHash() {
        return schemaHash;
    }

    public TStorageMedium getStorageMedium() {
        return storageMedium;
    }
}
