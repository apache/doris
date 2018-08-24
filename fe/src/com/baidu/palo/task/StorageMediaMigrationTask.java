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

import com.baidu.palo.thrift.TStorageMedium;
import com.baidu.palo.thrift.TStorageMediumMigrateReq;
import com.baidu.palo.thrift.TTaskType;

public class StorageMediaMigrationTask extends AgentTask {

    private int schemaHash;
    private TStorageMedium toStorageMedium;

    public StorageMediaMigrationTask(long backendId, long tabletId, int schemaHash,
                                     TStorageMedium toStorageMedium) {
        super(null, backendId, tabletId, TTaskType.STORAGE_MEDIUM_MIGRATE, -1L, -1L, -1L, -1L, tabletId);

        this.schemaHash = schemaHash;
        this.toStorageMedium = toStorageMedium;
    }

    public TStorageMediumMigrateReq toThrift() {
        TStorageMediumMigrateReq request = new TStorageMediumMigrateReq(tabletId, schemaHash, toStorageMedium);
        return request;
    }

    public int getSchemaHash() {
        return schemaHash;
    }

    public TStorageMedium getToStorageMedium() {
        return toStorageMedium;
    }
}
