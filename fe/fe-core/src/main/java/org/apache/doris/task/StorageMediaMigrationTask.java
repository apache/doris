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

import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageMediumMigrateReq;
import org.apache.doris.thrift.TTaskType;

import com.google.common.base.Strings;

public class StorageMediaMigrationTask extends AgentTask {

    private int schemaHash;
    private TStorageMedium toStorageMedium;
    // if dataDir is specified, the toStorageMedium is meaning less
    private String dataDir;

    public StorageMediaMigrationTask(long backendId, long tabletId, int schemaHash,
                                     TStorageMedium toStorageMedium) {
        super(null, backendId, TTaskType.STORAGE_MEDIUM_MIGRATE, -1L, -1L, -1L, -1L, tabletId);

        this.schemaHash = schemaHash;
        this.toStorageMedium = toStorageMedium;
    }

    public TStorageMediumMigrateReq toThrift() {
        TStorageMediumMigrateReq request = new TStorageMediumMigrateReq(tabletId, schemaHash, toStorageMedium);
        if (!Strings.isNullOrEmpty(dataDir)) {
            request.setDataDir(dataDir);
        }
        return request;
    }

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public int getSchemaHash() {
        return schemaHash;
    }

    public TStorageMedium getToStorageMedium() {
        return toStorageMedium;
    }
}
