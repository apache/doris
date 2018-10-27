// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

package com.baidu.palo.common.proc;

import com.baidu.palo.catalog.Database;
import com.baidu.palo.catalog.Replica;
import com.baidu.palo.catalog.Tablet;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.Arrays;

/*
 * SHOW PROC /dbs/dbId/tableId/partitions/partitionId/indexId/tabletId
 * show replicas' detail info within a tablet
 */
public class ReplicasProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("ReplicaId").add("BackendId").add("Version").add("VersionHash")
            .add("DataSize").add("RowCount").add("State").add("VersionCount")
            .build();

    private Database db;
    private Tablet tablet;

    public ReplicasProcNode(Database db, Tablet tablet) {
        this.db = db;
        this.tablet = tablet;
    }

    @Override
    public ProcResult fetchResult() {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(tablet);

        db.readLock();
        try {
            BaseProcResult result = new BaseProcResult();
            result.setNames(TITLE_NAMES);
            for (Replica replica : tablet.getReplicas()) {
                // id -- backendId -- version -- versionHash -- dataSize -- rowCount -- state
                result.addRow(Arrays.asList(String.valueOf(replica.getId()),
                                            String.valueOf(replica.getBackendId()),
                                            String.valueOf(replica.getVersion()),
                                            String.valueOf(replica.getVersionHash()),
                                            String.valueOf(replica.getDataSize()),
                                            String.valueOf(replica.getRowCount()),
                                            String.valueOf(replica.getState()),
                                            String.valueOf(replica.getVersionCount())));
            }
            return result;
        } finally {
            db.readUnlock();
        }
    }
}

