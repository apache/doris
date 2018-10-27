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

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.catalog.MaterializedIndex;
import com.baidu.palo.catalog.Replica;
import com.baidu.palo.catalog.Tablet;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.util.ListComparator;
import com.baidu.palo.common.util.TimeUtils;
import com.baidu.palo.system.Backend;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/*
 * SHOW PROC /dbs/dbId/tableId/partitions/partitionId/indexId
 * show tablets' detail info within an index
 */
public class TabletsProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("TabletId").add("ReplicaId").add("BackendId").add("HostName").add("Version")
            .add("VersionHash").add("DataSize").add("RowCount").add("State")
            .add("LastConsistencyCheckTime").add("CheckVersion").add("CheckVersionHash")
            .add("VersionCount")
            .build();

    private Database db;
    private MaterializedIndex index;
    
    public TabletsProcDir(Database db, MaterializedIndex index) {
        this.db = db;
        this.index = index;
    }
    
    @Override
    public ProcResult fetchResult() {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(index);

        List<List<Comparable>> tabletInfos = new ArrayList<List<Comparable>>();
        db.readLock();
        try {
            // get infos
            for (Tablet tablet : index.getTablets()) {
                long tabletId = tablet.getId();
                if (tablet.getReplicas().size() == 0) {
                    List<Comparable> tabletInfo = new ArrayList<Comparable>();
                    tabletInfo.add(tabletId);
                    tabletInfo.add(-1);
                    tabletInfo.add(-1);
                    tabletInfo.add(-1);
                    tabletInfo.add(-1);
                    tabletInfo.add(-1);
                    tabletInfo.add(-1);
                    tabletInfo.add("N/A");
                    tabletInfo.add("N/A");
                    tabletInfo.add(-1);
                    tabletInfo.add(-1);
                    tabletInfo.add(-1);

                    tabletInfos.add(tabletInfo);
                } else {
                    for (Replica replica : tablet.getReplicas()) {
                        List<Comparable> tabletInfo = new ArrayList<Comparable>();
                        // tabletId -- replicaId -- backendId -- version -- versionHash -- dataSize -- rowCount -- state
                        tabletInfo.add(tabletId);
                        tabletInfo.add(replica.getId());
                        long backendId = replica.getBackendId();
                        tabletInfo.add(replica.getBackendId());
                        Backend backend = Catalog.getCurrentSystemInfo().getBackend(backendId);
                        // backend may be dropped concurrently, ignore it.
                        if (backend == null) {
                            continue;
                        }
                        String hostName = null;
                        try {
                            InetAddress address = InetAddress.getByName(backend.getHost());
                            hostName = address.getHostName();
                        } catch (UnknownHostException e) {
                            continue;
                        }
                        tabletInfo.add(hostName);
                        tabletInfo.add(replica.getVersion());
                        tabletInfo.add(replica.getVersionHash());
                        tabletInfo.add(replica.getDataSize());
                        tabletInfo.add(replica.getRowCount());
                        tabletInfo.add(replica.getState());

                        tabletInfo.add(TimeUtils.longToTimeString(tablet.getLastCheckTime()));
                        tabletInfo.add(tablet.getCheckedVersion());
                        tabletInfo.add(tablet.getCheckedVersionHash());
                        tabletInfo.add(replica.getVersionCount());

                        tabletInfos.add(tabletInfo);
                    }
                }
            }
        } finally {
            db.readUnlock();
        }

        // sort by tabletId, replicaId
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(0, 1);
        Collections.sort(tabletInfos, comparator);

        // set result
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        for (int i = 0; i < tabletInfos.size(); i++) {
            List<Comparable> info = tabletInfos.get(i);
            List<String> row = new ArrayList<String>(info.size());
            for (int j = 0; j < info.size(); j++) {
                row.add(info.get(j).toString());
            }
            result.addRow(row);
        }
        return result;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String tabletIdStr) throws AnalysisException {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(index);

        long tabletId = -1L;
        try {
            tabletId = Long.valueOf(tabletIdStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid tablet id format: " + tabletIdStr);
        }

        db.readLock();
        try {
            Tablet tablet = index.getTablet(tabletId);
            if (tablet == null) {
                throw new AnalysisException("Tablet[" + tabletId + "] does not exist.");
            }
            return new ReplicasProcNode(db, tablet);
        } finally {
            db.readUnlock();
        }
    }

}

