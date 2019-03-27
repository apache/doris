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

package org.apache.doris.common.proc;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.TimeUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/*
 * SHOW PROC /dbs/dbId/tableId/partitions/partitionId/indexId
 * show tablets' detail info within an index
 */
public class TabletsProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("TabletId").add("ReplicaId").add("BackendId").add("Version")
            .add("VersionHash").add("LstSuccessVersion").add("LstSuccessVersionHash")
            .add("LstFailedVersion").add("LstFailedVersionHash").add("LstFailedTime")
            .add("DataSize").add("RowCount").add("State")
            .add("LstConsistencyCheckTime").add("CheckVersion").add("CheckVersionHash")
            .add("VersionCount").add("PathHash")
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
                    tabletInfo.add(-1); // replica id
                    tabletInfo.add(-1); // backend id
                    tabletInfo.add("N/A"); // host name
                    tabletInfo.add(-1); // version
                    tabletInfo.add(-1); // version hash
                    tabletInfo.add(-1); // lst success version
                    tabletInfo.add(-1); // lst success version hash
                    tabletInfo.add(-1); // lst failed version
                    tabletInfo.add(-1); // lst failed version hash
                    tabletInfo.add(-1); // lst failed time
                    tabletInfo.add(-1); // data size
                    tabletInfo.add(-1); // row count
                    tabletInfo.add("N/A"); // state
                    tabletInfo.add(-1); // lst consistency check time
                    tabletInfo.add(-1); // check version
                    tabletInfo.add(-1); // check version hash
                    tabletInfo.add(-1); // version count
                    tabletInfo.add(-1); // path hash

                    tabletInfos.add(tabletInfo);
                } else {
                    for (Replica replica : tablet.getReplicas()) {
                        List<Comparable> tabletInfo = new ArrayList<Comparable>();
                        // tabletId -- replicaId -- backendId -- version -- versionHash -- dataSize -- rowCount -- state
                        tabletInfo.add(tabletId);
                        tabletInfo.add(replica.getId());
                        long backendId = replica.getBackendId();
                        tabletInfo.add(replica.getBackendId());
                        tabletInfo.add(replica.getVersion());
                        tabletInfo.add(replica.getVersionHash());
                        tabletInfo.add(replica.getLastSuccessVersion());
                        tabletInfo.add(replica.getLastSuccessVersionHash());
                        tabletInfo.add(replica.getLastFailedVersion());
                        tabletInfo.add(replica.getLastFailedVersionHash());
                        tabletInfo.add(TimeUtils.longToTimeString(replica.getLastFailedTimestamp()));
                        tabletInfo.add(replica.getDataSize());
                        tabletInfo.add(replica.getRowCount());
                        tabletInfo.add(replica.getState());

                        tabletInfo.add(TimeUtils.longToTimeString(tablet.getLastCheckTime()));
                        tabletInfo.add(tablet.getCheckedVersion());
                        tabletInfo.add(tablet.getCheckedVersionHash());
                        tabletInfo.add(replica.getVersionCount());
                        tabletInfo.add(replica.getPathHash());

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

        TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
        List<Replica> replicas = invertedIndex.getReplicasByTabletId(tabletId);
        return new ReplicasProcNode(replicas);
    }
}

