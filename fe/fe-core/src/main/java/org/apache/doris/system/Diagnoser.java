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

package org.apache.doris.system;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;

import com.google.common.collect.Lists;
import org.json.simple.JSONObject;

import java.util.List;

// This is a util class to diagnose the Doris system
public class Diagnoser {
    // To diagnose a given tablet and return the info and issues about it
    // - tablet exist:
    // - tablet id
    // - database
    // - table
    // - partition
    // - materialized view
    // - replica info: {"replica_id" : "backend id"}
    // - replica num
    // - ReplicaBackendStatus
    // - ReplicaVersionStatus
    // - ReplicaStatus
    // - ReplicaCompactionStatus
    //
    public static List<List<String>> diagnoseTablet(long tabletId) {
        List<List<String>> results = Lists.newArrayList();
        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
        if (tabletMeta == null) {
            results.add(Lists.newArrayList("TabletExist", "No", ""));
            return results;
        }
        results.add(Lists.newArrayList("TabletExist", "Yes", ""));
        results.add(Lists.newArrayList("TabletId", String.valueOf(tabletId), ""));
        // database
        Database db = Env.getCurrentInternalCatalog().getDbNullable(tabletMeta.getDbId());
        if (db == null) {
            results.add(Lists.newArrayList("Database", "Not exist", ""));
            return results;
        }
        results.add(Lists.newArrayList("Database", db.getFullName() + ": " + db.getId(), ""));
        // table
        OlapTable tbl = (OlapTable) db.getTableNullable(tabletMeta.getTableId());
        if (tbl == null) {
            results.add(Lists.newArrayList("Table", "Not exist", ""));
            return results;
        }
        results.add(Lists.newArrayList("Table", tbl.getName() + ": " + tbl.getId(), ""));
        // partition
        Partition partition = tbl.getPartition(tabletMeta.getPartitionId());
        if (partition == null) {
            results.add(Lists.newArrayList("Partition", "Not exist", ""));
            return results;
        }
        results.add(Lists.newArrayList("Partition", partition.getName() + ": " + partition.getId(), ""));
        // materialized index
        MaterializedIndex mIndex = partition.getIndex(tabletMeta.getIndexId());
        if (mIndex == null) {
            results.add(Lists.newArrayList("MaterializedIndex", "Not exist", ""));
            return results;
        }
        results.add(Lists.newArrayList("MaterializedIndex",
                tbl.getIndexNameById(mIndex.getId()) + ": " + mIndex.getId(), ""));
        // replica info
        Tablet tablet = mIndex.getTablet(tabletId);
        List<Replica> replicas = tablet.getReplicas();
        JSONObject jobj = new JSONObject();
        for (Replica replica : replicas) {
            jobj.put(replica.getId(), replica.getBackendId());
        }
        results.add(Lists.newArrayList("Replicas(ReplicaId -> BackendId)", jobj.toJSONString(), ""));
        // replica
        short replicaNum = tbl.getPartitionInfo().getReplicaAllocation(partition.getId()).getTotalReplicaNum();
        if (replicas.size() != replicaNum) {
            results.add(Lists.newArrayList("ReplicasNum", "Replica num is "
                    + replicas.size() + ", expected: " + replicaNum, ""));
        } else {
            results.add(Lists.newArrayList("ReplicasNum", "OK", ""));
        }

        SystemInfoService infoService = Env.getCurrentSystemInfo();
        StringBuilder backendErr = new StringBuilder();
        StringBuilder versionErr = new StringBuilder();
        StringBuilder statusErr = new StringBuilder();
        StringBuilder compactionErr = new StringBuilder();
        long visibleVersion = partition.getVisibleVersion();
        for (Replica replica : replicas) {
            // backend
            do {
                Backend be = infoService.getBackend(replica.getBackendId());
                if (be == null) {
                    backendErr.append("Backend " + replica.getBackendId() + " does not exist. ");
                    break;
                }
                if (!be.isAlive()) {
                    backendErr.append("Backend " + replica.getBackendId() + " is not alive. ");
                    break;
                }
                if (be.isDecommissioned()) {
                    backendErr.append("Backend " + replica.getBackendId() + " is decommission. ");
                    break;
                }
                if (!be.isLoadAvailable()) {
                    backendErr.append("Backend " + replica.getBackendId() + " is not load available. ");
                    break;
                }
                if (!be.isQueryAvailable()) {
                    backendErr.append("Backend " + replica.getBackendId() + " is not query available. ");
                    break;
                }
                if (be.diskExceedLimit()) {
                    backendErr.append("Backend " + replica.getBackendId() + " has no space left. ");
                    break;
                }
            } while (false);
            // version
            if (replica.getVersion() != visibleVersion) {
                versionErr.append("Replica on backend " + replica.getBackendId() + "'s version ("
                        + replica.getVersion() + ") does not equal"
                        + " to partition visible version (" + visibleVersion + ")");
            } else if (replica.getLastFailedVersion() != -1) {
                versionErr.append("Replica on backend " + replica.getBackendId() + "'s last failed version is "
                        + replica.getLastFailedVersion());
            }
            // status
            if (!replica.isAlive() || replica.isUserDrop()) {
                statusErr.append("Replica on backend " + replica.getBackendId() + "'s state is " + replica.getState()
                        + ", and is bad: " + (replica.isBad() ? "Yes" : "No")
                        + ", and is going to drop: " + (replica.isUserDrop() ? "Yes" : "No"));
            }
            if (replica.tooBigVersionCount()) {
                compactionErr.append("Replica on backend " + replica.getBackendId() + "'s version count is too high: "
                        + replica.getVisibleVersionCount());
            }
        }
        results.add(Lists.newArrayList("ReplicaBackendStatus", (backendErr.length() == 0
                ? "OK" : backendErr.toString()), ""));
        results.add(Lists.newArrayList("ReplicaVersionStatus", (versionErr.length() == 0
                ? "OK" : versionErr.toString()), ""));
        results.add(Lists.newArrayList("ReplicaStatus", (statusErr.length() == 0
                ? "OK" : statusErr.toString()), ""));
        results.add(Lists.newArrayList("ReplicaCompactionStatus", (compactionErr.length() == 0
                ? "OK" : compactionErr.toString()), ""));
        return results;
    }
}
