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

import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.ColocateTableIndex.GroupId;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/*
 * show proc "/colocation_group";
 */
public class ColocationGroupProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("GroupId").add("GroupName").add("TableIds")
            .add("BucketsNum").add("ReplicaAllocation").add("DistCols").add("IsStable")
            .add("ErrorMsg").build();

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String groupIdStr) throws AnalysisException {
        String[] parts = groupIdStr.split("\\.");
        if (parts.length != 2) {
            throw new AnalysisException("Invalid group id: " + groupIdStr);
        }

        long dbId = -1;
        long grpId = -1;
        try {
            dbId = Long.valueOf(parts[0]);
            grpId = Long.valueOf(parts[1]);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid group id: " + groupIdStr);
        }

        GroupId groupId = new GroupId(dbId, grpId);
        ColocateTableIndex index = Env.getCurrentColocateIndex();
        Map<Tag, List<List<Long>>> beSeqs = index.getBackendsPerBucketSeq(groupId);
        boolean showBackendIdsColumn = false;
        if ((beSeqs == null || beSeqs.isEmpty()) && Config.isCloudMode()) {
            // In cloud mode, legacy backend sequence metadata may be empty.
            // Use only local replica metadata for proc display. This path must not
            // resolve cloud backends because that may auto-start a compute group.
            beSeqs = getCloudBackendSeqsFromTablets(groupId, index);
            // A null tag key means there is no per-compute-group information (e.g.
            // local-style replicas), so fall back to a single merged BackendIds column.
            showBackendIdsColumn = beSeqs.containsKey(null);
        }
        return new ColocationGroupBackendSeqsProcNode(beSeqs, showBackendIdsColumn);
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        ColocateTableIndex index = Env.getCurrentColocateIndex();
        List<List<String>> infos = index.getInfos();
        result.setRows(infos);
        return result;
    }

    private Map<Tag, List<List<Long>>> getCloudBackendSeqsFromTablets(GroupId groupId, ColocateTableIndex index) {
        Map<Tag, List<List<Long>>> backendsSeq = Maps.newHashMap();
        List<Long> tableIds = index.getAllTableIds(groupId);
        for (Long tableId : tableIds) {
            long dbId = groupId.dbId;
            if (dbId == 0) {
                Long tableDbId = index.getDbIdByTblIdNullable(groupId, tableId);
                if (tableDbId == null) {
                    continue;
                }
                dbId = tableDbId;
            }
            Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                continue;
            }
            Table table = db.getTableNullable(tableId);
            if (!(table instanceof OlapTable)) {
                continue;
            }
            backendsSeq = getCloudBackendSeqsFromTable((OlapTable) table);
            if (!backendsSeq.isEmpty()) {
                return backendsSeq;
            }
        }
        return backendsSeq;
    }

    private Map<Tag, List<List<Long>>> getCloudBackendSeqsFromTable(OlapTable olapTable) {
        // Snapshot replicas (ordered by bucket) under the table lock only. Resolving the
        // per-compute-group placement of colocate cloud replicas calls into
        // CloudSystemInfoService / the colocate index, which must run outside the table
        // lock to avoid nested lock acquisition.
        List<List<Replica>> bucketReplicas = Lists.newArrayList();
        olapTable.readLock();
        try {
            Partition firstPartition = null;
            for (Partition partition : olapTable.getAllPartitions()) {
                firstPartition = partition;
                break;
            }
            if (firstPartition == null) {
                return Maps.newLinkedHashMap();
            }
            MaterializedIndex baseIndex = firstPartition.getBaseIndex();
            for (Tablet tablet : baseIndex.getTablets()) {
                bucketReplicas.add(new ArrayList<>(tablet.getReplicas()));
            }
        } finally {
            olapTable.readUnlock();
        }

        // Resolve each replica's per-compute-group placement outside the table lock. In
        // cloud mode a replica is hashed to a different BE in each compute group, so build
        // a separate bucket sequence per compute group. Merging across groups (picking an
        // arbitrary first BE) would mix BEs from different compute groups into one bucket
        // sequence, which is meaningless. For colocate cloud tables placement is computed
        // on the fly; otherwise it comes from the cached clusterId -> backendId map (or an
        // empty scope key for local-style replicas).
        List<List<Map<String, Long>>> tabletReplicaBackends = Lists.newArrayListWithCapacity(bucketReplicas.size());
        Set<String> scopeKeys = Sets.newLinkedHashSet();
        // Shared across all replicas in this proc call so each compute group's backend
        // list is fetched only once (colocate placement is resolved per compute group).
        Map<String, List<Backend>> computeGroupBackendCache = Maps.newHashMap();
        for (List<Replica> replicas : bucketReplicas) {
            List<Map<String, Long>> replicaBackends = new ArrayList<>();
            for (Replica replica : replicas) {
                Map<String, Long> clusterToBackend =
                        replica.getClusterToBackendForProcDisplay(computeGroupBackendCache);
                replicaBackends.add(clusterToBackend);
                scopeKeys.addAll(clusterToBackend.keySet());
            }
            tabletReplicaBackends.add(replicaBackends);
        }

        Map<String, List<List<Long>>> seqByScopeKey = Maps.newLinkedHashMap();
        for (String scopeKey : scopeKeys) {
            List<List<Long>> bucketSeq = Lists.newArrayListWithCapacity(tabletReplicaBackends.size());
            boolean hasBackend = false;
            for (List<Map<String, Long>> replicaBackends : tabletReplicaBackends) {
                List<Long> bucketBackends = new ArrayList<>();
                for (Map<String, Long> clusterToBackend : replicaBackends) {
                    Long backendId = clusterToBackend.get(scopeKey);
                    if (backendId == null || backendId < 0) {
                        continue;
                    }
                    bucketBackends.add(backendId);
                    hasBackend = true;
                }
                bucketSeq.add(bucketBackends);
            }
            if (hasBackend) {
                seqByScopeKey.put(scopeKey, bucketSeq);
            }
        }

        // Resolve scope keys to display tags (also outside the table lock): name
        // resolution acquires CloudSystemInfoService's lock.
        Map<Tag, List<List<Long>>> backendsSeq = Maps.newLinkedHashMap();
        for (Map.Entry<String, List<List<Long>>> entry : seqByScopeKey.entrySet()) {
            backendsSeq.put(scopeKeyToTag(entry.getKey()), entry.getValue());
        }
        return backendsSeq;
    }

    // Map a proc-display scope key to its display tag. An empty key means there is no
    // per-compute-group information (local-style replicas), rendered as null so the
    // caller falls back to a single merged BackendIds column. Otherwise the key is a
    // cloud compute group id, shown as a compute group tag (resolved to its name).
    private Tag scopeKeyToTag(String scopeKey) {
        if (Strings.isNullOrEmpty(scopeKey)) {
            return null;
        }
        String computeGroupName = scopeKey;
        try {
            String name = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                    .getClusterNameByClusterId(scopeKey);
            if (!Strings.isNullOrEmpty(name)) {
                computeGroupName = name;
            }
        } catch (Exception e) {
            // Fall back to the raw compute group id if name resolution is unavailable.
        }
        return Tag.createNotCheck(Tag.COMPUTE_GROUP_NAME, computeGroupName);
    }
}
