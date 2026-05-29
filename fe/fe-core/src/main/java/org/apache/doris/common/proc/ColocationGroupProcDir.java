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
import org.apache.doris.cloud.catalog.CloudReplica;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.resource.Tag;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
            // Build bucket->backend mapping from current tablet replicas for proc display.
            beSeqs = getCloudBackendSeqsFromTablets(groupId, index);
            showBackendIdsColumn = true;
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
        Map<Tag, List<List<Long>>> backendsSeq = Maps.newHashMap();
        olapTable.readLock();
        try {
            Partition firstPartition = null;
            for (Partition partition : olapTable.getAllPartitions()) {
                firstPartition = partition;
                break;
            }
            if (firstPartition == null) {
                return backendsSeq;
            }
            MaterializedIndex baseIndex = firstPartition.getBaseIndex();
            List<Tablet> tablets = baseIndex.getTablets();
            List<List<Long>> bucketSeq = Lists.newArrayListWithCapacity(tablets.size());
            boolean hasBackend = false;
            for (int i = 0; i < tablets.size(); i++) {
                List<Long> bucketBackends = new ArrayList<>();
                for (Replica replica : tablets.get(i).getReplicas()) {
                    long backendId = replica.getBackendIdWithoutException();
                    if (backendId < 0 && replica instanceof CloudReplica) {
                        backendId = ((CloudReplica) replica).getPrimaryBackendId();
                    }
                    if (backendId < 0) {
                        continue;
                    }
                    bucketBackends.add(backendId);
                    hasBackend = true;
                }
                // Cloud mode should not expose real tag here, use null as placeholder.
                bucketSeq.add(bucketBackends);
            }
            if (hasBackend) {
                backendsSeq.put(null, bucketSeq);
            }
        } finally {
            olapTable.readUnlock();
        }
        return backendsSeq;
    }
}
