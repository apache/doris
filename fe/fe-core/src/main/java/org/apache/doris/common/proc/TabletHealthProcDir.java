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

import org.apache.doris.catalog.ColocateGroupSchema;
import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.thrift.TTaskType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/*
 * show proc "/cluster_health/tablet_health";
 */

public class TabletHealthProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("DbId").add("DbName").add("TabletNum").add("HealthyNum").add("ReplicaMissingNum")
            .add("VersionIncompleteNum").add("ReplicaRelocatingNum").add("RedundantNum")
            .add("ReplicaMissingForTagNum")
            .add("ForceRedundantNum").add("ColocateMismatchNum").add("ColocateRedundantNum")
            .add("NeedFurtherRepairNum").add("UnrecoverableNum").add("ReplicaCompactionTooSlowNum")
            .add("InconsistentNum").add("OversizeNum").add("CloningNum")
            .build();

    private Env env;

    private ForkJoinPool taskPool = new ForkJoinPool();

    public TabletHealthProcDir(Env env) {
        Preconditions.checkNotNull(env);
        this.env = env;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String dbIdStr) throws AnalysisException {
        try {
            long dbId = Long.parseLong(dbIdStr);
            return env.getInternalCatalog().getDb(dbId).map(IncompleteTabletsProcNode::new).orElse(null);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid db id format: " + dbIdStr);
        }
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        List<DBTabletStatistic> statistics = taskPool.submit(() ->
                env.getInternalCatalog().getDbIds().parallelStream()
                    // skip information_schema database
                    .flatMap(id -> Stream.of(id == 0 ? null : env.getInternalCatalog().getDbNullable(id)))
                    .filter(Objects::nonNull).map(DBTabletStatistic::new)
                    // sort by dbName
                    .sorted(Comparator.comparing(db -> db.db.getFullName())).collect(Collectors.toList())
        ).join();

        List<List<String>> rows = new ArrayList<>(statistics.size() + 1);
        for (DBTabletStatistic statistic : statistics) {
            rows.add(statistic.toRow());
        }
        rows.add(statistics.stream().reduce(new DBTabletStatistic(), DBTabletStatistic::reduce).toRow());
        return new BaseProcResult(TITLE_NAMES, rows);
    }

    static class DBTabletStatistic {
        boolean summary;
        DatabaseIf<TableIf> db;
        int dbNum;
        int tabletNum;
        int healthyNum;
        int replicaMissingNum;
        int versionIncompleteNum;
        int replicaRelocatingNum;
        int redundantNum;
        int replicaMissingForTagNum;
        int forceRedundantNum;
        int colocateMismatchNum;
        int colocateRedundantNum;
        int needFurtherRepairNum;
        int unrecoverableNum;
        int replicaCompactionTooSlowNum;
        int inconsistentNum;
        int oversizeNum;
        int cloningNum;

        Set<Long> replicaMissingTabletIds;
        Set<Long> versionIncompleteTabletIds;
        Set<Long> replicaRelocatingTabletIds;
        Set<Long> redundantTabletIds;
        Set<Long> replicaMissingForTagTabletIds;
        Set<Long> forceRedundantTabletIds;
        Set<Long> colocateMismatchTabletIds;
        Set<Long> colocateRedundantTabletIds;
        Set<Long> needFurtherRepairTabletIds;
        Set<Long> unrecoverableTabletIds;
        Set<Long> replicaCompactionTooSlowTabletIds;
        Set<Long> inconsistentTabletIds;
        Set<Long> oversizeTabletIds;
        Set<Long> cloningTabletIds;

        DBTabletStatistic() {
            this.summary = true;
        }

        DBTabletStatistic(DatabaseIf db) {
            Preconditions.checkNotNull(db);
            this.summary = false;
            this.db = db;
            this.dbNum = 1;
            this.replicaMissingTabletIds = new HashSet<>();
            this.versionIncompleteTabletIds = new HashSet<>();
            this.replicaRelocatingTabletIds = new HashSet<>();
            this.redundantTabletIds = new HashSet<>();
            this.replicaMissingForTagTabletIds = new HashSet<>();
            this.forceRedundantTabletIds = new HashSet<>();
            this.colocateMismatchTabletIds = new HashSet<>();
            this.colocateRedundantTabletIds = new HashSet<>();
            this.needFurtherRepairTabletIds = new HashSet<>();
            this.unrecoverableTabletIds = new HashSet<>();
            this.replicaCompactionTooSlowTabletIds = new HashSet<>();
            this.inconsistentTabletIds = new HashSet<>();
            this.oversizeTabletIds = new HashSet<>();
            this.cloningTabletIds = new HashSet<>();

            SystemInfoService infoService = Env.getCurrentSystemInfo();
            ColocateTableIndex colocateTableIndex = Env.getCurrentColocateIndex();
            List<Long> aliveBeIds = infoService.getAllBackendIds(true);
            this.cloningTabletIds = AgentTaskQueue.getTask(db.getId(), TTaskType.CLONE)
                    .stream().map(AgentTask::getTabletId).collect(Collectors.toSet());
            this.cloningNum = cloningTabletIds.size();
            this.db.getTables().stream().filter(t -> t != null && t.getType() == Table.TableType.OLAP).forEach(t -> {
                OlapTable olapTable = (OlapTable) t;
                ColocateTableIndex.GroupId groupId = colocateTableIndex.isColocateTable(olapTable.getId())
                        ? colocateTableIndex.getGroup(olapTable.getId()) : null;
                olapTable.readLock();
                try {
                    List<Partition> partitions = Lists.newArrayList(olapTable.getAllPartitions());
                    List<Long> visibleVersions = null;
                    try {
                        visibleVersions = Partition.getVisibleVersions(partitions);
                    } catch (RpcException e) {
                        throw new RuntimeException("get version from meta service failed:" + e.getMessage());
                    }
                    for (int j = 0; j < partitions.size(); j++) {
                        Partition partition = partitions.get(j);
                        long visibleVersion = visibleVersions.get(j);
                        ReplicaAllocation replicaAlloc = olapTable.getPartitionInfo()
                                .getReplicaAllocation(partition.getId());
                        for (MaterializedIndex materializedIndex : partition.getMaterializedIndices(
                                MaterializedIndex.IndexExtState.VISIBLE)) {
                            List<Tablet> tablets = materializedIndex.getTablets();
                            for (int i = 0; i < tablets.size(); ++i) {
                                Tablet tablet = tablets.get(i);
                                ++tabletNum;
                                Tablet.TabletStatus res = null;
                                if (groupId != null) {
                                    ColocateGroupSchema groupSchema = colocateTableIndex.getGroupSchema(groupId);
                                    if (groupSchema != null) {
                                        replicaAlloc = groupSchema.getReplicaAlloc();
                                    }
                                    Set<Long> backendsSet = colocateTableIndex.getTabletBackendsByGroup(groupId, i);
                                    res = tablet.getColocateHealth(visibleVersion, replicaAlloc, backendsSet).status;
                                } else {
                                    res = tablet.getHealth(infoService, visibleVersion, replicaAlloc,
                                            aliveBeIds).status;
                                }
                                switch (res) { // CHECKSTYLE IGNORE THIS LINE: missing switch default
                                    case HEALTHY:
                                        healthyNum++;
                                        break;
                                    case REPLICA_MISSING:
                                        replicaMissingNum++;
                                        replicaMissingTabletIds.add(tablet.getId());
                                        break;
                                    case VERSION_INCOMPLETE:
                                        versionIncompleteNum++;
                                        versionIncompleteTabletIds.add(tablet.getId());
                                        break;
                                    case REPLICA_RELOCATING:
                                        replicaRelocatingNum++;
                                        replicaRelocatingTabletIds.add(tablet.getId());
                                        break;
                                    case REDUNDANT:
                                        redundantNum++;
                                        redundantTabletIds.add(tablet.getId());
                                        break;
                                    case FORCE_REDUNDANT:
                                        forceRedundantNum++;
                                        forceRedundantTabletIds.add(tablet.getId());
                                        break;
                                    case REPLICA_MISSING_FOR_TAG:
                                        replicaMissingForTagNum++;
                                        replicaMissingForTagTabletIds.add(tablet.getId());
                                        break;
                                    case COLOCATE_MISMATCH:
                                        colocateMismatchNum++;
                                        colocateMismatchTabletIds.add(tablet.getId());
                                        break;
                                    case COLOCATE_REDUNDANT:
                                        colocateRedundantNum++;
                                        colocateRedundantTabletIds.add(tablet.getId());
                                        break;
                                    case NEED_FURTHER_REPAIR:
                                        needFurtherRepairNum++;
                                        needFurtherRepairTabletIds.add(tablet.getId());
                                        break;
                                    case UNRECOVERABLE:
                                        unrecoverableNum++;
                                        unrecoverableTabletIds.add(tablet.getId());
                                        break;
                                    case REPLICA_COMPACTION_TOO_SLOW:
                                        // use more strict mode to show REPLICA_COMPACTION_TOO_SLOW state for tablet
                                        break;
                                }
                                if (!tablet.isConsistent()) {
                                    inconsistentNum++;
                                    inconsistentTabletIds.add(tablet.getId());
                                }
                                if (tablet.getDataSize(true) > Config.min_bytes_indicate_replica_too_large) {
                                    oversizeNum++;
                                    oversizeTabletIds.add(tablet.getId());
                                }
                                for (Replica replica : tablet.getReplicas()) {
                                    if (replica.tooBigVersionCount()) {
                                        replicaCompactionTooSlowNum++;
                                        replicaCompactionTooSlowTabletIds.add(tablet.getId());
                                        break;
                                    }
                                }
                            } // end for tablets
                        } // end for indices
                    } // end for partitions
                } finally {
                    olapTable.readUnlock();
                }
            });
        }

        DBTabletStatistic reduce(DBTabletStatistic other) {
            if (this.summary) {
                this.dbNum += other.dbNum;
                this.tabletNum += other.tabletNum;
                this.healthyNum += other.healthyNum;
                this.replicaMissingNum += other.replicaMissingNum;
                this.versionIncompleteNum += other.versionIncompleteNum;
                this.replicaRelocatingNum += other.replicaRelocatingNum;
                this.redundantNum += other.redundantNum;
                this.forceRedundantNum += other.forceRedundantNum;
                this.replicaMissingForTagNum += other.replicaMissingForTagNum;
                this.colocateMismatchNum += other.colocateMismatchNum;
                this.colocateRedundantNum += other.colocateRedundantNum;
                this.needFurtherRepairNum += other.needFurtherRepairNum;
                this.unrecoverableNum += other.unrecoverableNum;
                this.replicaCompactionTooSlowNum += other.replicaCompactionTooSlowNum;
                this.inconsistentNum += other.inconsistentNum;
                this.oversizeNum += other.oversizeNum;
                this.cloningNum += other.cloningNum;
                return this;
            } else if (other.summary) {
                return other.reduce(this);
            } else {
                return new DBTabletStatistic().reduce(this).reduce(other);
            }
        }

        List<String> toRow() {
            List<Object> row = new ArrayList<>(TITLE_NAMES.size());
            if (summary) {
                row.add("Total");
                row.add(dbNum);
            } else {
                row.add(db.getId());
                row.add(db.getFullName());
            }

            row.add(tabletNum);
            row.add(healthyNum);
            row.add(replicaMissingNum);
            row.add(versionIncompleteNum);
            row.add(replicaRelocatingNum);
            row.add(redundantNum);
            row.add(replicaMissingForTagNum);
            row.add(forceRedundantNum);
            row.add(colocateMismatchNum);
            row.add(colocateRedundantNum);
            row.add(needFurtherRepairNum);
            row.add(unrecoverableNum);
            row.add(replicaCompactionTooSlowNum);
            row.add(inconsistentNum);
            row.add(oversizeNum);
            row.add(cloningNum);
            return row.stream().map(String::valueOf).collect(Collectors.toList());
        }
    }
}
