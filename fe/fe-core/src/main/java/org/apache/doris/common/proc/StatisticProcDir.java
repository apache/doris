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
import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.Tablet.TabletStatus;
import org.apache.doris.clone.TabletSchedCtx.Priority;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.thrift.TTaskType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StatisticProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("DbId").add("DbName").add("TableNum").add("PartitionNum")
            .add("IndexNum").add("TabletNum").add("ReplicaNum").add("UnhealthyTabletNum")
            .add("InconsistentTabletNum").add("CloningTabletNum").add("BadTabletNum")
            .add("CompactionTooSlowTabletNum").add("OversizeTabletNum")
            .build();
    private static final Logger LOG = LogManager.getLogger(StatisticProcDir.class);

    private Catalog catalog;

    public StatisticProcDir(Catalog catalog) {
        Preconditions.checkNotNull(catalog);
        this.catalog = catalog;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        List<DBStatistic> statistics = catalog.getDbIds().parallelStream()
                // skip information_schema database
                .flatMap(id -> Stream.of(id == 0 ? null : catalog.getDbNullable(id)))
                .filter(Objects::nonNull).map(DBStatistic::new)
                // sort by dbName
                .sorted(Comparator.comparing(db -> db.db.getFullName()))
                .collect(Collectors.toList());

        List<List<String>> rows = new ArrayList<>(statistics.size() + 1);
        for (DBStatistic statistic : statistics) {
            rows.add(statistic.toRow());
        }
        rows.add(statistics.stream().reduce(new DBStatistic(), DBStatistic::reduce).toRow());

        return new BaseProcResult(TITLE_NAMES, rows);
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String dbIdStr) throws AnalysisException {
        try {
            long dbId = Long.parseLong(dbIdStr);
            return catalog.getDb(dbId).map(IncompleteTabletsProcNode::new).orElse(null);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid db id format: " + dbIdStr);
        }
    }

    static class DBStatistic {
        boolean summary;
        Database db;
        int dbNum;
        int tableNum;
        int partitionNum;
        int indexNum;
        int tabletNum;
        int replicaNum;
        int unhealthyTabletNum;
        int inconsistentTabletNum;
        int cloningTabletNum;
        int badTabletNum;
        int compactionTooSlowTabletNum;
        int oversizeTabletNum;

        Set<Long> unhealthyTabletIds;
        Set<Long> inconsistentTabletIds;
        Set<Long> cloningTabletIds;
        Set<Long> unrecoverableTabletIds;
        Set<Long> compactionTooSlowTabletIds;
        Set<Long> oversizeTabletIds;

        DBStatistic() {
            this.summary = true;
        }

        DBStatistic(Database db) {
            Preconditions.checkNotNull(db);
            this.summary = false;
            this.db = db;
            this.dbNum = 1;
            this.unhealthyTabletIds = new HashSet<>();
            this.inconsistentTabletIds = new HashSet<>();
            this.unrecoverableTabletIds = new HashSet<>();
            this.cloningTabletIds = AgentTaskQueue.getTask(db.getId(), TTaskType.CLONE)
                    .stream().map(AgentTask::getTabletId).collect(Collectors.toSet());
            this.compactionTooSlowTabletIds = new HashSet<>();
            this.oversizeTabletIds = new HashSet<>();

            SystemInfoService infoService = Catalog.getCurrentSystemInfo();
            ColocateTableIndex colocateTableIndex = Catalog.getCurrentColocateIndex();
            List<Long> aliveBeIdsInCluster = infoService.getClusterBackendIds(db.getClusterName(), true);
            db.getTables().stream().filter(t -> t != null && t.getType() == TableType.OLAP).forEach(t -> {
                ++tableNum;
                OlapTable olapTable = (OlapTable) t;
                ColocateTableIndex.GroupId groupId = colocateTableIndex.isColocateTable(olapTable.getId()) ?
                        colocateTableIndex.getGroup(olapTable.getId()) : null;
                olapTable.readLock();
                try {
                    for (Partition partition : olapTable.getAllPartitions()) {
                        ReplicaAllocation replicaAlloc = olapTable.getPartitionInfo().getReplicaAllocation(partition.getId());
                        ++partitionNum;
                        for (MaterializedIndex materializedIndex : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                            ++indexNum;
                            List<Tablet> tablets = materializedIndex.getTablets();
                            for (int i = 0; i < tablets.size(); ++i) {
                                Tablet tablet = tablets.get(i);
                                ++tabletNum;
                                replicaNum += tablet.getReplicas().size();

                                TabletStatus res = null;
                                if (groupId != null) {
                                    Set<Long> backendsSet = colocateTableIndex.getTabletBackendsByGroup(groupId, i);
                                    res = tablet.getColocateHealthStatus(partition.getVisibleVersion(), replicaAlloc, backendsSet);
                                } else {
                                    Pair<TabletStatus, Priority> pair = tablet.getHealthStatusWithPriority(
                                            infoService, db.getClusterName(),
                                            partition.getVisibleVersion(),
                                            replicaAlloc, aliveBeIdsInCluster);
                                    res = pair.first;
                                }

                                // here we treat REDUNDANT as HEALTHY, for user friendly.
                                if (res != TabletStatus.HEALTHY && res != TabletStatus.REDUNDANT
                                        && res != TabletStatus.COLOCATE_REDUNDANT && res != TabletStatus.NEED_FURTHER_REPAIR
                                        && res != TabletStatus.UNRECOVERABLE) {
                                    unhealthyTabletIds.add(tablet.getId());
                                } else if (res == TabletStatus.UNRECOVERABLE) {
                                    unrecoverableTabletIds.add(tablet.getId());
                                }

                                if (!tablet.isConsistent()) {
                                    inconsistentTabletIds.add(tablet.getId());
                                }

                                if (tablet.getDataSize(true) > Config.min_bytes_indicate_replica_too_large) {
                                    oversizeTabletIds.add(tablet.getId());
                                }

                                for (Replica replica : tablet.getReplicas()) {
                                    if (replica.getVersionCount() > Config.min_version_count_indicate_replica_compaction_too_slow) {
                                        compactionTooSlowTabletIds.add(tablet.getId());
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
            unhealthyTabletNum = unhealthyTabletIds.size();
            inconsistentTabletNum = inconsistentTabletIds.size();
            cloningTabletNum = cloningTabletIds.size();
            badTabletNum = unrecoverableTabletIds.size();
            compactionTooSlowTabletNum = compactionTooSlowTabletIds.size();
            oversizeTabletNum = oversizeTabletIds.size();
        }

        DBStatistic reduce(DBStatistic other) {
            if (this.summary) {
                this.dbNum += other.dbNum;
                this.tableNum += other.tableNum;
                this.partitionNum += other.partitionNum;
                this.indexNum += other.indexNum;
                this.tabletNum += other.tabletNum;
                this.replicaNum += other.replicaNum;
                this.unhealthyTabletNum += other.unhealthyTabletNum;
                this.inconsistentTabletNum += other.inconsistentTabletNum;
                this.cloningTabletNum += other.cloningTabletNum;
                this.badTabletNum += other.badTabletNum;
                this.compactionTooSlowTabletNum += other.compactionTooSlowTabletNum;
                this.oversizeTabletNum += other.oversizeTabletNum;
                return this;
            } else if (other.summary) {
                return other.reduce(this);
            } else {
                return new DBStatistic().reduce(this).reduce(other);
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
            row.add(tableNum);
            row.add(partitionNum);
            row.add(indexNum);
            row.add(tabletNum);
            row.add(replicaNum);
            row.add(unhealthyTabletNum);
            row.add(inconsistentTabletNum);
            row.add(cloningTabletNum);
            row.add(badTabletNum);
            row.add(compactionTooSlowTabletNum);
            row.add(oversizeTabletNum);
            return row.stream().map(String::valueOf).collect(Collectors.toList());
        }
    }
}
