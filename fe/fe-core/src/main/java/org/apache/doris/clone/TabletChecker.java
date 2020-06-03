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

package org.apache.doris.clone;

import org.apache.doris.analysis.AdminCancelRepairTableStmt;
import org.apache.doris.analysis.AdminRepairTableStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Partition.PartitionState;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.Tablet.TabletStatus;
import org.apache.doris.clone.TabletScheduler.AddResult;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Table.Cell;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * This checker is responsible for checking all unhealthy tablets.
 * It does not responsible for any scheduler of tablet repairing or balance
 */
public class TabletChecker extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(TabletChecker.class);

    private static final long CHECK_INTERVAL_MS = 20 * 1000L; // 20 second

    private Catalog catalog;
    private SystemInfoService infoService;
    private TabletScheduler tabletScheduler;
    private TabletSchedulerStat stat;

    // db id -> (tbl id -> PrioPart)
    // priority of replicas of partitions in this table will be set to VERY_HIGH if not healthy
    private com.google.common.collect.Table<Long, Long, Set<PrioPart>> prios = HashBasedTable.create();
    
    // represent a partition which need to be repaired preferentially
    public static class PrioPart {
        public long partId;
        public long addTime;
        public long timeoutMs;

        public PrioPart(long partId, long addTime, long timeoutMs) {
            this.partId = partId;
            this.addTime = addTime;
            this.timeoutMs = timeoutMs;
        }

        public boolean isTimeout() {
            return System.currentTimeMillis() - addTime > timeoutMs;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof PrioPart)) {
                return false;
            }
            return partId == ((PrioPart) obj).partId;
        }

        @Override
        public int hashCode() {
            return Long.valueOf(partId).hashCode();
        }
    }

    public static class RepairTabletInfo {
        public long dbId;
        public long tblId;
        public List<Long> partIds;

        public RepairTabletInfo(Long dbId, Long tblId, List<Long> partIds) {
            this.dbId = dbId;
            this.tblId = tblId;
            this.partIds = partIds;
        }
    }

    public TabletChecker(Catalog catalog, SystemInfoService infoService, TabletScheduler tabletScheduler,
            TabletSchedulerStat stat) {
        super("tablet checker", CHECK_INTERVAL_MS);
        this.catalog = catalog;
        this.infoService = infoService;
        this.tabletScheduler = tabletScheduler;
        this.stat = stat;
    }

    private void addPrios(RepairTabletInfo repairTabletInfo, long timeoutMs) {
        Preconditions.checkArgument(!repairTabletInfo.partIds.isEmpty());
        long currentTime = System.currentTimeMillis();
        synchronized (prios) {
            Set<PrioPart> parts = prios.get(repairTabletInfo.dbId, repairTabletInfo.tblId);
            if (parts == null) {
                parts = Sets.newHashSet();
                prios.put(repairTabletInfo.dbId, repairTabletInfo.tblId, parts);
            }

            for (long partId : repairTabletInfo.partIds) {
                PrioPart prioPart = new PrioPart(partId, currentTime, timeoutMs);
                parts.add(prioPart);
            }
        }

        // we also need to change the priority of tablets which are already in
        tabletScheduler.changeTabletsPriorityToVeryHigh(repairTabletInfo.dbId, repairTabletInfo.tblId, repairTabletInfo.partIds);
    }

    private void removePrios(RepairTabletInfo repairTabletInfo) {
        Preconditions.checkArgument(!repairTabletInfo.partIds.isEmpty());
        synchronized (prios) {
            Map<Long, Set<PrioPart>> tblMap = prios.row(repairTabletInfo.dbId);
            if (tblMap == null) {
                return;
            }
            Set<PrioPart> parts = tblMap.get(repairTabletInfo.tblId);
            if (parts == null) {
                return;
            }
            for (long partId : repairTabletInfo.partIds) {
                parts.remove(new PrioPart(partId, -1, -1));
            }
            if (parts.isEmpty()) {
                tblMap.remove(repairTabletInfo.tblId);
            }
        }

    }

    /*
     * For each cycle, TabletChecker will check all OlapTable's tablet.
     * If a tablet is not healthy, a TabletInfo will be created and sent to TabletScheduler for repairing.
     */
    @Override
    protected void runAfterCatalogReady() {
        int pendingNum = tabletScheduler.getPendingNum();
        int runningNum = tabletScheduler.getRunningNum();
        if (pendingNum > Config.max_scheduling_tablets
                || runningNum > Config.max_scheduling_tablets) {
            LOG.info("too many tablets are being scheduled. pending: {}, running: {}, limit: {}. skip check",
                    pendingNum, runningNum, Config.max_scheduling_tablets);
            return;
        }
        
        checkTablets();

        removePriosIfNecessary();

        stat.counterTabletCheckRound.incrementAndGet();
        LOG.info(stat.incrementalBrief());
    }

    private void checkTablets() {
        long start = System.currentTimeMillis();
        long totalTabletNum = 0;
        long unhealthyTabletNum = 0;
        long addToSchedulerTabletNum = 0;
        long tabletInScheduler = 0;
        long tabletNotReady = 0;

        List<Long> dbIds = catalog.getDbIds();
        OUT: for (Long dbId : dbIds) {
            Database db = catalog.getDb(dbId);
            if (db == null) {
                continue;
            }

            if (db.isInfoSchemaDb()) {
                continue;
            }

            int availableBackendsNum = infoService.getClusterBackendIds(db.getClusterName(), true).size();
            db.readLock();
            List<Table> tableList = null;
            List<Long> aliveBeIdsInCluster = null;
            try {
                aliveBeIdsInCluster = infoService.getClusterBackendIds(db.getClusterName(), true);
                tableList = db.getTables();
            } finally {
                db.readUnlock();
            }

            for (Table table : tableList) {
                table.readLock();
                try {
                    if (!table.needSchedule()) {
                        continue;
                    }

                    OlapTable olapTbl = (OlapTable) table;
                    for (Partition partition : olapTbl.getAllPartitions()) {
                        if (partition.getState() != PartitionState.NORMAL) {
                            // when alter job is in FINISHING state, partition state will be set to NORMAL,
                            // and we can schedule the tablets in it.
                            continue;
                        }
                        boolean isInPrios = isInPrios(dbId, table.getId(), partition.getId());
                        boolean prioPartIsHealthy = true;
                        /*
                         * Tablet in SHADOW index can not be repaired of balanced
                         */
                        for (MaterializedIndex idx : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                            for (Tablet tablet : idx.getTablets()) {
                                totalTabletNum++;

                                if (tabletScheduler.containsTablet(tablet.getId())) {
                                    tabletInScheduler++;
                                    continue;
                                }

                                Pair<TabletStatus, TabletSchedCtx.Priority> statusWithPrio = tablet.getHealthStatusWithPriority(
                                        infoService,
                                        db.getClusterName(),
                                        partition.getVisibleVersion(),
                                        partition.getVisibleVersionHash(),
                                        olapTbl.getPartitionInfo().getReplicationNum(partition.getId()),
                                        aliveBeIdsInCluster);

                                if (statusWithPrio.first == TabletStatus.HEALTHY) {
                                    // Only set last status check time when status is healthy.
                                    tablet.setLastStatusCheckTime(start);
                                    continue;
                                } else if (isInPrios) {
                                    statusWithPrio.second = TabletSchedCtx.Priority.VERY_HIGH;
                                    prioPartIsHealthy = false;
                                }

                                unhealthyTabletNum++;

                                if (!tablet.readyToBeRepaired(statusWithPrio.second)) {
                                    tabletNotReady++;
                                    continue;
                                }

                                TabletSchedCtx tabletCtx = new TabletSchedCtx(
                                        TabletSchedCtx.Type.REPAIR,
                                        db.getClusterName(),
                                        db.getId(), olapTbl.getId(),
                                        partition.getId(), idx.getId(), tablet.getId(),
                                        System.currentTimeMillis());
                                // the tablet status will be set again when being scheduled
                                tabletCtx.setTabletStatus(statusWithPrio.first);
                                tabletCtx.setOrigPriority(statusWithPrio.second);

                                AddResult res = tabletScheduler.addTablet(tabletCtx, false /* not force */);
                                if (res == AddResult.LIMIT_EXCEED) {
                                    LOG.info("number of scheduling tablets in tablet scheduler"
                                            + " exceed to limit. stop tablet checker");
                                    break OUT;
                                } else if (res == AddResult.ADDED) {
                                    addToSchedulerTabletNum++;
                                }
                            }
                        } // indices

                        if (prioPartIsHealthy && isInPrios) {
                            // if all replicas in this partition are healthy, remove this partition from
                            // priorities.
                            LOG.debug("partition is healthy, remove from prios: {}-{}-{}",
                                    db.getId(), olapTbl.getId(), partition.getId());
                            removePrios(new RepairTabletInfo(db.getId(),
                                    olapTbl.getId(), Lists.newArrayList(partition.getId())));
                        }
                    } // partitions
                } finally {
                    table.readUnlock();
                }
            } // tables
        } // end for dbs

        long cost = System.currentTimeMillis() - start;

        stat.counterTabletCheckCostMs.addAndGet(cost);
        stat.counterTabletChecked.addAndGet(totalTabletNum);
        stat.counterUnhealthyTabletNum.addAndGet(unhealthyTabletNum);
        stat.counterTabletAddToBeScheduled.addAndGet(addToSchedulerTabletNum);

        LOG.info("finished to check tablets. unhealth/total/added/in_sched/not_ready: {}/{}/{}/{}/{}, cost: {} ms",
                unhealthyTabletNum, totalTabletNum, addToSchedulerTabletNum, tabletInScheduler, tabletNotReady, cost);
    }

    private boolean isInPrios(long dbId, long tblId, long partId) {
        synchronized (prios) {
            if (prios.contains(dbId, tblId)) {
                return prios.get(dbId, tblId).contains(new PrioPart(partId, -1, -1));
            }
            return false;
        }
    }

    // remove partition from prios if:
    // 1. timeout
    // 2. meta not found
    private void removePriosIfNecessary() {
        com.google.common.collect.Table<Long, Long, Set<PrioPart>> copiedPrios = null;
        synchronized (prios) {
            copiedPrios = HashBasedTable.create(prios);
        }
        List<Pair<Long, Long>> deletedPrios = Lists.newArrayList();
        Iterator<Map.Entry<Long, Map<Long, Set<PrioPart>>>> iter = copiedPrios.rowMap().entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Long, Map<Long, Set<PrioPart>>> dbEntry = iter.next();
            long dbId = dbEntry.getKey();
            Database db = Catalog.getCurrentCatalog().getDb(dbId);
            if (db == null) {
                iter.remove();
                continue;
            }

            db.readLock();
            try {
                Iterator<Map.Entry<Long, Set<PrioPart>>> jter = dbEntry.getValue().entrySet().iterator();
                while (jter.hasNext()) {
                    Map.Entry<Long, Set<PrioPart>> tblEntry = jter.next();
                    long tblId = tblEntry.getKey();
                    OlapTable tbl = (OlapTable) db.getTable(tblId);
                    if (tbl == null) {
                        deletedPrios.add(Pair.create(dbId, tblId));
                        continue;
                    }
                    tbl.readLock();
                    try {
                        Set<PrioPart> parts = tblEntry.getValue();
                        parts = parts.stream().filter(p -> (tbl.getPartition(p.partId) != null && !p.isTimeout())).collect(
                                Collectors.toSet());
                        if (parts.isEmpty()) {
                            deletedPrios.add(Pair.create(dbId, tblId));
                        }
                    } finally {
                        tbl.readUnlock();
                    }
                }

                if (dbEntry.getValue().isEmpty()) {
                    iter.remove();
                }
            } finally {
                db.readUnlock();
            }
        }
        for (Pair<Long, Long> prio : deletedPrios) {
            copiedPrios.remove(prio.first, prio.second);
        }
        prios = copiedPrios;
    }

    /*
     * handle ADMIN REPAIR TABLE stmt send by user.
     * This operation will add specified tables into 'prios', and tablets of this table will be set VERY_HIGH
     * when being scheduled.
     */
    public void repairTable(AdminRepairTableStmt stmt) throws DdlException {
        RepairTabletInfo repairTabletInfo = getRepairTabletInfo(stmt.getDbName(), stmt.getTblName(), stmt.getPartitions());
        addPrios(repairTabletInfo, stmt.getTimeoutS());
        LOG.info("repair database: {}, table: {}, partition: {}", repairTabletInfo.dbId, repairTabletInfo.tblId, repairTabletInfo.partIds);
    }

    /*
     * handle ADMIN CANCEL REPAIR TABLE stmt send by user.
     * This operation will remove the specified partitions from 'prios'
     */
    public void cancelRepairTable(AdminCancelRepairTableStmt stmt) throws DdlException {
        RepairTabletInfo repairTabletInfo = getRepairTabletInfo(stmt.getDbName(), stmt.getTblName(), stmt.getPartitions());
        removePrios(repairTabletInfo);
        LOG.info("cancel repair database: {}, table: {}, partition: {}", repairTabletInfo.dbId, repairTabletInfo.tblId, repairTabletInfo.partIds);
    }

    public int getPrioPartitionNum() {
        int count = 0;
        synchronized (prios) {
            for (Set<PrioPart> set : prios.values()) {
                count += set.size();
            }
        }
        return count;
    }

    public List<List<String>> getPriosInfo() {
        List<List<String>> infos = Lists.newArrayList();
        synchronized (prios) {
            for (Cell<Long, Long, Set<PrioPart>> cell : prios.cellSet()) {
                for (PrioPart part : cell.getValue()) {
                    List<String> row = Lists.newArrayList();
                    row.add(cell.getRowKey().toString());
                    row.add(cell.getColumnKey().toString());
                    row.add(String.valueOf(part.partId));
                    row.add(String.valueOf(part.timeoutMs - (System.currentTimeMillis() - part.addTime)));
                    infos.add(row);
                }
            }
        }
        return infos;
    }

    public static RepairTabletInfo getRepairTabletInfo(String dbName, String tblName, List<String> partitions) throws DdlException {
        Catalog catalog = Catalog.getCurrentCatalog();
        Database db = catalog.getDb(dbName);
        if (db == null) {
            throw new DdlException("Database " + dbName + " does not exist");
        }

        long dbId = db.getId();
        long tblId = -1;
        List<Long> partIds = Lists.newArrayList();
        Table tbl = db.getTable(tblName);
        if (tbl == null || tbl.getType() != TableType.OLAP) {
            throw new DdlException("Table does not exist or is not OLAP table: " + tblName);
        }
        tbl.readLock();
        try {
            tblId = tbl.getId();
            OlapTable olapTable = (OlapTable) tbl;

            if (partitions == null || partitions.isEmpty()) {
                partIds = olapTable.getPartitions().stream().map(Partition::getId).collect(Collectors.toList());
            } else {
                for (String partName : partitions) {
                    Partition partition = olapTable.getPartition(partName);
                    if (partition == null) {
                        throw new DdlException("Partition does not exist: " + partName);
                    }
                    partIds.add(partition.getId());
                }
            }
        } finally {
            tbl.readUnlock();
        }

        Preconditions.checkState(tblId != -1);

        return new RepairTabletInfo(dbId, tblId, partIds);
    }
}
