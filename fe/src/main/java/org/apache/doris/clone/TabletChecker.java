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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.Tablet.TabletStatus;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.Daemon;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Set;

/*
 * This checker is responsible for checking all unhealthy tablets.
 * It does not responsible for any scheduler of tablet repairing or balance
 */
public class TabletChecker extends Daemon {
    private static final Logger LOG = LogManager.getLogger(TabletChecker.class);

    private static final long INTERVAL_MS = 60 * 1000L; // 1min

    private Catalog catalog;
    private SystemInfoService infoService;
    private TabletScheduler tabletScheduler;
    private TabletSchedulerStat stat;

    // db id -> (tbl id -> partition id)
    // priority of replicas of partitions in this table will be set to VERY_HIGH if not healthy
    private com.google.common.collect.Table<Long, Long, Set<Long>> prios = HashBasedTable.create();

    public TabletChecker(Catalog catalog, SystemInfoService infoService, TabletScheduler tabletScheduler,
            TabletSchedulerStat stat) {
        super("tablet checker", INTERVAL_MS);
        this.catalog = catalog;
        this.infoService = infoService;
        this.tabletScheduler = tabletScheduler;
        this.stat = stat;
    }

    public void addPrios(long dbId, long tblId, List<Long> partitionIds) {
        Preconditions.checkArgument(!partitionIds.isEmpty());
        synchronized (prios) {
            Set<Long> parts = prios.get(dbId, tblId);
            if (parts == null) {
                parts = Sets.newHashSet();
                prios.put(dbId, tblId, parts);
            }
            parts.addAll(partitionIds);
        }

        // we also need to change the priority of tablets which are already in
        tabletScheduler.changePriorityOfTablets(dbId, tblId, partitionIds);
    }

    /*
     * For each cycle, TabletChecker will check all OlapTable's tablet.
     * If a tablet is not healthy, a TabletInfo will be created and sent to TabletScheduler for repairing.
     */
    @Override
    protected void runOneCycle() {
        boolean schedulerHasTask = !tabletScheduler.isEmpty();

        // if scheduler has tasks, we only check tablets in prios
        checkTablets(schedulerHasTask);
        stat.counterTabletCheckRound.incrementAndGet();

        LOG.info(stat.incrementalBrief());
    }

    private void checkTablets(boolean skipNonPrios) {
        long start = System.currentTimeMillis();
        long totalTabletNum = 0;
        long unhealthyTabletNum = 0;
        long addToSchedulerTabletNum = 0;

        List<Long> dbIds = catalog.getDbIds();
        for (Long dbId : dbIds) {
            Database db = catalog.getDb(dbId);
            if (db == null) {
                continue;
            }

            if (db.isInfoSchemaDb()) {
                continue;
            }

            db.readLock();
            try {
                for (Table table : db.getTables()) {
                    if (!table.needCheck()) {
                        continue;
                    }

                    OlapTable olapTbl = (OlapTable) table;
                    for (Partition partition : olapTbl.getPartitions()) {
                        boolean isInPrios = isInPrios(dbId, table.getId(), partition.getId());
                        if (skipNonPrios && !isInPrios) {
                            // skip non prios
                            continue;
                        }
                        boolean prioPartIsHealthy = true;
                        for (MaterializedIndex idx : partition.getMaterializedIndices()) {
                            for (Tablet tablet : idx.getTablets()) {
                                totalTabletNum++;
                                Pair<TabletStatus, TabletInfo.Priority> statusWithPrio = tablet.getHealthStatusWithPriority(
                                        infoService,
                                        db.getClusterName(),
                                        partition.getCommittedVersion(),
                                        partition.getCommittedVersion(),
                                        olapTbl.getPartitionInfo().getReplicationNum(partition.getId()));

                                if (statusWithPrio.first == TabletStatus.HEALTHY) {
                                    continue;
                                } else if (isInPrios) {
                                    statusWithPrio.second = TabletInfo.Priority.VERY_HIGH;
                                    prioPartIsHealthy = false;
                                }

                                unhealthyTabletNum++;

                                if (!tablet.readyToBeRepaired(statusWithPrio.second)) {
                                    continue;
                                }

                                TabletInfo tabletInfo = new TabletInfo(db.getClusterName(),
                                        db.getId(), olapTbl.getId(),
                                        partition.getId(), idx.getId(), tablet.getId(),
                                        olapTbl.getSchemaHashByIndexId(idx.getId()),
                                        olapTbl.getPartitionInfo().getDataProperty(partition.getId()).getStorageMedium(),
                                        System.currentTimeMillis());
                                tabletInfo.setOrigPriority(statusWithPrio.second);

                                if (tabletScheduler.addTablet(tabletInfo, false /* not force */)) {
                                    addToSchedulerTabletNum++;
                                }
                            }
                        }

                        if (prioPartIsHealthy) {
                            // if all replicas in this partition are healthy, remove this partition from
                            // priorities.
                            removeHealthyPartFromPrios(db.getId(), olapTbl.getId(), partition.getId());
                        }
                    }
                }
            } finally {
                db.readUnlock();
            }
        } // end for dbs

        long cost = System.currentTimeMillis() - start;

        stat.counterTabletCheckCostMs.addAndGet(cost);
        stat.counterTabletChecked.addAndGet(totalTabletNum);
        stat.counterUnhealthyTabletNum.addAndGet(unhealthyTabletNum);
        stat.counterTabletAddToBeScheduled.addAndGet(addToSchedulerTabletNum);

        LOG.info("finished to check tablets. unhealth/total/added: {}/{}/{}, skip non prio: {}, cost: {} ms",
                 unhealthyTabletNum, totalTabletNum, addToSchedulerTabletNum, skipNonPrios, cost);
    }

    public void removeHealthyPartFromPrios(Long dbId, Long tblId, Long partId) {
       synchronized (prios) {
            Set<Long> parts = prios.get(dbId, tblId);
            if (parts == null) {
                return;
            }
            parts.remove(partId);
            if (parts.isEmpty()) {
                prios.remove(dbId, tblId);
            }
       }
    }

    private boolean isInPrios(long dbId, long tblId, long partId) {
        synchronized (prios) {
            if (prios.contains(dbId, tblId)) {
                return prios.get(dbId, tblId).contains(partId);
            }
            return false;
        }
    }
}
