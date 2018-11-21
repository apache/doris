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

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Sets;

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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Set;

/*
 * This scanner is responsible for scanning all unhealthy tablets.
 * It does not responsible for any scheduler of tablet repairing or balance
 */
public class TabletScanner extends Daemon {
    private static final Logger LOG = LogManager.getLogger(TabletScanner.class);

    private static long INTERVAL_MS = 60 * 1000L; // 1min

    private Catalog catalog;
    private SystemInfoService infoService;
    private TabletFactory tabletFactory;

    // db id -> (tbl id -> partition id)
    // priority of replicas of partitions in this table will be set to VERY_HIGH if not healthy
    private com.google.common.collect.Table<Long, Long, Set<Long>> priors = HashBasedTable.create();

    public TabletScanner() {
        super("tablet scanner", INTERVAL_MS);
        catalog = Catalog.getCurrentCatalog();
        infoService = Catalog.getCurrentSystemInfo();
        tabletFactory = new TabletFactory();
    }

    public void addPriors(long dbId, long tblId, List<Long> partitionIds) {
        Preconditions.checkArgument(!partitionIds.isEmpty());
        synchronized (priors) {
            Set<Long> parts = priors.get(dbId, tblId);
            if (parts == null) {
                parts = Sets.newHashSet();
                priors.put(dbId, tblId, parts);
            }
            parts.addAll(partitionIds);
        }
    }

    /*
     * For each cycle, TabletScanner will scan all OlapTable's tablet.
     * If a tablet is not healthy, a TabletInfo will be created and sent to TabletFactory for repairing.
     */
    @Override
    protected void runOneCycle() {

        if (!tabletFactory.isEmpty()) {
            LOG.info("tablet factory has unfinished tasks. skip this round of checking");
            return;
        }

        scanTablets();
    }

    private void scanTablets() {
        List<Long> dbIds = catalog.getDbIds();
        int unhealthyTabletNum = 0;
        int totalTabletNum = 0;

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
                    if (!table.needScan()) {
                        continue;
                    }

                    OlapTable olapTbl = (OlapTable) table;
                    for (Partition partition : olapTbl.getPartitions()) {
                        boolean healthyPrioriPart = true;
                        for (MaterializedIndex idx : partition.getMaterializedIndices()) {
                            for (Tablet tablet : idx.getTablets()) {
                                totalTabletNum++;
                                Pair<TabletStatus, TabletInfo.Priority> statusWithPrior =
                                        tablet.getHealthStatusWithPriority(infoService,
                                                     db.getClusterName(),
                                                     partition.getCommittedVersion(),
                                                     partition.getCommittedVersion(),
                                                     olapTbl.getPartitionInfo().getReplicationNum(partition.getId()));
                                    
                                if (statusWithPrior.first == TabletStatus.HEALTHY) {
                                    continue;
                                } else if (isInPriors(db.getId(), table.getId(), partition.getId())) {
                                    statusWithPrior.second = TabletInfo.Priority.VERY_HIGH;
                                    healthyPrioriPart = false;
                                }

                                TabletInfo tabletInfo = new TabletInfo(db.getClusterName(),
                                        db.getId(), table.getId(),
                                        partition.getId(), idx.getId(), tablet.getId(),
                                        olapTbl.getSchemaHashByIndexId(idx.getId()),
                                        olapTbl.getPartitionInfo().getDataProperty(partition.getId()).getStorageMedium(),
                                        System.currentTimeMillis());
                                tabletInfo.setPriority(statusWithPrior.second);
                                if (tabletFactory.addTablet(tabletInfo)) {
                                    unhealthyTabletNum++;
                                }
                            }
                        }

                        if (healthyPrioriPart) {
                            removeHealthyPartFromPriori(dbId, table.getId(), partition.getId());
                        }
                    }
                }
            } finally {
                db.readUnlock();
            }
        } // end for dbs

        LOG.info("finished to scanning tablets. unhealth/healthy: {}/{}", unhealthyTabletNum, totalTabletNum);
    }

    public void removeHealthyPartFromPriori(Long dbId, Long tblId, Long partId) {
       synchronized (priors) {
            Set<Long> parts = priors.get(dbId, tblId);
            if (parts == null) {
                return;
            }
            parts.remove(partId);
            if (parts.isEmpty()) {
                priors.remove(dbId, tblId);
            }
       }
    }

    private boolean isInPriors(long dbId, long tblId, long partId) {
        synchronized (priors) {
            if (priors.contains(dbId, tblId)) {
                return priors.get(dbId, tblId).contains(partId);
            }
            return false;
        }
    }
}
