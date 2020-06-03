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

package org.apache.doris.catalog;

import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TTabletStat;
import org.apache.doris.thrift.TTabletStatResult;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/*
 * TabletStatMgr is for collecting tablet(replica) statistics from backends.
 * Each FE will collect by itself.
 */
public class TabletStatMgr extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(TabletStatMgr.class);

    public TabletStatMgr() {
        super("tablet stat mgr", Config.tablet_stat_update_interval_second * 1000);
    }

    @Override
    protected void runAfterCatalogReady() {
        ImmutableMap<Long, Backend> backends = Catalog.getCurrentSystemInfo().getIdToBackend();

        long start = System.currentTimeMillis();
        for (Backend backend : backends.values()) {
            BackendService.Client client = null;
            TNetworkAddress address = null;
            boolean ok = false;
            try {
                address = new TNetworkAddress(backend.getHost(), backend.getBePort());
                client = ClientPool.backendPool.borrowObject(address);
                TTabletStatResult result = client.getTabletStat();

                LOG.debug("get tablet stat from backend: {}, num: {}", backend.getId(), result.getTabletsStatsSize());
                updateTabletStat(backend.getId(), result);

                ok = true;
            } catch (Exception e) {
                LOG.warn("task exec error. backend[{}]", backend.getId(), e);
            } finally {
                if (ok) {
                    ClientPool.backendPool.returnObject(address, client);
                } else {
                    ClientPool.backendPool.invalidateObject(address, client);
                }
            }
        }
        LOG.info("finished to get tablet stat of all backends. cost: {} ms",
                (System.currentTimeMillis() - start));

        // after update replica in all backends, update index row num
        start = System.currentTimeMillis();
        List<Long> dbIds = Catalog.getCurrentCatalog().getDbIds();
        for (Long dbId : dbIds) {
            Database db = Catalog.getCurrentCatalog().getDb(dbId);
            if (db == null) {
                continue;
            }
            List<Table> tableList = null;
            db.readLock();
            try {
                tableList = db.getTables();
            } finally {
                db.readUnlock();
            }

            for (Table table : tableList) {
                if (table.getType() != TableType.OLAP) {
                    continue;
                }

                OlapTable olapTable = (OlapTable) table;
                table.writeLock();
                try {
                    for (Partition partition : olapTable.getAllPartitions()) {
                        long version = partition.getVisibleVersion();
                        long versionHash = partition.getVisibleVersionHash();
                        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                            long indexRowCount = 0L;
                            for (Tablet tablet : index.getTablets()) {
                                long tabletRowCount = 0L;
                                for (Replica replica : tablet.getReplicas()) {
                                    if (replica.checkVersionCatchUp(version, versionHash, false)
                                            && replica.getRowCount() > tabletRowCount) {
                                        tabletRowCount = replica.getRowCount();
                                    }
                                }
                                indexRowCount += tabletRowCount;
                            } // end for tablets
                            index.setRowCount(indexRowCount);
                        } // end for indices
                    } // end for partitions
                    LOG.debug("finished to set row num for table: {} in database: {}",
                             table.getName(), db.getFullName());
                } finally {
                    table.writeUnlock();
                }
            }
        }
        LOG.info("finished to update index row num of all databases. cost: {} ms",
                (System.currentTimeMillis() - start));
    }

    private void updateTabletStat(Long beId, TTabletStatResult result) {
        TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();

        for (Map.Entry<Long, TTabletStat> entry : result.getTabletsStats().entrySet()) {
            if (invertedIndex.getTabletMeta(entry.getKey()) == null) {
                // the replica is obsolete, ignore it.
                continue;
            }
            Replica replica = invertedIndex.getReplica(entry.getKey(), beId);
            if (replica == null) {
                // replica may be deleted from catalog, ignore it.
                continue;
            }
            // TODO(cmy) no db lock protected. I think it is ok even we get wrong row num
            replica.updateStat(entry.getValue().getDataSize(), entry.getValue().getRowNum());
        }
    }
}
