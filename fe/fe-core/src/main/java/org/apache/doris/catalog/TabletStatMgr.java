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
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TTabletStat;
import org.apache.doris.thrift.TTabletStatResult;

import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

/*
 * TabletStatMgr is for collecting tablet(replica) statistics from backends.
 * Each FE will collect by itself.
 */
public class TabletStatMgr extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(TabletStatMgr.class);

    private ForkJoinPool taskPool = new ForkJoinPool(Runtime.getRuntime().availableProcessors());

    public TabletStatMgr() {
        super("tablet stat mgr", Config.tablet_stat_update_interval_second * 1000);
    }

    @Override
    protected void runAfterCatalogReady() {
        ImmutableMap<Long, Backend> backends = Env.getCurrentSystemInfo().getIdToBackend();
        long start = System.currentTimeMillis();
        taskPool.submit(() -> {
            // no need to get tablet stat if backend is not alive
            backends.values().stream().filter(Backend::isAlive).parallel().forEach(backend -> {
                BackendService.Client client = null;
                TNetworkAddress address = null;
                boolean ok = false;
                try {
                    address = new TNetworkAddress(backend.getHost(), backend.getBePort());
                    client = ClientPool.backendPool.borrowObject(address);
                    TTabletStatResult result = client.getTabletStat();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("get tablet stat from backend: {}, num: {}", backend.getId(),
                                result.getTabletsStatsSize());
                    }
                    updateTabletStat(backend.getId(), result);
                    ok = true;
                } catch (Throwable e) {
                    LOG.warn("task exec error. backend[{}]", backend.getId(), e);
                }

                try {
                    if (ok) {
                        ClientPool.backendPool.returnObject(address, client);
                    } else {
                        ClientPool.backendPool.invalidateObject(address, client);
                    }
                } catch (Throwable e) {
                    LOG.warn("client pool recyle error. backend[{}]", backend.getId(), e);
                }
            });
        }).join();
        if (LOG.isDebugEnabled()) {
            LOG.debug("finished to get tablet stat of all backends. cost: {} ms",
                    (System.currentTimeMillis() - start));
        }

        // after update replica in all backends, update index row num
        start = System.currentTimeMillis();
        List<Long> dbIds = Env.getCurrentInternalCatalog().getDbIds();
        for (Long dbId : dbIds) {
            Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                continue;
            }
            List<Table> tableList = db.getTables();
            for (Table table : tableList) {
                // Will process OlapTable and MTMV
                if (!table.isManagedTable()) {
                    continue;
                }
                OlapTable olapTable = (OlapTable) table;

                Long tableDataSize = 0L;
                Long tableTotalReplicaDataSize = 0L;

                Long tableRemoteDataSize = 0L;

                Long tableReplicaCount = 0L;

                Long tableRowCount = 0L;

                // Use try write lock to avoid such cases
                //    Time1: Thread1 hold read lock for 5min
                //    Time2: Thread2 want to add write lock, then it will be the first element in lock queue
                //    Time3: Thread3 want to add read lock, but it will not, because thread 2 want to add write lock
                // In this case, thread 3 has to wait more than 5min, because it has to wait thread 2 to add
                // write lock and release write lock and thread 2 has to wait thread 1 to release read lock
                if (!table.tryWriteLockIfExist(3000, TimeUnit.MILLISECONDS)) {
                    continue;
                }
                Map<Long, Long> indexesRowCount = new HashMap<>();
                try {
                    for (Partition partition : olapTable.getAllPartitions()) {
                        long version = partition.getVisibleVersion();
                        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                            long indexRowCount = 0L;
                            for (Tablet tablet : index.getTablets()) {

                                Long tabletDataSize = 0L;
                                Long tabletRemoteDataSize = 0L;

                                Long tabletRowCount = 0L;

                                for (Replica replica : tablet.getReplicas()) {
                                    if (replica.checkVersionCatchUp(version, false)
                                            && replica.getRowCount() > tabletRowCount) {
                                        tabletRowCount = replica.getRowCount();
                                    }

                                    if (replica.getDataSize() > tabletDataSize) {
                                        tabletDataSize = replica.getDataSize();
                                    }
                                    tableTotalReplicaDataSize += replica.getDataSize();

                                    if (replica.getRemoteDataSize() > tabletRemoteDataSize) {
                                        tabletRemoteDataSize = replica.getRemoteDataSize();
                                    }
                                    tableReplicaCount++;
                                }

                                tableDataSize += tabletDataSize;
                                tableRemoteDataSize += tabletRemoteDataSize;

                                tableRowCount += tabletRowCount;
                                indexRowCount += tabletRowCount;
                            } // end for tablets
                            index.setRowCount(indexRowCount);
                            indexesRowCount.put(index.getId(), indexRowCount);
                        } // end for indices
                    } // end for partitions

                    olapTable.setStatistics(new OlapTable.Statistics(db.getName(), table.getName(),
                            tableDataSize, tableTotalReplicaDataSize,
                            tableRemoteDataSize, tableReplicaCount, tableRowCount, 0L, 0L, indexesRowCount));

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("finished to set row num for table: {} in database: {}",
                                 table.getName(), db.getFullName());
                    }
                } finally {
                    table.writeUnlock();
                }
            }
        }
        LOG.info("finished to update index row num of all databases. cost: {} ms",
                (System.currentTimeMillis() - start));
    }

    private void updateTabletStat(Long beId, TTabletStatResult result) {
        TabletInvertedIndex invertedIndex = Env.getCurrentInvertedIndex();
        if (result.isSetTabletStatList()) {
            for (TTabletStat stat : result.getTabletStatList()) {
                if (invertedIndex.getTabletMeta(stat.getTabletId()) != null) {
                    Replica replica = invertedIndex.getReplica(stat.getTabletId(), beId);
                    if (replica != null) {
                        replica.setDataSize(stat.getDataSize());
                        replica.setRemoteDataSize(stat.getRemoteDataSize());
                        replica.setRowCount(stat.getRowCount());
                        replica.setTotalVersionCount(stat.getTotalVersionCount());
                        replica.setVisibleVersionCount(stat.isSetVisibleVersionCount() ? stat.getVisibleVersionCount()
                                : stat.getTotalVersionCount());
                    }
                }
            }
        } else {
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
                replica.setDataSize(entry.getValue().getDataSize());
                replica.setRowCount(entry.getValue().getRowCount());
            }
        }
    }
}
