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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.CloneTask;
import org.apache.doris.thrift.TFinishTaskRequest;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TTabletInfo;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import mockit.Delegate;
import mockit.Expectations;
import mockit.Mocked;
import static com.google.common.collect.MoreCollectors.onlyElement;

public class RebalanceTest {
    private static final Logger LOG = LogManager.getLogger(RebalanceTest.class);

    @Mocked
    private Catalog catalog;

    private long id = 10086;

    private Database db;
    private OlapTable olapTable;

    private final SystemInfoService systemInfoService = new SystemInfoService();
    private final TabletInvertedIndex invertedIndex = new TabletInvertedIndex();
    private Map<String, ClusterLoadStatistic> statisticMap;

    @Before
    public void setUp() throws AnalysisException {
        db = new Database(1, "test db");
        db.setClusterName(SystemInfoService.DEFAULT_CLUSTER);
        new Expectations() {
            {
                catalog.getDbIds();
                minTimes = 0;
                result = db.getId();

                catalog.getDb(anyLong);
                minTimes = 0;
                result = db;

                Catalog.getCurrentCatalogJournalVersion();
                minTimes = 0;
                result = FeConstants.meta_version;

                catalog.getNextId();
                minTimes = 0;
                result = new Delegate() {
                    long a() {
                        return id++;
                    }
                };

                Catalog.getCurrentSystemInfo();
                minTimes = 0;
                result = systemInfoService;

                Catalog.getCurrentInvertedIndex();
                minTimes = 0;
                result = invertedIndex;

                Catalog.getCurrentGlobalTransactionMgr().getTransactionIDGenerator().getNextTransactionId();
                result = 111;

                Catalog.getCurrentGlobalTransactionMgr().isPreviousTransactionsFinished(anyLong, anyLong, (List<Long>) any);
                result = true;
            }
        };
        // Test mock validation
        Assert.assertEquals(111, Catalog.getCurrentGlobalTransactionMgr().getTransactionIDGenerator().getNextTransactionId());
        Assert.assertTrue(Catalog.getCurrentGlobalTransactionMgr().isPreviousTransactionsFinished(1, 2, Lists.newArrayList(3L)));

        List<Long> beIds = Lists.newArrayList(10001L, 10002L, 10003L, 10004L);
        beIds.forEach(id -> systemInfoService.addBackend(RebalancerTestUtil.createBackend(id, 2048, 0)));

        olapTable = new OlapTable(2, "fake table", new ArrayList<>(), KeysType.DUP_KEYS,
                new RangePartitionInfo(), new HashDistributionInfo());
        db.createTable(olapTable);

        // 1 table, 3 partitions p0,p1,p2
        MaterializedIndex materializedIndex = new MaterializedIndex(olapTable.getId(), null);
        createPartitionsForTable(olapTable, materializedIndex, 3L);
        olapTable.setIndexMeta(materializedIndex.getId(), "fake index", Lists.newArrayList(new Column()),
                0, 0, (short) 0, TStorageType.COLUMN, KeysType.DUP_KEYS);

        // Tablet distribution: we add them to olapTable & build invertedIndex manually
        RebalancerTestUtil.createTablet(invertedIndex, db, olapTable, "p0", TStorageMedium.HDD,
                50000, Lists.newArrayList(10001L, 10002L, 10003L));

        RebalancerTestUtil.createTablet(invertedIndex, db, olapTable, "p1", TStorageMedium.HDD,
                60000, Lists.newArrayList(10001L, 10002L, 10003L));

        RebalancerTestUtil.createTablet(invertedIndex, db, olapTable, "p2", TStorageMedium.HDD,
                70000, Lists.newArrayList(10001L, 10002L, 10003L));

        // be4(10004) doesn't have any replica

        generateStatisticMap();
    }

    private void generateStatisticMap() {
        ClusterLoadStatistic loadStatistic = new ClusterLoadStatistic(SystemInfoService.DEFAULT_CLUSTER,
                systemInfoService, invertedIndex);
        loadStatistic.init();
        statisticMap = Maps.newConcurrentMap();
        statisticMap.put(SystemInfoService.DEFAULT_CLUSTER, loadStatistic);
    }

    private void createPartitionsForTable(OlapTable olapTable, MaterializedIndex index, Long partitionCount) {
        // partition id start from 31
        LongStream.range(0, partitionCount).forEach(idx -> {
            long id = 31 + idx;
            Partition partition = new Partition(id, "p" + idx, index, new HashDistributionInfo());
            olapTable.addPartition(partition);
            olapTable.getPartitionInfo().addPartition(id, new DataProperty(TStorageMedium.HDD),
                    ReplicaAllocation.DEFAULT_ALLOCATION, false);
        });

    }

    @Test
    public void testPartitionRebalancer() {
        Configurator.setLevel("org.apache.doris.clone.PartitionRebalancer", Level.DEBUG);

        // Disable scheduler's rebalancer adding balance task, add balance tasks manually
        Config.disable_balance = true;
        // Create a new scheduler & checker for redundant tablets handling
        // Call runAfterCatalogReady manually instead of starting daemon thread
        TabletSchedulerStat stat = new TabletSchedulerStat();
        PartitionRebalancer rebalancer = new PartitionRebalancer(Catalog.getCurrentSystemInfo(), Catalog.getCurrentInvertedIndex());
        TabletScheduler tabletScheduler = new TabletScheduler(catalog, systemInfoService, invertedIndex, stat, "");
        // The rebalancer inside the scheduler will use this rebalancer, for getToDeleteReplicaId
        Deencapsulation.setField(tabletScheduler, "rebalancer", rebalancer);

        TabletChecker tabletChecker = new TabletChecker(catalog, systemInfoService, tabletScheduler, stat);

        rebalancer.updateLoadStatistic(statisticMap);
        List<TabletSchedCtx> alternativeTablets = rebalancer.selectAlternativeTablets();

        // Run once for update slots info, scheduler won't select balance cuz balance is disabled
        tabletScheduler.runAfterCatalogReady();

        AgentBatchTask batchTask = new AgentBatchTask();
        for (TabletSchedCtx tabletCtx : alternativeTablets) {
            LOG.info("try to schedule tablet {}", tabletCtx.getTabletId());
            try {
                tabletCtx.setStorageMedium(TStorageMedium.HDD);
                tabletCtx.setTablet(olapTable.getPartition(tabletCtx.getPartitionId()).getIndex(tabletCtx.getIndexId()).getTablet(tabletCtx.getTabletId()));
                tabletCtx.setVersionInfo(1, 0, 1, 0);
                tabletCtx.setSchemaHash(olapTable.getSchemaHashByIndexId(tabletCtx.getIndexId()));
                tabletCtx.setTabletStatus(Tablet.TabletStatus.HEALTHY); // rebalance tablet should be healthy first

                // createCloneReplicaAndTask, create replica will change invertedIndex too.
                rebalancer.createBalanceTask(tabletCtx, tabletScheduler.getBackendsWorkingSlots(), batchTask);
            } catch (SchedException e) {
                LOG.warn("schedule tablet {} failed: {}", tabletCtx.getTabletId(), e.getMessage());
            }
        }

        // Show debug info of MoveInProgressMap detail
        rebalancer.updateLoadStatistic(statisticMap);
        rebalancer.selectAlternativeTablets();

        // Get created tasks, and finish them manually
        List<AgentTask> tasks = batchTask.getAllTasks();
        List<Long> needCheckTablets = tasks.stream().map(AgentTask::getTabletId).collect(Collectors.toList());
        LOG.info("created tasks for tablet: {}", needCheckTablets);
        needCheckTablets.forEach(t -> Assert.assertEquals(4, invertedIndex.getReplicasByTabletId(t).size()));

//        // If clone task execution is too slow, tabletChecker may want to delete the CLONE replica.
//        tabletChecker.runAfterCatalogReady();
//        Assert.assertTrue(tabletScheduler.containsTablet(50000));
//        // tabletScheduler handle redundant
//        tabletScheduler.runAfterCatalogReady();

        for (Long tabletId : needCheckTablets) {
            TabletSchedCtx tabletSchedCtx = alternativeTablets.stream().filter(ctx -> ctx.getTabletId() == tabletId).collect(onlyElement());
            AgentTask task = tasks.stream().filter(t -> t.getTabletId() == tabletId).collect(onlyElement());

            LOG.info("try to finish tabletCtx {}", tabletId);
            try {
                TFinishTaskRequest fakeReq = new TFinishTaskRequest();
                fakeReq.task_status = new TStatus(TStatusCode.OK);
                fakeReq.finish_tablet_infos = Lists.newArrayList(new TTabletInfo(tabletSchedCtx.getTabletId(), 5, 1, 0, 0, 0));
                tabletSchedCtx.finishCloneTask((CloneTask) task, fakeReq);
            } catch (SchedException e) {
                e.printStackTrace();
            }
        }

        // NeedCheckTablets are redundant, TabletChecker will add them to TabletScheduler
        tabletChecker.runAfterCatalogReady();
        needCheckTablets.forEach(t -> Assert.assertEquals(4, invertedIndex.getReplicasByTabletId(t).size()));
        needCheckTablets.forEach(t -> Assert.assertTrue(tabletScheduler.containsTablet(t)));

        // TabletScheduler handle redundant tablet
        tabletScheduler.runAfterCatalogReady();

        // One replica is set to DECOMMISSION, still 4 replicas
        needCheckTablets.forEach(t -> {
            List<Replica> replicas = invertedIndex.getReplicasByTabletId(t);
            Assert.assertEquals(4, replicas.size());
            Replica decommissionedReplica = replicas.stream().filter(r -> r.getState() == Replica.ReplicaState.DECOMMISSION).collect(onlyElement());
            // expected watermarkTxnId is 111
            Assert.assertEquals(111, decommissionedReplica.getWatermarkTxnId());
        });

        // Delete replica should change invertedIndex too
        tabletScheduler.runAfterCatalogReady();
        needCheckTablets.forEach(t -> Assert.assertEquals(3, invertedIndex.getReplicasByTabletId(t).size()));

        // Check moves completed
        rebalancer.selectAlternativeTablets();
        rebalancer.updateLoadStatistic(statisticMap);
        AtomicLong succeeded = Deencapsulation.getField(rebalancer, "counterBalanceMoveSucceeded");
        Assert.assertEquals(needCheckTablets.size(), succeeded.get());
    }

    @Test
    public void testMoveInProgressMap() {
        Configurator.setLevel("org.apache.doris.clone.MovesInProgressCache", Level.DEBUG);
        MovesCacheMap m = new MovesCacheMap();
        m.updateMapping(statisticMap, 3);
        m.getCache(SystemInfoService.DEFAULT_CLUSTER, TStorageMedium.HDD).get().put(1L, new Pair<>(null, -1L));
        m.getCache(SystemInfoService.DEFAULT_CLUSTER, TStorageMedium.SSD).get().put(2L, new Pair<>(null, -1L));
        m.getCache(SystemInfoService.DEFAULT_CLUSTER, TStorageMedium.SSD).get().put(3L, new Pair<>(null, -1L));
        // Maintenance won't clean up the entries of cache
        m.maintain();
        Assert.assertEquals(3, m.size());

        // Reset the expireAfterAccess, the whole cache map will be cleared.
        m.updateMapping(statisticMap, 1);
        Assert.assertEquals(0, m.size());

        m.getCache(SystemInfoService.DEFAULT_CLUSTER, TStorageMedium.SSD).get().put(3L, new Pair<>(null, -1L));
        try {
            Thread.sleep(1000);
            m.maintain();
            Assert.assertEquals(0, m.size());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

