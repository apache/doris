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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
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
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
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
import com.google.common.collect.MoreCollectors;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mocked;
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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class RebalanceTest {
    private static final Logger LOG = LogManager.getLogger(RebalanceTest.class);

    @Mocked
    private Env env;
    @Mocked
    private InternalCatalog catalog;

    private long id = 10086;

    private Database db;
    private OlapTable olapTable;

    private final SystemInfoService systemInfoService = new SystemInfoService();
    private final TabletInvertedIndex invertedIndex = new TabletInvertedIndex();
    private Map<Tag, LoadStatisticForTag> statisticMap;

    @Before
    public void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        db = new Database(1, "test db");
        new Expectations() {
            {
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;

                catalog.getDbIds();
                minTimes = 0;
                result = db.getId();

                catalog.getDbNullable(anyLong);
                minTimes = 0;
                result = db;

                catalog.getDbOrException(anyLong, (Function<Long, SchedException>) any);
                minTimes = 0;
                result = db;

                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                Env.getCurrentEnvJournalVersion();
                minTimes = 0;
                result = FeConstants.meta_version;

                env.getNextId();
                minTimes = 0;
                result = new Delegate() {
                    long ignored() {
                        return id++;
                    }
                };

                Env.getCurrentSystemInfo();
                minTimes = 0;
                result = systemInfoService;

                Env.getCurrentInvertedIndex();
                minTimes = 0;
                result = invertedIndex;

                Env.getCurrentGlobalTransactionMgr().getTransactionIDGenerator().getNextTransactionId();
                result = 111;

                Env.getCurrentGlobalTransactionMgr().isPreviousTransactionsFinished(anyLong, anyLong, (List<Long>) any);
                result = true;
            }
        };
        // Test mock validation
        Assert.assertEquals(111,
                Env.getCurrentGlobalTransactionMgr().getTransactionIDGenerator().getNextTransactionId());
        Assert.assertTrue(
                Env.getCurrentGlobalTransactionMgr().isPreviousTransactionsFinished(1, 2, Lists.newArrayList(3L)));

        List<Long> beIds = Lists.newArrayList(10001L, 10002L, 10003L, 10004L);
        beIds.forEach(id -> systemInfoService.addBackend(RebalancerTestUtil.createBackend(id, 2048, 0)));

        olapTable = new OlapTable(2, "fake table", new ArrayList<>(), KeysType.DUP_KEYS, new RangePartitionInfo(),
                new HashDistributionInfo());
        db.registerTable(olapTable);

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
        LoadStatisticForTag loadStatistic = new LoadStatisticForTag(
                Tag.DEFAULT_BACKEND_TAG, systemInfoService, invertedIndex, null);
        loadStatistic.init();
        statisticMap = Maps.newHashMap();
        statisticMap.put(Tag.DEFAULT_BACKEND_TAG, loadStatistic);
    }

    private void createPartitionsForTable(OlapTable olapTable, MaterializedIndex index, Long partitionCount) {
        // partition id start from 31
        LongStream.range(0, partitionCount).forEach(idx -> {
            long id = 31 + idx;
            Partition partition = new Partition(id, "p" + idx, index, new HashDistributionInfo());
            olapTable.addPartition(partition);
            olapTable.getPartitionInfo().addPartition(id, new DataProperty(TStorageMedium.HDD),
                    ReplicaAllocation.DEFAULT_ALLOCATION, false, true);
        });
    }

    @Test
    public void testPrioBackends() {
        Rebalancer rebalancer = new DiskRebalancer(Env.getCurrentSystemInfo(), Env.getCurrentInvertedIndex(), null);
        // add
        { // CHECKSTYLE IGNORE THIS LINE
            List<Backend> backends = Lists.newArrayList();
            for (int i = 0; i < 3; i++) {
                backends.add(RebalancerTestUtil.createBackend(10086 + i, 2048, 0));
            }
            rebalancer.addPrioBackends(backends, 1000);
            Assert.assertTrue(rebalancer.hasPrioBackends());
        } // CHECKSTYLE IGNORE THIS LINE

        // remove
        for (int i = 0; i < 3; i++) {
            List<Backend> backends = Lists.newArrayList(RebalancerTestUtil.createBackend(10086 + i, 2048, 0));
            rebalancer.removePrioBackends(backends);
            if (i == 2) {
                Assert.assertFalse(rebalancer.hasPrioBackends());
            } else {
                Assert.assertTrue(rebalancer.hasPrioBackends());
            }
        }
    }

    @Test
    public void testPartitionRebalancer() {
        Configurator.setLevel("org.apache.doris.clone.PartitionRebalancer", Level.DEBUG);

        // Disable scheduler's rebalancer adding balance task, add balance tasks manually
        Config.disable_balance = true;
        // generate statistic map again to create skewmap
        Config.tablet_rebalancer_type = "partition";
        generateStatisticMap();
        // Create a new scheduler & checker for redundant tablets handling
        // Call runAfterCatalogReady manually instead of starting daemon thread
        TabletSchedulerStat stat = new TabletSchedulerStat();
        PartitionRebalancer rebalancer = new PartitionRebalancer(Env.getCurrentSystemInfo(),
                Env.getCurrentInvertedIndex(), null);
        TabletScheduler tabletScheduler = new TabletScheduler(env, systemInfoService, invertedIndex, stat, "");
        // The rebalancer inside the scheduler will use this rebalancer, for getToDeleteReplicaId
        Deencapsulation.setField(tabletScheduler, "rebalancer", rebalancer);

        TabletChecker tabletChecker = new TabletChecker(env, systemInfoService, tabletScheduler, stat);

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
                tabletCtx.setVersionInfo(1, 1);
                tabletCtx.setSchemaHash(olapTable.getSchemaHashByIndexId(tabletCtx.getIndexId()));
                tabletCtx.setTabletStatus(Tablet.TabletStatus.HEALTHY); // rebalance tablet should be healthy first

                // createCloneReplicaAndTask, create replica will change invertedIndex too.
                AgentTask task = rebalancer.createBalanceTask(tabletCtx);
                batchTask.addTask(task);
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

        for (Long tabletId : needCheckTablets) {
            TabletSchedCtx tabletSchedCtx = alternativeTablets.stream()
                    .filter(ctx -> ctx.getTabletId() == tabletId)
                    .collect(MoreCollectors.onlyElement());
            AgentTask task = tasks.stream()
                    .filter(t -> t.getTabletId() == tabletId)
                    .collect(MoreCollectors.onlyElement());

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
            Replica decommissionedReplica = replicas.stream()
                    .filter(r -> r.getState() == Replica.ReplicaState.DECOMMISSION)
                    .collect(MoreCollectors.onlyElement());
            Assert.assertEquals(111, decommissionedReplica.getPreWatermarkTxnId());
            Assert.assertEquals(112, decommissionedReplica.getPostWatermarkTxnId());
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
        m.getCache(Tag.DEFAULT_BACKEND_TAG, TStorageMedium.HDD).get().put(1L, Pair.of(null, -1L));
        m.getCache(Tag.DEFAULT_BACKEND_TAG, TStorageMedium.SSD).get().put(2L, Pair.of(null, -1L));
        m.getCache(Tag.DEFAULT_BACKEND_TAG, TStorageMedium.SSD).get().put(3L, Pair.of(null, -1L));
        // Maintenance won't clean up the entries of cache
        m.maintain();
        Assert.assertEquals(3, m.size());

        // Reset the expireAfterAccess, the whole cache map will be cleared.
        m.updateMapping(statisticMap, 1);
        Assert.assertEquals(0, m.size());

        m.getCache(Tag.DEFAULT_BACKEND_TAG, TStorageMedium.SSD).get().put(3L, Pair.of(null, -1L));
        try {
            Thread.sleep(1000);
            m.maintain();
            Assert.assertEquals(0, m.size());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
