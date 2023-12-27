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
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.clone.TabletScheduler.PathSlot;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.StorageMediaMigrationTask;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import java.util.function.Function;
import java.util.stream.LongStream;

public class DiskRebalanceTest {
    private static final Logger LOG = LogManager.getLogger(DiskRebalanceTest.class);

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
    private Map<Long, PathSlot> backendsWorkingSlots = Maps.newHashMap();

    @Before
    public void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.used_capacity_percent_max_diff = 1.0;
        Config.balance_slot_num_per_path = 1;
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
    }

    private void generateStatisticsAndPathSlots() {
        LoadStatisticForTag loadStatistic = new LoadStatisticForTag(Tag.DEFAULT_BACKEND_TAG, systemInfoService,
                invertedIndex);
        loadStatistic.init();
        statisticMap = Maps.newHashMap();
        statisticMap.put(Tag.DEFAULT_BACKEND_TAG, loadStatistic);
        backendsWorkingSlots.clear();
        for (BackendLoadStatistic beStat : loadStatistic.getSortedBeLoadStats(null)) {
            Map<Long, TStorageMedium> paths = Maps.newHashMap();
            beStat.getPathStatistics().stream().forEach(
                    path -> paths.put(path.getPathHash(), path.getStorageMedium()));
            backendsWorkingSlots.put(beStat.getBeId(), new PathSlot(paths, beStat.getBeId()));
        }
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
    public void testDiskRebalancerWithSameUsageDisk() {
        // init system
        List<Long> beIds = Lists.newArrayList(10001L, 10002L, 10003L);
        beIds.forEach(id -> systemInfoService.addBackend(RebalancerTestUtil.createBackend(
                id, 2048, Lists.newArrayList(512L, 512L), 2)));

        olapTable = new OlapTable(2, "fake table", new ArrayList<>(), KeysType.DUP_KEYS,
                new RangePartitionInfo(), new HashDistributionInfo());
        db.createTable(olapTable);

        // 1 table, 3 partitions p0,p1,p2
        MaterializedIndex materializedIndex = new MaterializedIndex(olapTable.getId(), null);
        createPartitionsForTable(olapTable, materializedIndex, 3L);
        olapTable.setIndexMeta(materializedIndex.getId(), "fake index", Lists.newArrayList(new Column()),
                0, 0, (short) 0, TStorageType.COLUMN, KeysType.DUP_KEYS);

        // Tablet distribution: we add them to olapTable & build invertedIndex manually
        // all of tablets are in first path of it's backend
        RebalancerTestUtil.createTablet(invertedIndex, db, olapTable, "p0", TStorageMedium.HDD,
                50000, Lists.newArrayList(10001L, 10002L, 10003L));

        RebalancerTestUtil.createTablet(invertedIndex, db, olapTable, "p1", TStorageMedium.HDD,
                60000, Lists.newArrayList(10001L, 10002L, 10003L));

        RebalancerTestUtil.createTablet(invertedIndex, db, olapTable, "p2", TStorageMedium.HDD,
                70000, Lists.newArrayList(10001L, 10002L, 10003L));

        // case start
        Configurator.setLevel("org.apache.doris.clone.DiskRebalancer", Level.DEBUG);

        generateStatisticsAndPathSlots();
        Rebalancer rebalancer = new DiskRebalancer(Env.getCurrentSystemInfo(), Env.getCurrentInvertedIndex(),
                backendsWorkingSlots);
        rebalancer.updateLoadStatistic(statisticMap);
        List<TabletSchedCtx> alternativeTablets = rebalancer.selectAlternativeTablets();
        // check alternativeTablets;
        Assert.assertTrue(alternativeTablets.isEmpty());
    }

    @Test
    public void testDiskRebalancerWithDiffUsageDisk() {
        // init system
        systemInfoService.addBackend(RebalancerTestUtil.createBackend(10001L, 2048,
                Lists.newArrayList(1024L), 1));
        systemInfoService.addBackend(RebalancerTestUtil.createBackend(10002L, 2048,
                Lists.newArrayList(1024L, 512L), 2));
        systemInfoService.addBackend(RebalancerTestUtil.createBackend(10003L, 2048,
                Lists.newArrayList(1024L, 512L, 1024L), 3));

        olapTable = new OlapTable(2, "fake table", new ArrayList<>(), KeysType.DUP_KEYS,
                new RangePartitionInfo(), new HashDistributionInfo());
        db.createTable(olapTable);

        // 1 table, 3 partitions p0,p1,p2
        MaterializedIndex materializedIndex = new MaterializedIndex(olapTable.getId(), null);
        createPartitionsForTable(olapTable, materializedIndex, 3L);
        olapTable.setIndexMeta(materializedIndex.getId(), "fake index", Lists.newArrayList(new Column()),
                0, 0, (short) 0, TStorageType.COLUMN, KeysType.DUP_KEYS);

        // Tablet distribution: we add them to olapTable & build invertedIndex manually
        // all of tablets are in first path of it's backend
        RebalancerTestUtil.createTablet(invertedIndex, db, olapTable, "p0", TStorageMedium.HDD,
                50000, Lists.newArrayList(10001L, 10002L, 10003L), Lists.newArrayList(0L, 100L, 300L));

        RebalancerTestUtil.createTablet(invertedIndex, db, olapTable, "p1", TStorageMedium.HDD,
                60000, Lists.newArrayList(10001L, 10002L, 10003L), Lists.newArrayList(50L, 0L, 200L));

        RebalancerTestUtil.createTablet(invertedIndex, db, olapTable, "p2", TStorageMedium.HDD,
                70000, Lists.newArrayList(10001L, 10002L, 10003L), Lists.newArrayList(100L, 200L, 0L));

        // case start
        Configurator.setLevel("org.apache.doris.clone.DiskRebalancer", Level.DEBUG);

        generateStatisticsAndPathSlots();
        Rebalancer rebalancer = new DiskRebalancer(Env.getCurrentSystemInfo(), Env.getCurrentInvertedIndex(),
                backendsWorkingSlots);
        rebalancer.updateLoadStatistic(statisticMap);
        for (Map.Entry<Tag, LoadStatisticForTag> s : statisticMap.entrySet()) {
            if (s.getValue() != null) {
                LOG.info("tag = {}, statistic = {}", s.getKey(), s.getValue().getBrief());
            }
        }
        List<TabletSchedCtx> alternativeTablets = rebalancer.selectAlternativeTablets();
        // check alternativeTablets;
        Assert.assertEquals(2, alternativeTablets.size());
        for (TabletSchedCtx tabletCtx : alternativeTablets) {
            LOG.info("try to schedule tablet {}", tabletCtx.getTabletId());
            try {
                tabletCtx.setStorageMedium(TStorageMedium.HDD);
                tabletCtx.setTablet(olapTable.getPartition(tabletCtx.getPartitionId()).getIndex(tabletCtx.getIndexId()).getTablet(tabletCtx.getTabletId()));
                tabletCtx.setVersionInfo(1, 1);
                tabletCtx.setSchemaHash(olapTable.getSchemaHashByIndexId(tabletCtx.getIndexId()));
                tabletCtx.setTabletStatus(Tablet.TabletStatus.HEALTHY); // rebalance tablet should be healthy first

                AgentTask task = rebalancer.createBalanceTask(tabletCtx);
                if (tabletCtx.getTabletSize() == 0) {
                    Assert.fail("no exception");
                } else {
                    Assert.assertTrue(task instanceof StorageMediaMigrationTask);
                }
            } catch (SchedException e) {
                LOG.info("schedule tablet {} failed: {}", tabletCtx.getTabletId(), e.getMessage());
            }
        }
    }

}
