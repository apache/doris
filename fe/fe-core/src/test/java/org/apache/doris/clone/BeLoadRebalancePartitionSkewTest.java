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

import org.apache.doris.alter.Alter;
import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.LocalReplica;
import org.apache.doris.catalog.LocalTabletInvertedIndex;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.clone.TabletScheduler.PathSlot;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Reproduce DORIS-27244: a freshly created table is distributed round-robin, but the default
 * BeLoadRebalancer immediately migrates its (still empty) tablets away and destroys that
 * distribution.
 *
 * <p>Root cause under test: the BE load score mixes two terms
 * (see {@code BackendLoadStatistic#calcScore})
 *
 * <pre>
 *   score = capacityProportion * capCoeff + replicaNumProportion * (1 - capCoeff)
 * </pre>
 *
 * A brand new tablet has dataSize == 0, so moving it leaves the capacity term untouched while the
 * replica-count term improves. {@code LoadStatisticForTag#isMoreBalanced} therefore accepts the
 * move even though zero bytes are relocated. The disk-usage difference that triggered the balance
 * (partly non-Doris data, which no tablet move can ever fix) gets "compensated" by shifting replica
 * counts, and the per-partition replica distribution is the thing that pays for it.
 */
public class BeLoadRebalancePartitionSkewTest {
    private static final Logger LOG = LogManager.getLogger(BeLoadRebalancePartitionSkewTest.class);

    private static final long MB = 1024L * 1024L;
    private static final long GB = 1024L * MB;

    // TPC-DS store_sales in the issue: 261 buckets, replication_num = 1, 16 BEs
    private static final int BE_NUM = 16;
    private static final int TABLET_NUM = 261;
    // other tables already living in the cluster: the issue's screenshot shows 960 replicas in
    // total, i.e. ~60 per BE. That ratio matters: it decides how much one single replica move
    // shifts the replica-count term of the load score.
    private static final int BACKGROUND_TABLET_NUM = 45 * BE_NUM;
    private static final long BE_TOTAL_CAPACITY = 1000 * GB;

    private static final long FIRST_BE_ID = 10001L;
    private static final long DB_ID = 1L;
    private static final long TABLE_ID = 2L;
    private static final long PARTITION_ID = 31L;
    private static final long BACKGROUND_TABLE_ID = 3L;
    private static final long BACKGROUND_PARTITION_ID = 41L;

    private Env env;
    private MockedStatic<Env> mockedEnvStatic;
    private TabletSchedulerStat schedulerStat;

    private Database db;
    private final SystemInfoService systemInfoService = new SystemInfoService();
    private final TabletInvertedIndex invertedIndex = new LocalTabletInvertedIndex();

    private long nextId = 100000L;
    private String origRebalancerType;

    @Before
    public void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        origRebalancerType = Config.tablet_rebalancer_type;
        Config.tablet_rebalancer_type = "BeLoad";

        db = new Database(DB_ID, "test_db");

        env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Alter alter = Mockito.mock(Alter.class);
        ColocateTableIndex colocateTableIndex = Mockito.mock(ColocateTableIndex.class);
        schedulerStat = new TabletSchedulerStat();

        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getDbIds()).thenReturn(Lists.newArrayList(db.getId()));
        Mockito.when(catalog.getDbNullable(Mockito.anyLong())).thenReturn(db);
        Mockito.when(catalog.getDbOrException(Mockito.anyLong(), Mockito.any())).thenReturn(db);
        Mockito.when(env.getNextId()).thenAnswer(inv -> nextId++);
        Mockito.when(env.getAlterInstance()).thenReturn(alter);
        Mockito.when(alter.getUnfinishedAlterTableIds()).thenReturn(Collections.emptySet());
        Mockito.when(env.getColocateTableIndex()).thenReturn(colocateTableIndex);
        Mockito.when(colocateTableIndex.isColocateTable(Mockito.anyLong())).thenReturn(false);

        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        mockedEnvStatic.when(Env::getCurrentEnvJournalVersion).thenReturn(FeConstants.meta_version);
        mockedEnvStatic.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);
        mockedEnvStatic.when(Env::getCurrentInvertedIndex).thenReturn(invertedIndex);
        mockedEnvStatic.when(Env::getCurrentColocateIndex).thenReturn(colocateTableIndex);
        mockedEnvStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
    }

    @After
    public void tearDown() {
        if (mockedEnvStatic != null) {
            mockedEnvStatic.close();
        }
        Config.tablet_rebalancer_type = origRebalancerType;
    }

    /**
     * The regression test for DORIS-27244.
     *
     * <p>16 BEs with identical disk capacity but slightly different disk usage (10.0% ~ 16.3%,
     * which yields CapCoeff = 0.5256, exactly what the issue's screenshot shows). That difference
     * is disk level, not replica level: the BEs hold the same amount of Doris data, the rest is
     * non-Doris usage that no tablet migration can ever remove.
     *
     * <p>On top of that cluster a table is created: 261 tablets, replication 1, distributed
     * round-robin, so every BE holds 16 or 17 replicas and all of them are still empty.
     *
     * <p>Expected: the empty tablets do not move, so the new table keeps its even distribution.
     */
    @Test
    public void testEmptyTabletBalanceShouldNotBreakRoundRobin() {
        List<Long> beIds = createBackendsWithSkewedDiskUsage();
        // an already loaded table, evenly distributed: 45 replicas of 1GB on every BE
        MaterializedIndex background = createRoundRobinTable(BACKGROUND_TABLE_ID, "background",
                BACKGROUND_PARTITION_ID, 60000, BACKGROUND_TABLET_NUM, beIds, 1 * GB);
        // the table that was just created, all its tablets are still empty
        MaterializedIndex storeSales = createRoundRobinTable(TABLE_ID, "store_sales",
                PARTITION_ID, 50000, TABLET_NUM, beIds, 0L);

        LOG.info("store_sales before balance: {}", sortedCounts(countReplicaPerBe(storeSales)));
        LOG.info("background before balance: {}", sortedCounts(countReplicaPerBe(background)));
        Assert.assertEquals(1, skewOf(countReplicaPerBe(storeSales)));
        Assert.assertEquals(0, skewOf(countReplicaPerBe(background)));

        int moves = runBalanceUntilStable(Lists.newArrayList(storeSales, background), 100);

        Map<Long, Integer> storeSalesAfter = countReplicaPerBe(storeSales);
        Map<Long, Integer> backgroundAfter = countReplicaPerBe(background);
        LOG.info("store_sales after balance: {}, moves: {}", sortedCounts(storeSalesAfter), moves);
        LOG.info("background after balance: {}", sortedCounts(backgroundAfter));

        Assert.assertTrue("the newly created table must keep its round-robin distribution,"
                        + " actual: " + sortedCounts(storeSalesAfter),
                skewOf(storeSalesAfter) <= 1);
        // Loaded tablets can legitimately move for capacity balancing. Any resulting per-index
        // skew belongs to a follow-up change and is intentionally only observed here.
        LOG.info("background table skew after legitimate capacity balance: {}", skewOf(backgroundAfter));
    }

    /**
     * Replica data size is not persisted in the FE image. After an FE restart a loaded tablet's
     * size remains zero until the next tablet stat report, and balancing with that unknown size
     * must be blocked.
     */
    @Test
    public void testLoadedTableIsNotBalancedBeforeSizeIsReportedAfterRestart() {
        List<Long> beIds = createBackendsWithSkewedDiskUsage();
        MaterializedIndex loadedTable = createRoundRobinTable(TABLE_ID, "loaded_table",
                PARTITION_ID, 50000, TABLET_NUM, beIds, 1 * GB);
        setReplicaSizes(loadedTable, 0L);

        Map<Long, Integer> before = countReplicaPerBe(loadedTable);
        int moves = runBalanceUntilStable(Lists.newArrayList(loadedTable), 100);
        Map<Long, Integer> after = countReplicaPerBe(loadedTable);

        Assert.assertEquals("no tablet with an unreported size should move", 0, moves);
        Assert.assertEquals("the loaded table must retain its distribution during the restart window",
                before, after);
        // Selection already filters these out, so nothing ever reaches the scheduler and the stat
        // counter stays at zero. How many tablets were skipped is reported in the round summary log
        // instead, because it counts scanned tablets rather than balance attempts.
        Assert.assertEquals("selection should filter zero-size tablets before scheduling",
                0L, schedulerStat.counterBalanceRejectByZeroDataSize.get());
    }

    /** The zero-size guard is unconditional and also applies to urgent BE balance. */
    @Test
    public void testUrgentBalanceRejectsTabletWhoseSizeBecomesZeroBeforeScheduling() {
        List<Long> beIds = createBackendsWithUrgentDiskUsage();
        long highBeId = beIds.get(beIds.size() - 1);
        OlapTable table = createTable(TABLE_ID, "urgent_table", PARTITION_ID);
        MaterializedIndex index = table.getPartition(PARTITION_ID).getBaseIndex();
        for (int i = 0; i < 8; i++) {
            RebalancerTestUtil.createTablet(invertedIndex, db, table, "p0", TStorageMedium.HDD,
                    50000 + i, Lists.newArrayList(highBeId), Lists.newArrayList(1 * GB));
        }

        Map<Long, PathSlot> slots = createWorkingSlots();
        BeLoadRebalancer rebalancer = new BeLoadRebalancer(systemInfoService, invertedIndex, slots);
        rebalancer.setSchedulerStat(schedulerStat);
        LoadStatisticForTag loadStatistic = newLoadStatistic(rebalancer);
        List<BackendLoadStatistic> lowBEs = Lists.newArrayList();
        List<BackendLoadStatistic> highBEs = Lists.newArrayList();
        Assert.assertTrue("the test must exercise urgent balance",
                loadStatistic.getLowHighBEsWithIsUrgent(lowBEs, highBEs, TStorageMedium.HDD));
        rebalancer.updateLoadStatistic(Maps.newHashMap(
                Collections.singletonMap(Tag.DEFAULT_BACKEND_TAG, loadStatistic)));

        List<TabletSchedCtx> candidates = rebalancer.selectAlternativeTablets();
        Assert.assertFalse("a reported, non-empty tablet should be selected", candidates.isEmpty());
        TabletSchedCtx tabletCtx = candidates.get(0);
        Tablet tablet = findTablet(Lists.newArrayList(index), tabletCtx.getTabletId());
        Assert.assertNotNull(tablet);
        setReplicaSizes(index, 0L);
        tabletCtx.setTablet(tablet);
        tabletCtx.updateTabletSize();
        tabletCtx.setStorageMedium(TStorageMedium.HDD);

        Replica replica = tablet.getReplicaByBackendId(highBeId);
        int availableSlotsBefore = slots.get(highBeId).getAvailableBalanceNum(replica.getPathHash());
        try {
            rebalancer.completeSchedCtx(tabletCtx);
            Assert.fail("urgent balance must reject a tablet whose size is no longer reported");
        } catch (SchedException e) {
            Assert.assertTrue(e.getMessage().contains("size of src replica is zero"));
        }
        Assert.assertEquals("the zero-size check must run before taking the source slot",
                availableSlotsBefore, slots.get(highBeId).getAvailableBalanceNum(replica.getPathHash()));
        Assert.assertEquals("the scheduling-time rejection should increment the counter",
                1L, schedulerStat.counterBalanceRejectByZeroDataSize.get());
    }

    /**
     * Documents the mechanism itself, independent of any fix: with a zero-sized tablet the capacity
     * term of the load score does not move at all, yet the move is still accepted as "more
     * balanced" purely because of the replica-count term.
     */
    @Test
    public void testZeroSizeMoveIsAcceptedAlthoughCapacityIsUnchanged() {
        List<Long> beIds = createBackendsWithSkewedDiskUsage();
        createRoundRobinTable(TABLE_ID, "store_sales", PARTITION_ID, 50000, TABLET_NUM, beIds, 0L);

        LoadStatisticForTag stat = newLoadStatistic(null);
        List<BackendLoadStatistic> lowBEs = Lists.newArrayList();
        List<BackendLoadStatistic> highBEs = Lists.newArrayList();
        stat.getLowHighBEsWithIsUrgent(lowBEs, highBEs, TStorageMedium.HDD);

        Assert.assertFalse("the disk usage spread should classify some BE as HIGH", highBEs.isEmpty());
        Assert.assertFalse("the disk usage spread should classify some BE as LOW", lowBEs.isEmpty());

        BackendLoadStatistic high = highBEs.get(highBEs.size() - 1);
        BackendLoadStatistic low = lowBEs.get(0);

        long highUsedBefore = high.getTotalUsedCapacityB(TStorageMedium.HDD);
        long lowUsedBefore = low.getTotalUsedCapacityB(TStorageMedium.HDD);

        Assert.assertTrue("a zero-sized tablet is accepted as a balance move even though it"
                        + " relocates no data at all",
                stat.isMoreBalanced(high.getBeId(), low.getBeId(), 50000L, 0L, TStorageMedium.HDD));

        Assert.assertEquals("the capacity term is untouched by a zero-sized move",
                highUsedBefore, high.getTotalUsedCapacityB(TStorageMedium.HDD));
        Assert.assertEquals("the capacity term is untouched by a zero-sized move",
                lowUsedBefore, low.getTotalUsedCapacityB(TStorageMedium.HDD));
    }

    /**
     * The counter test: the fix must not turn BeLoadRebalancer into a no-op. A genuinely skewed
     * index (all replicas piled on one BE) still has to be spread out.
     */
    @Test
    public void testGenuinelySkewedIndexIsStillBalanced() {
        List<Long> beIds = Lists.newArrayList();
        for (int i = 0; i < 4; i++) {
            long beId = FIRST_BE_ID + i;
            // identical disks, so only the replica count term drives the balance
            systemInfoService.addBackend(
                    RebalancerTestUtil.createBackend(beId, BE_TOTAL_CAPACITY, 100 * GB));
            beIds.add(beId);
        }

        OlapTable table = createTable(TABLE_ID, "store_sales", PARTITION_ID);
        MaterializedIndex index = table.getPartition(PARTITION_ID).getBaseIndex();
        // all 8 tablets sit on the first BE
        for (int i = 0; i < 8; i++) {
            RebalancerTestUtil.createTablet(invertedIndex, db, table, "p0", TStorageMedium.HDD,
                    50000 + i, Lists.newArrayList(beIds.get(0)), Lists.newArrayList(1 * GB));
        }

        Map<Long, Integer> before = countReplicaPerBe(index);
        LOG.info("skewed distribution before balance: {}", sortedCounts(before));
        Assert.assertEquals(8, skewOf(before));

        int moves = runBalanceUntilStable(Lists.newArrayList(index), 100);
        Map<Long, Integer> after = countReplicaPerBe(index);
        LOG.info("skewed distribution after balance: {}, moves: {}", sortedCounts(after), moves);

        Assert.assertTrue("balancer should have moved replicas away from the overloaded BE",
                moves > 0);
        Assert.assertTrue("a genuinely skewed index must still be balanced, actual: "
                + sortedCounts(after), skewOf(after) <= 1);
    }

    // ------------------------------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------------------------------

    /**
     * 16 BEs, same total capacity, disk usage from 10.0% up to 16.3%. Part of that difference is
     * non-Doris data in reality; no tablet migration can ever remove it.
     */
    private List<Long> createBackendsWithSkewedDiskUsage() {
        List<Long> beIds = Lists.newArrayList();
        for (int i = 0; i < BE_NUM; i++) {
            long beId = FIRST_BE_ID + i;
            long usedCapacity = 100 * GB + i * (4 * GB + 200 * MB);
            systemInfoService.addBackend(
                    RebalancerTestUtil.createBackend(beId, BE_TOTAL_CAPACITY, usedCapacity));
            beIds.add(beId);
        }
        return beIds;
    }

    private List<Long> createBackendsWithUrgentDiskUsage() {
        List<Long> beIds = Lists.newArrayList();
        for (int i = 0; i < 4; i++) {
            long beId = FIRST_BE_ID + i;
            long usedCapacity = i == 3 ? 700 * GB : 100 * GB;
            systemInfoService.addBackend(
                    RebalancerTestUtil.createBackend(beId, BE_TOTAL_CAPACITY, usedCapacity));
            beIds.add(beId);
        }
        return beIds;
    }

    private OlapTable createTable(long tableId, String name, long partitionId) {
        OlapTable table = new OlapTable(tableId, name, new ArrayList<>(), KeysType.DUP_KEYS,
                new RangePartitionInfo(), new HashDistributionInfo());
        db.registerTable(table);

        MaterializedIndex index = new MaterializedIndex(table.getId(), null);
        Partition partition = new Partition(partitionId, "p0", index, new HashDistributionInfo());
        table.addPartition(partition);
        table.getPartitionInfo().addPartition(partitionId, new DataProperty(TStorageMedium.HDD),
                ReplicaAllocation.DEFAULT_ALLOCATION, false, true);
        table.setIndexMeta(index.getId(), name, Lists.newArrayList(new Column()),
                0, 0, (short) 0, TStorageType.COLUMN, KeysType.DUP_KEYS);
        return table;
    }

    /** Distribute single-replica tablets round-robin, exactly like createTablets() does. */
    private MaterializedIndex createRoundRobinTable(long tableId, String name, long partitionId,
            int firstTabletId, int tabletNum, List<Long> beIds, long replicaSize) {
        OlapTable table = createTable(tableId, name, partitionId);
        for (int i = 0; i < tabletNum; i++) {
            long beId = beIds.get(i % beIds.size());
            RebalancerTestUtil.createTablet(invertedIndex, db, table, "p0", TStorageMedium.HDD,
                    firstTabletId + i, Lists.newArrayList(beId), Lists.newArrayList(replicaSize));
        }
        return table.getPartition(partitionId).getBaseIndex();
    }

    private LoadStatisticForTag newLoadStatistic(Rebalancer rebalancer) {
        LoadStatisticForTag stat = new LoadStatisticForTag(
                Tag.DEFAULT_BACKEND_TAG, systemInfoService, invertedIndex, rebalancer);
        stat.init();
        return stat;
    }

    private Map<Long, PathSlot> createWorkingSlots() {
        Map<Long, PathSlot> slots = Maps.newHashMap();
        for (long beId : systemInfoService.getAllBackendIds(false)) {
            Backend be = systemInfoService.getBackend(beId);
            Map<Long, TStorageMedium> paths = Maps.newHashMap();
            be.getDisks().values().forEach(disk -> paths.put(disk.getPathHash(), disk.getStorageMedium()));
            slots.put(beId, new PathSlot(paths, beId));
        }
        return slots;
    }

    /**
     * Drive BeLoadRebalancer the way TabletScheduler does: select alternative tablets, complete the
     * sched ctx (which picks src replica + dest BE), then apply the move. Statistics are rebuilt
     * every round, so the loop stops as soon as the cluster is considered balanced.
     *
     * @return total number of applied moves
     */
    private int runBalanceUntilStable(List<MaterializedIndex> indexes, int maxRounds) {
        int totalMoves = 0;
        for (int round = 0; round < maxRounds; round++) {
            Map<Long, PathSlot> slots = createWorkingSlots();
            BeLoadRebalancer rebalancer = new BeLoadRebalancer(systemInfoService, invertedIndex, slots);
            rebalancer.setSchedulerStat(schedulerStat);
            Map<Tag, LoadStatisticForTag> statisticMap = Maps.newHashMap();
            statisticMap.put(Tag.DEFAULT_BACKEND_TAG, newLoadStatistic(rebalancer));
            rebalancer.updateLoadStatistic(statisticMap);

            List<TabletSchedCtx> candidates = rebalancer.selectAlternativeTablets();
            if (candidates.isEmpty()) {
                LOG.info("round {}: cluster is balanced, no more candidates", round);
                break;
            }

            int movesThisRound = 0;
            for (TabletSchedCtx tabletCtx : candidates) {
                Tablet tablet = findTablet(indexes, tabletCtx.getTabletId());
                if (tablet == null) {
                    continue;
                }
                tabletCtx.setTablet(tablet);
                tabletCtx.updateTabletSize();
                tabletCtx.setStorageMedium(TStorageMedium.HDD);
                try {
                    rebalancer.completeSchedCtx(tabletCtx);
                } catch (SchedException e) {
                    LOG.debug("tablet {} not scheduled: {}", tabletCtx.getTabletId(), e.getMessage());
                    continue;
                }
                applyMove(tablet, tabletCtx.getSrcBackendId(), tabletCtx.getDestBackendId());
                movesThisRound++;
            }

            totalMoves += movesThisRound;
            if (movesThisRound == 0) {
                LOG.info("round {}: no move was accepted, stop", round);
                break;
            }
        }
        return totalMoves;
    }

    private Tablet findTablet(List<MaterializedIndex> indexes, long tabletId) {
        for (MaterializedIndex index : indexes) {
            Tablet tablet = index.getTablet(tabletId);
            if (tablet != null) {
                return tablet;
            }
        }
        return null;
    }

    /** Simulate a finished clone + redundant replica deletion. */
    private void applyMove(Tablet tablet, long srcBeId, long destBeId) {
        TabletMeta tabletMeta = invertedIndex.getTabletMeta(tablet.getId());
        Replica srcReplica = tablet.getReplicaByBackendId(srcBeId);
        Assert.assertNotNull(srcReplica);
        long dataSize = srcReplica.getDataSize();

        Replica destReplica = new LocalReplica(nextId++, destBeId, Replica.ReplicaState.NORMAL,
                srcReplica.getVersion(), tabletMeta.getOldSchemaHash());
        destReplica.setPathHash(destBeId);
        destReplica.setDataSize(dataSize);

        // deleteReplicaByBackendId updates the inverted index by itself
        tablet.deleteReplicaByBackendId(srcBeId);
        tablet.addReplica(destReplica, true);
        invertedIndex.addReplica(tablet.getId(), destReplica);
    }

    private Map<Long, Integer> countReplicaPerBe(MaterializedIndex index) {
        Map<Long, Integer> counts = Maps.newHashMap();
        systemInfoService.getAllBackendIds(false).forEach(beId -> counts.put(beId, 0));
        for (Tablet tablet : index.getTablets()) {
            for (Replica replica : tablet.getReplicas()) {
                counts.merge(replica.getBackendIdWithoutException(), 1, Integer::sum);
            }
        }
        return counts;
    }

    private void setReplicaSizes(MaterializedIndex index, long dataSize) {
        for (Tablet tablet : index.getTablets()) {
            tablet.getReplicas().forEach(replica -> replica.setDataSize(dataSize));
        }
    }

    private int skewOf(Map<Long, Integer> counts) {
        return Collections.max(counts.values()) - Collections.min(counts.values());
    }

    private List<Integer> sortedCounts(Map<Long, Integer> counts) {
        List<Integer> values = Lists.newArrayList(counts.values());
        Collections.sort(values);
        return values;
    }
}
