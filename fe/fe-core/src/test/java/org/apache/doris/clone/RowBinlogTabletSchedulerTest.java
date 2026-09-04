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

import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DiskInfo.DiskState;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.LocalReplica;
import org.apache.doris.catalog.LocalTablet;
import org.apache.doris.catalog.LocalTabletInvertedIndex;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Tablet.TabletHealth;
import org.apache.doris.catalog.Tablet.TabletStatus;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.clone.SchedException.Status;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.CloneTask;
import org.apache.doris.task.StorageMediaMigrationTask;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

public class RowBinlogTabletSchedulerTest {
    private MockedStatic<Env> mockedEnvStatic;
    private Env env;
    private SystemInfoService infoService;
    private LocalTabletInvertedIndex invertedIndex;
    private TabletScheduler tabletScheduler;

    @Before
    public void setUp() {
        infoService = new SystemInfoService();
        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentSystemInfo).thenReturn(infoService);

        env = Mockito.mock(Env.class);
        Mockito.when(env.getNextId()).thenReturn(1000L);
        Mockito.when(env.getColocateTableIndex()).thenReturn(new ColocateTableIndex());
        invertedIndex = new LocalTabletInvertedIndex();
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        mockedEnvStatic.when(Env::getCurrentInvertedIndex).thenReturn(invertedIndex);
        tabletScheduler = new TabletScheduler(env, infoService, invertedIndex,
                new TabletSchedulerStat(), "");
    }

    @After
    public void tearDown() {
        mockedEnvStatic.close();
    }

    @Test
    public void rowBinlogRequiredPathUsesExactPathAcrossStorageMedium() {
        long destBackendId = 10001L;
        long srcBackendId = 10002L;
        long requiredPathHash = 70001L;
        long srcPathHash = 80001L;
        RootPathLoadStatistic requiredPath = path(
                destBackendId, "/required-hdd", requiredPathHash, TStorageMedium.HDD);
        RootPathLoadStatistic unrelatedPath = path(
                destBackendId, "/unrelated-ssd", 70002L, TStorageMedium.SSD);
        setLoadStatistics(destBackendId, requiredPath, unrelatedPath);
        infoService.addBackend(backend(srcBackendId, "127.0.0.2"));
        tabletScheduler.getBackendsWorkingSlots().put(destBackendId, new TabletScheduler.PathSlot(
                ImmutableMap.of(70001L, TStorageMedium.HDD, 70002L, TStorageMedium.SSD), destBackendId));
        tabletScheduler.getBackendsWorkingSlots().put(srcBackendId, new TabletScheduler.PathSlot(
                ImmutableMap.of(srcPathHash, TStorageMedium.SSD), srcBackendId));

        LocalTablet rowBinlogTablet = new LocalTablet(5L);
        rowBinlogTablet.addReplica(replica(6L, srcBackendId, 10L, srcPathHash), true);
        invertedIndex.addTablet(rowBinlogTablet.getId(), new TabletMeta(
                1L, 2L, 3L, 4L, 100, TStorageMedium.SSD, true));
        TabletSchedCtx tabletCtx = createTabletCtx(rowBinlogTablet, (short) 1);
        TabletHealth tabletHealth = new TabletHealth();
        tabletHealth.status = TabletStatus.COLOCATE_MISMATCH;
        tabletCtx.setTabletHealth(tabletHealth);
        tabletCtx.setRowBinlogRequiredDestPathHashByBackend(
                ImmutableMap.of(destBackendId, requiredPathHash));
        tabletCtx.setColocateGroupBackendIds(ImmutableSet.of(destBackendId));
        markRowBinlogRepair(tabletCtx, RowBinlogRepairReason.BACKEND_MISMATCH);
        AgentBatchTask batchTask = new AgentBatchTask();

        Deencapsulation.invoke(tabletScheduler, "handleColocateMismatch", tabletCtx, batchTask);

        Assert.assertEquals(1, batchTask.getTaskNum());
        CloneTask cloneTask = (CloneTask) batchTask.getAllTasks().get(0);
        Assert.assertEquals(destBackendId, cloneTask.getBackendId());
        Assert.assertEquals(TStorageMedium.HDD, cloneTask.getStorageMedium());
        Assert.assertEquals(requiredPathHash, cloneTask.toThrift().getDestPathHash());
        Assert.assertEquals(1L, tabletScheduler.getStat().counterReplicaRowBinlogMismatch.get());
        Assert.assertEquals(0L, tabletScheduler.getStat().counterReplicaColocateMismatch.get());
    }

    @Test
    public void rowBinlogRequiredPathDoesNotFallbackToOtherPath() {
        long backendId = 10001L;
        RootPathLoadStatistic unrelatedPath = path(backendId, "/unrelated", 70002L, TStorageMedium.SSD);
        setLoadStatistics(backendId, unrelatedPath);
        tabletScheduler.getBackendsWorkingSlots().put(backendId, new TabletScheduler.PathSlot(
                ImmutableMap.of(70002L, TStorageMedium.SSD), backendId));

        TabletSchedCtx tabletCtx = createTabletCtx(new LocalTablet(5L), (short) 3);
        tabletCtx.setRowBinlogRequiredDestPathHashByBackend(ImmutableMap.of(backendId, 79999L));
        tabletCtx.setColocateGroupBackendIds(ImmutableSet.of(backendId));

        SchedException exception = Assert.assertThrows(SchedException.class, () -> Deencapsulation.invoke(
                tabletScheduler, "doChooseAvailableDestPath", tabletCtx, Tag.class, true));

        Assert.assertEquals(Status.UNRECOVERABLE, exception.getStatus());
    }

    @Test
    public void rowBinlogWrongPathUsesInPlaceStorageMigration() {
        long backendId = 10001L;
        long requiredPathHash = 70001L;
        long sourcePathHash = 70002L;
        RootPathLoadStatistic requiredPath = path(
                backendId, "/required", requiredPathHash, TStorageMedium.HDD);
        setLoadStatistics(backendId, requiredPath);
        tabletScheduler.getBackendsWorkingSlots().put(backendId, new TabletScheduler.PathSlot(
                ImmutableMap.of(requiredPathHash, TStorageMedium.HDD, sourcePathHash, TStorageMedium.SSD),
                backendId));

        LocalTablet rowBinlogTablet = new LocalTablet(5L);
        Replica replica = new LocalReplica(6L, backendId, 10L, 100, 100L, 0L, 1L,
                ReplicaState.NORMAL, -1L, 10L);
        replica.setPathHash(sourcePathHash);
        rowBinlogTablet.addReplica(replica, true);
        TabletSchedCtx tabletCtx = createTabletCtx(rowBinlogTablet, (short) 1);
        tabletCtx.setRowBinlogRequiredDestPathHashByBackend(ImmutableMap.of(backendId, requiredPathHash));
        tabletCtx.setColocateGroupBackendIds(ImmutableSet.of(backendId));
        markRowBinlogRepair(tabletCtx, RowBinlogRepairReason.PATH_MISMATCH);
        AgentBatchTask batchTask = new AgentBatchTask();

        Deencapsulation.invoke(tabletScheduler, "handleColocateMismatch", tabletCtx, batchTask);

        Assert.assertEquals(TabletSchedCtx.State.RUNNING, tabletCtx.getState());
        Assert.assertEquals(TabletSchedCtx.BalanceType.DISK_BALANCE, tabletCtx.getBalanceType());
        Assert.assertEquals(backendId, tabletCtx.getSrcBackendId());
        Assert.assertEquals(sourcePathHash, tabletCtx.getSrcPathHash());
        Assert.assertEquals(backendId, tabletCtx.getDestBackendId());
        Assert.assertEquals(requiredPathHash, tabletCtx.getDestPathHash());
        Assert.assertEquals(TStorageMedium.HDD, tabletCtx.getStorageMedium());
        Assert.assertEquals(1, batchTask.getTaskNum());
        StorageMediaMigrationTask task = (StorageMediaMigrationTask) batchTask.getAllTasks().get(0);
        Assert.assertEquals(backendId, task.getBackendId());
        Assert.assertEquals("/required", task.getDataDir());
        Assert.assertEquals(TStorageMedium.HDD, task.getToStorageMedium());
        Assert.assertEquals(1L, tabletScheduler.getStat().counterReplicaRowBinlogMismatch.get());
        Assert.assertEquals(0L, tabletScheduler.getStat().counterReplicaColocateMismatch.get());

        tabletScheduler.updateDestPathHash(tabletCtx);
        Assert.assertEquals(requiredPathHash, replica.getPathHash());
    }

    @Test
    public void upgradedMixedMediumPairConvergesAfterBaseMigration() {
        long backendId = 10001L;
        long oldHddPathHash = 70001L;
        long newSsdPathHash = 70002L;
        RootPathLoadStatistic newBasePath = path(
                backendId, "/base-ssd", newSsdPathHash, TStorageMedium.SSD);
        setLoadStatistics(backendId, newBasePath);
        tabletScheduler.getBackendsWorkingSlots().put(backendId, new TabletScheduler.PathSlot(
                ImmutableMap.of(oldHddPathHash, TStorageMedium.HDD, newSsdPathHash, TStorageMedium.SSD),
                backendId));

        MaterializedIndex baseIndex = new MaterializedIndex(4L, IndexState.NORMAL);
        MaterializedIndex rowBinlogIndex = new MaterializedIndex(7L, IndexState.NORMAL);
        rowBinlogIndex.setIsRowBinlog(true);
        LocalTablet baseTablet = new LocalTablet(5L);
        LocalTablet rowBinlogTablet = new LocalTablet(6L);
        baseTablet.setRowBinlogTabletId(rowBinlogTablet.getId());
        rowBinlogTablet.setRowBinlogBaseTabletId(baseTablet.getId());
        Replica baseReplica = replica(8L, backendId, 10L, oldHddPathHash);
        Replica rowBinlogReplica = replica(9L, backendId, 10L, oldHddPathHash);
        baseTablet.addReplica(baseReplica, true);
        rowBinlogTablet.addReplica(rowBinlogReplica, true);
        baseIndex.addTablet(baseTablet, null, true);
        rowBinlogIndex.addTablet(rowBinlogTablet, null, true);
        Partition partition = new Partition(3L, "p1", baseIndex, null);
        partition.updateVisibleVersion(10L);
        partition.createRollupIndex(rowBinlogIndex);

        RowBinlogTabletLocality.RowBinlogHealthResult initialHealth =
                RowBinlogTabletLocality.getRowBinlogHealth(
                        partition, rowBinlogTablet, new ReplicaAllocation((short) 1), 10L);
        Assert.assertEquals(TabletStatus.HEALTHY, initialHealth.getTabletHealth().status);

        // Simulate the next BE report after the configured-medium migration moves the base replica first.
        baseReplica.setPathHash(newSsdPathHash);
        RowBinlogTabletLocality.RowBinlogHealthResult healthResult =
                RowBinlogTabletLocality.getRowBinlogHealth(
                        partition, rowBinlogTablet, new ReplicaAllocation((short) 1), 10L);
        Assert.assertEquals(TabletStatus.COLOCATE_MISMATCH, healthResult.getTabletHealth().status);
        Assert.assertEquals(RowBinlogRepairReason.PATH_MISMATCH, healthResult.getRepairReason());
        Assert.assertEquals(ImmutableMap.of(backendId, newSsdPathHash),
                healthResult.getRequiredDestPathHashByBackend());

        TabletSchedCtx tabletCtx = createTabletCtx(rowBinlogTablet, rowBinlogIndex.getId(), (short) 1);
        tabletCtx.setTabletHealth(healthResult.getTabletHealth());
        healthResult.applyTo(tabletCtx);
        AgentBatchTask batchTask = new AgentBatchTask();

        Deencapsulation.invoke(tabletScheduler, "handleColocateMismatch", tabletCtx, batchTask);

        Assert.assertEquals(1, batchTask.getTaskNum());
        StorageMediaMigrationTask task = (StorageMediaMigrationTask) batchTask.getAllTasks().get(0);
        Assert.assertEquals(backendId, task.getBackendId());
        Assert.assertEquals("/base-ssd", task.getDataDir());
        Assert.assertEquals(TStorageMedium.SSD, task.getToStorageMedium());

        tabletScheduler.updateDestPathHash(tabletCtx);
        RowBinlogTabletLocality.RowBinlogHealthResult repairedHealth =
                RowBinlogTabletLocality.getRowBinlogHealth(
                        partition, rowBinlogTablet, new ReplicaAllocation((short) 1), 10L);
        Assert.assertEquals(TabletStatus.HEALTHY, repairedHealth.getTabletHealth().status);
        Assert.assertEquals(RowBinlogRepairReason.NONE, repairedHealth.getRepairReason());
    }

    @Test
    public void rowBinlogMissingBackendIsClonedBeforeWrongPathMigration() {
        long existingBackendId = 10001L;
        long missingBackendId = 10002L;
        long existingRequiredPathHash = 70001L;
        long sourcePathHash = 70002L;
        long missingRequiredPathHash = 80001L;
        RootPathLoadStatistic missingRequiredPath = path(
                missingBackendId, "/missing-required", missingRequiredPathHash, TStorageMedium.HDD);
        setLoadStatistics(missingBackendId, missingRequiredPath);
        infoService.addBackend(backend(existingBackendId, "127.0.0.2"));
        tabletScheduler.getBackendsWorkingSlots().put(existingBackendId, new TabletScheduler.PathSlot(
                ImmutableMap.of(sourcePathHash, TStorageMedium.SSD), existingBackendId));
        tabletScheduler.getBackendsWorkingSlots().put(missingBackendId, new TabletScheduler.PathSlot(
                ImmutableMap.of(missingRequiredPathHash, TStorageMedium.HDD), missingBackendId));

        LocalTablet rowBinlogTablet = new LocalTablet(5L);
        rowBinlogTablet.addReplica(replica(6L, existingBackendId, 10L, sourcePathHash), true);
        invertedIndex.addTablet(rowBinlogTablet.getId(), new TabletMeta(
                1L, 2L, 3L, 4L, 100, TStorageMedium.SSD, true));
        TabletSchedCtx tabletCtx = createTabletCtx(rowBinlogTablet, (short) 2);
        TabletHealth tabletHealth = new TabletHealth();
        tabletHealth.status = TabletStatus.COLOCATE_MISMATCH;
        tabletCtx.setTabletHealth(tabletHealth);
        tabletCtx.setRowBinlogRequiredDestPathHashByBackend(ImmutableMap.of(
                existingBackendId, existingRequiredPathHash,
                missingBackendId, missingRequiredPathHash));
        tabletCtx.setColocateGroupBackendIds(ImmutableSet.of(existingBackendId, missingBackendId));
        markRowBinlogRepair(tabletCtx, RowBinlogRepairReason.BACKEND_MISMATCH);
        AgentBatchTask batchTask = new AgentBatchTask();

        Deencapsulation.invoke(tabletScheduler, "handleColocateMismatch", tabletCtx, batchTask);

        Assert.assertEquals(1, batchTask.getTaskNum());
        Assert.assertTrue(batchTask.getAllTasks().get(0) instanceof CloneTask);
        CloneTask cloneTask = (CloneTask) batchTask.getAllTasks().get(0);
        Assert.assertEquals(missingBackendId, cloneTask.getBackendId());
        Assert.assertEquals(missingRequiredPathHash, cloneTask.toThrift().getDestPathHash());
        Assert.assertEquals(TStorageMedium.HDD, cloneTask.getStorageMedium());
        Assert.assertEquals(1L, tabletScheduler.getStat().counterReplicaRowBinlogMismatch.get());
        Assert.assertEquals(0L, tabletScheduler.getStat().counterReplicaColocateMismatch.get());
    }

    @Test
    public void colocateMismatchUsesOnlyColocateCounter() {
        long requiredBackendId = 10001L;
        TabletSchedCtx tabletCtx = createTabletCtx(new LocalTablet(5L), (short) 1);
        tabletCtx.setColocateGroupBackendIds(ImmutableSet.of(requiredBackendId));

        Assert.assertThrows(SchedException.class, () -> Deencapsulation.invoke(
                tabletScheduler, "handleColocateMismatch", tabletCtx, new AgentBatchTask()));

        Assert.assertEquals(0L, tabletScheduler.getStat().counterReplicaRowBinlogMismatch.get());
        Assert.assertEquals(1L, tabletScheduler.getStat().counterReplicaColocateMismatch.get());
    }

    @Test
    public void colocateRedundantUsesOnlyColocateCounter() {
        long backendId = 10001L;
        LocalTablet tablet = new LocalTablet(5L);
        tablet.addReplica(replica(6L, backendId, 10L, 70001L), true);
        TabletSchedCtx tabletCtx = createTabletCtx(tablet, (short) 1);
        tabletCtx.setColocateGroupBackendIds(ImmutableSet.of(backendId));

        SchedException exception = Assert.assertThrows(SchedException.class, () -> Deencapsulation.invoke(
                tabletScheduler, "handleColocateRedundant", tabletCtx, new AgentBatchTask()));

        Assert.assertEquals(Status.UNRECOVERABLE, exception.getStatus());
        Assert.assertEquals(0L, tabletScheduler.getStat().counterReplicaRowBinlogRedundant.get());
        Assert.assertEquals(1L, tabletScheduler.getStat().counterReplicaColocateRedundant.get());
    }

    @Test
    public void rowBinlogRequiredBackendAllowsTemporarySameHostConflict() {
        long requiredBackendId = 10001L;
        long unrelatedBackendId = 10002L;
        infoService.addBackend(backend(requiredBackendId, "127.0.0.1"));
        infoService.addBackend(backend(unrelatedBackendId, "127.0.0.1"));

        LocalTablet rowBinlogTablet = new LocalTablet(5L);
        rowBinlogTablet.addReplica(new LocalReplica(
                6L, unrelatedBackendId, ReplicaState.NORMAL, 10L, 100), true);
        TabletSchedCtx tabletCtx = createTabletCtx(rowBinlogTablet, (short) 1);
        tabletCtx.setRowBinlogRequiredDestPathHashByBackend(ImmutableMap.of(requiredBackendId, 70001L));

        boolean previousAllowReplicaOnSameHost = Config.allow_replica_on_same_host;
        boolean previousRunningUnitTest = FeConstants.runningUnitTest;
        try {
            Config.allow_replica_on_same_host = false;
            FeConstants.runningUnitTest = false;

            Assert.assertTrue(tabletCtx.filterDestBE(requiredBackendId));
            Assert.assertFalse(tabletCtx.filterRowBinlogRequiredDestBE(requiredBackendId));

            tabletCtx.setRowBinlogRequiredDestPathHashByBackend(
                    ImmutableMap.of(requiredBackendId, 70001L, unrelatedBackendId, 70002L));
            Assert.assertTrue(tabletCtx.filterRowBinlogRequiredDestBE(requiredBackendId));
        } finally {
            Config.allow_replica_on_same_host = previousAllowReplicaOnSameHost;
            FeConstants.runningUnitTest = previousRunningUnitTest;
        }
    }

    @Test
    public void replicaMissingUsesRowBinlogPreferredPathForBaseRepair() {
        BaseRepairFixture fixture = createBaseRepairFixture(true);
        setTabletStatus(fixture.tabletCtx, TabletStatus.REPLICA_MISSING);
        AgentBatchTask batchTask = new AgentBatchTask();

        Deencapsulation.invoke(tabletScheduler, "handleReplicaMissing",
                fixture.tabletCtx, batchTask, fixture.partition, fixture.baseIndex);

        assertCloneUsesPath(batchTask, fixture.preferredBackendId, fixture.preferredPathHash);
    }

    @Test
    public void replicaRelocatingFallbackUsesRowBinlogPreferredPathForBaseRepair() {
        BaseRepairFixture fixture = createBaseRepairFixture(true);
        setTabletStatus(fixture.tabletCtx, TabletStatus.REPLICA_RELOCATING);
        AgentBatchTask batchTask = new AgentBatchTask();

        Deencapsulation.invoke(tabletScheduler, "handleReplicaRelocating",
                fixture.tabletCtx, batchTask, fixture.partition, fixture.baseIndex);

        assertCloneUsesPath(batchTask, fixture.preferredBackendId, fixture.preferredPathHash);
    }

    @Test
    public void versionIncompleteFallbackUsesRowBinlogPreferredPathForBaseRepair() {
        BaseRepairFixture fixture = createBaseRepairFixture(true);
        setTabletStatus(fixture.tabletCtx, TabletStatus.VERSION_INCOMPLETE);
        AgentBatchTask batchTask = new AgentBatchTask();

        Deencapsulation.invoke(tabletScheduler, "handleReplicaVersionIncomplete",
                fixture.tabletCtx, batchTask, fixture.partition, fixture.baseIndex);

        assertCloneUsesPath(batchTask, fixture.preferredBackendId, fixture.preferredPathHash);
    }

    @Test
    public void replicaMissingForTagUsesRowBinlogPreferredPathForBaseRepair() {
        BaseRepairFixture fixture = createBaseRepairFixture(true);
        setTabletStatus(fixture.tabletCtx, TabletStatus.REPLICA_MISSING_FOR_TAG);
        AgentBatchTask batchTask = new AgentBatchTask();

        Deencapsulation.invoke(tabletScheduler, "handleReplicaMissingForTag",
                fixture.tabletCtx, batchTask, fixture.partition, fixture.baseIndex);

        assertCloneUsesPath(batchTask, fixture.preferredBackendId, fixture.preferredPathHash);
    }

    @Test
    public void unavailableRowBinlogPreferredPathFallsBackToNormalSelection() {
        BaseRepairFixture fixture = createBaseRepairFixture(false);
        setTabletStatus(fixture.tabletCtx, TabletStatus.REPLICA_MISSING);
        AgentBatchTask batchTask = new AgentBatchTask();

        Deencapsulation.invoke(tabletScheduler, "handleReplicaMissing",
                fixture.tabletCtx, batchTask, fixture.partition, fixture.baseIndex);

        assertCloneUsesPath(batchTask, fixture.preferredBackendId, fixture.fallbackPathHash);
    }

    @Test
    public void redundantRowBinlogReplicaUsesVisibleVersionAndWaitsWhenMarkingFails() {
        long baseTabletId = 5L;
        long rowBinlogTabletId = 6L;
        long backendId = 10001L;
        infoService.addBackend(backend(backendId, "127.0.0.1"));
        MaterializedIndex baseIndex = new MaterializedIndex(4L, IndexState.NORMAL);
        MaterializedIndex rowBinlogIndex = new MaterializedIndex(7L, IndexState.NORMAL);
        rowBinlogIndex.setIsRowBinlog(true);
        LocalTablet baseTablet = new LocalTablet(baseTabletId);
        LocalTablet rowBinlogTablet = new LocalTablet(rowBinlogTabletId);
        baseTablet.setRowBinlogTabletId(rowBinlogTabletId);
        rowBinlogTablet.setRowBinlogBaseTabletId(baseTabletId);
        Replica baseReplica = replica(8L, backendId, 10L, 70001L);
        Replica rowBinlogReplica = replica(9L, backendId, 10L, 70001L);
        baseTablet.addReplica(baseReplica, true);
        rowBinlogTablet.addReplica(rowBinlogReplica, true);
        baseIndex.addTablet(baseTablet, null, true);
        rowBinlogIndex.addTablet(rowBinlogTablet, null, true);
        Partition partition = new Partition(3L, "p1", baseIndex, null);
        partition.createRollupIndex(rowBinlogIndex);

        Database database = Mockito.mock(Database.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(catalog.getDbNullable(1L)).thenReturn(database);
        Mockito.when(database.getTableNullable(2L)).thenReturn(table);
        Mockito.when(table.getPartition(3L)).thenReturn(partition);
        mockedEnvStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);

        TabletSchedCtx tabletCtx = new TabletSchedCtx(TabletSchedCtx.Type.REPAIR,
                1L, 2L, 3L, rowBinlogIndex.getId(), rowBinlogTabletId,
                new ReplicaAllocation((short) 1), System.currentTimeMillis());
        tabletCtx.setTablet(rowBinlogTablet);
        tabletCtx.setVersionInfo(10L, 11L);

        long previousTimeout = Config.tablet_binlog_missing_timeout_second;
        int previousMaxTimes = Config.tablet_binlog_missing_max_times;
        try {
            Config.tablet_binlog_missing_timeout_second = 60L;
            Config.tablet_binlog_missing_max_times = 5;

            Deencapsulation.invoke(tabletScheduler, "markBaseReplicaBinlogMissingIfNeeded",
                    tabletCtx, rowBinlogReplica);
            Assert.assertTrue(baseReplica.isBinlogMissing());

            baseReplica.setBinlogMissing(false);
            rowBinlogReplica.updateLastFailedVersion(11L);
            SchedException exception = Assert.assertThrows(SchedException.class, () -> Deencapsulation.invoke(
                    tabletScheduler, "markBaseReplicaBinlogMissingIfNeeded", tabletCtx, rowBinlogReplica));
            Assert.assertEquals(Status.SCHEDULE_FAILED, exception.getStatus());
            Assert.assertFalse(baseReplica.isBinlogMissing());

            Replica otherRowBinlogReplica = replica(10L, backendId + 1, 10L, 70002L);
            infoService.addBackend(backend(backendId + 1, "127.0.0.2"));
            rowBinlogTablet.addReplica(otherRowBinlogReplica, true);
            tabletCtx.setColocateGroupBackendIds(ImmutableSet.of(backendId, backendId + 1));
            tabletCtx.setRowBinlogRequiredDestPathHashByBackend(
                    ImmutableMap.of(backendId, 70001L, backendId + 1, 70002L));
            markRowBinlogRepair(tabletCtx, RowBinlogRepairReason.REDUNDANT);
            AgentBatchTask batchTask = new AgentBatchTask();

            exception = Assert.assertThrows(SchedException.class, () -> Deencapsulation.invoke(
                    tabletScheduler, "handleColocateRedundant", tabletCtx, batchTask));
            Assert.assertEquals(Status.SCHEDULE_FAILED, exception.getStatus());
            Assert.assertEquals(2, rowBinlogTablet.getReplicas().size());
            Assert.assertEquals(0, batchTask.getTaskNum());
            Assert.assertFalse(baseReplica.isBinlogMissing());
            Assert.assertEquals(1L, tabletScheduler.getStat().counterReplicaRowBinlogRedundant.get());
            Assert.assertEquals(0L, tabletScheduler.getStat().counterReplicaColocateRedundant.get());
        } finally {
            Config.tablet_binlog_missing_timeout_second = previousTimeout;
            Config.tablet_binlog_missing_max_times = previousMaxTimes;
        }
    }

    private TabletSchedCtx createTabletCtx(LocalTablet tablet, short replicaNum) {
        return createTabletCtx(tablet, 4L, replicaNum);
    }

    private TabletSchedCtx createTabletCtx(LocalTablet tablet, long indexId, short replicaNum) {
        TabletSchedCtx tabletCtx = new TabletSchedCtx(TabletSchedCtx.Type.REPAIR,
                1L, 2L, 3L, indexId, tablet.getId(),
                new ReplicaAllocation(replicaNum), System.currentTimeMillis());
        tabletCtx.setTablet(tablet);
        tabletCtx.updateTabletSize();
        tabletCtx.setVersionInfo(10L, 10L);
        tabletCtx.setSchemaHash(100);
        tabletCtx.setStorageMedium(TStorageMedium.SSD);
        return tabletCtx;
    }

    private BaseRepairFixture createBaseRepairFixture(boolean preferredPathAvailable) {
        long sourceBackendId = 10001L;
        long preferredBackendId = 10002L;
        long sourcePathHash = 60001L;
        long preferredPathHash = 70001L;
        long fallbackPathHash = 70002L;
        RootPathLoadStatistic preferredPath = path(
                preferredBackendId, "/row-binlog-preferred", preferredPathHash, TStorageMedium.SSD);
        RootPathLoadStatistic fallbackPath = path(
                preferredBackendId, "/normal-fallback", fallbackPathHash, TStorageMedium.SSD);
        if (preferredPathAvailable) {
            setLoadStatistics(preferredBackendId, preferredPath, fallbackPath);
        } else {
            setLoadStatistics(preferredBackendId, fallbackPath);
        }
        infoService.addBackend(backend(sourceBackendId, "127.0.0.2"));
        tabletScheduler.getBackendsWorkingSlots().put(sourceBackendId, new TabletScheduler.PathSlot(
                ImmutableMap.of(sourcePathHash, TStorageMedium.SSD), sourceBackendId));
        tabletScheduler.getBackendsWorkingSlots().put(preferredBackendId, new TabletScheduler.PathSlot(
                preferredPathAvailable
                        ? ImmutableMap.of(preferredPathHash, TStorageMedium.SSD,
                                fallbackPathHash, TStorageMedium.SSD)
                        : ImmutableMap.of(fallbackPathHash, TStorageMedium.SSD),
                preferredBackendId));

        MaterializedIndex baseIndex = new MaterializedIndex(4L, IndexState.NORMAL);
        MaterializedIndex rowBinlogIndex = new MaterializedIndex(7L, IndexState.NORMAL);
        rowBinlogIndex.setIsRowBinlog(true);
        LocalTablet baseTablet = new LocalTablet(5L);
        LocalTablet rowBinlogTablet = new LocalTablet(6L);
        baseTablet.setRowBinlogTabletId(rowBinlogTablet.getId());
        rowBinlogTablet.setRowBinlogBaseTabletId(baseTablet.getId());
        baseTablet.addReplica(replica(8L, sourceBackendId, 10L, sourcePathHash), true);
        rowBinlogTablet.addReplica(replica(9L, preferredBackendId, 10L, preferredPathHash), true);
        baseIndex.addTablet(baseTablet, null, true);
        rowBinlogIndex.addTablet(rowBinlogTablet, null, true);
        Partition partition = new Partition(3L, "p1", baseIndex, null);
        partition.updateVisibleVersion(10L);
        partition.createRollupIndex(rowBinlogIndex);
        invertedIndex.addTablet(baseTablet.getId(), new TabletMeta(
                1L, 2L, 3L, baseIndex.getId(), 100, TStorageMedium.SSD, false));

        TabletSchedCtx tabletCtx = createTabletCtx(baseTablet, baseIndex.getId(), (short) 2);
        return new BaseRepairFixture(partition, baseIndex, tabletCtx,
                preferredBackendId, preferredPathHash, fallbackPathHash);
    }

    private void setTabletStatus(TabletSchedCtx tabletCtx, TabletStatus status) {
        TabletHealth tabletHealth = new TabletHealth();
        tabletHealth.status = status;
        tabletCtx.setTabletHealth(tabletHealth);
    }

    private void assertCloneUsesPath(AgentBatchTask batchTask, long backendId, long pathHash) {
        Assert.assertEquals(1, batchTask.getTaskNum());
        Assert.assertTrue(batchTask.getAllTasks().get(0) instanceof CloneTask);
        CloneTask cloneTask = (CloneTask) batchTask.getAllTasks().get(0);
        Assert.assertEquals(backendId, cloneTask.getBackendId());
        Assert.assertEquals(pathHash, cloneTask.toThrift().getDestPathHash());
    }

    private void markRowBinlogRepair(TabletSchedCtx tabletCtx, RowBinlogRepairReason repairReason) {
        tabletCtx.setRowBinlogBaseTabletId(9000L);
        tabletCtx.setRowBinlogRepairReason(repairReason);
    }

    private void setLoadStatistics(long backendId, RootPathLoadStatistic... paths) {
        infoService.addBackend(backend(backendId, "127.0.0.1"));
        List<RootPathLoadStatistic> pathList = Lists.newArrayList(paths);
        BackendLoadStatistic backendLoadStatistic = Mockito.mock(BackendLoadStatistic.class);
        Mockito.when(backendLoadStatistic.getBeId()).thenReturn(backendId);
        Mockito.when(backendLoadStatistic.isAvailable()).thenReturn(true);
        Mockito.when(backendLoadStatistic.getTag()).thenReturn(Tag.DEFAULT_BACKEND_TAG);
        Mockito.when(backendLoadStatistic.getPathStatisticByPathHash(Mockito.anyLong())).thenAnswer(invocation -> {
            long pathHash = invocation.getArgument(0);
            return pathList.stream().filter(path -> path.getPathHash() == pathHash).findFirst().orElse(null);
        });
        Mockito.when(backendLoadStatistic.getPathStatistics()).thenReturn(pathList);
        Mockito.when(backendLoadStatistic.isFit(Mockito.anyLong(), Mockito.any(TStorageMedium.class),
                Mockito.anyList(), Mockito.anyBoolean())).thenAnswer(invocation -> {
                    TStorageMedium storageMedium = invocation.getArgument(1);
                    List<RootPathLoadStatistic> resultPaths = invocation.getArgument(2);
                    boolean isSupplement = invocation.getArgument(3);
                    pathList.stream()
                            .filter(path -> isSupplement || path.getStorageMedium() == storageMedium)
                            .forEach(resultPaths::add);
                    return resultPaths.isEmpty()
                            ? new BalanceStatus(BalanceStatus.ErrCode.COMMON_ERROR) : BalanceStatus.OK;
                });

        LoadStatisticForTag loadStatistic = Mockito.mock(LoadStatisticForTag.class);
        Mockito.when(loadStatistic.getBackendLoadStatistic(backendId)).thenReturn(backendLoadStatistic);
        Mockito.when(loadStatistic.getBackendLoadStatistics()).thenReturn(Lists.newArrayList(backendLoadStatistic));
        Mockito.when(loadStatistic.getSortedBeLoadStats(null))
                .thenReturn(Lists.newArrayList(backendLoadStatistic));
        tabletScheduler.getStatisticMap().clear();
        tabletScheduler.getStatisticMap().put(Tag.DEFAULT_BACKEND_TAG, loadStatistic);
    }

    private Backend backend(long backendId, String host) {
        Backend backend = new Backend(backendId, host, 9050);
        backend.setAlive(true);
        return backend;
    }

    private Replica replica(long replicaId, long backendId, long version, long pathHash) {
        Replica replica = new LocalReplica(replicaId, backendId, version, 100, 100L, 0L, 1L,
                ReplicaState.NORMAL, -1L, version);
        replica.setPathHash(pathHash);
        return replica;
    }

    private RootPathLoadStatistic path(long backendId, String path, long pathHash, TStorageMedium storageMedium) {
        return new RootPathLoadStatistic(backendId, path, pathHash, storageMedium,
                1024L * 1024L * 1024L * 1024L, 1024L, DiskState.ONLINE);
    }

    private static class BaseRepairFixture {
        private final Partition partition;
        private final MaterializedIndex baseIndex;
        private final TabletSchedCtx tabletCtx;
        private final long preferredBackendId;
        private final long preferredPathHash;
        private final long fallbackPathHash;

        private BaseRepairFixture(Partition partition, MaterializedIndex baseIndex, TabletSchedCtx tabletCtx,
                long preferredBackendId, long preferredPathHash, long fallbackPathHash) {
            this.partition = partition;
            this.baseIndex = baseIndex;
            this.tabletCtx = tabletCtx;
            this.preferredBackendId = preferredBackendId;
            this.preferredPathHash = preferredPathHash;
            this.fallbackPathHash = fallbackPathHash;
        }
    }
}
