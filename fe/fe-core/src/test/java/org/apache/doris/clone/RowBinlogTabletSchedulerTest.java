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
        AgentBatchTask batchTask = new AgentBatchTask();

        Deencapsulation.invoke(tabletScheduler, "handleColocateMismatch", tabletCtx, batchTask);

        Assert.assertEquals(1, batchTask.getTaskNum());
        CloneTask cloneTask = (CloneTask) batchTask.getAllTasks().get(0);
        Assert.assertEquals(destBackendId, cloneTask.getBackendId());
        Assert.assertEquals(TStorageMedium.HDD, cloneTask.getStorageMedium());
        Assert.assertEquals(requiredPathHash, cloneTask.toThrift().getDestPathHash());
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

        tabletScheduler.updateDestPathHash(tabletCtx);
        Assert.assertEquals(requiredPathHash, replica.getPathHash());
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
        AgentBatchTask batchTask = new AgentBatchTask();

        Deencapsulation.invoke(tabletScheduler, "handleColocateMismatch", tabletCtx, batchTask);

        Assert.assertEquals(1, batchTask.getTaskNum());
        Assert.assertTrue(batchTask.getAllTasks().get(0) instanceof CloneTask);
        CloneTask cloneTask = (CloneTask) batchTask.getAllTasks().get(0);
        Assert.assertEquals(missingBackendId, cloneTask.getBackendId());
        Assert.assertEquals(missingRequiredPathHash, cloneTask.toThrift().getDestPathHash());
        Assert.assertEquals(TStorageMedium.HDD, cloneTask.getStorageMedium());
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
    public void basePreferredPathIsClearedBeforeEverySchedulingAttempt() {
        MaterializedIndex baseIndex = new MaterializedIndex(4L, IndexState.NORMAL);
        LocalTablet baseTablet = new LocalTablet(5L);
        baseIndex.addTablet(baseTablet, null, true);
        Partition partition = new Partition(3L, "p1", baseIndex, null);
        TabletSchedCtx tabletCtx = createTabletCtx(baseTablet, (short) 1);
        tabletCtx.setBasePreferredDestPathHashByBackend(ImmutableMap.of(10001L, 70001L));

        Deencapsulation.invoke(tabletScheduler, "setBasePreferredDestPathIfNecessary",
                tabletCtx, partition, baseIndex, baseTablet, TabletStatus.HEALTHY);

        Assert.assertFalse(tabletCtx.hasBasePreferredDestPathHash());
        Assert.assertTrue(tabletCtx.getBasePreferredDestPathHashByBackend().isEmpty());
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
            AgentBatchTask batchTask = new AgentBatchTask();

            exception = Assert.assertThrows(SchedException.class, () -> Deencapsulation.invoke(
                    tabletScheduler, "handleRowBinlogColocateRedundant", tabletCtx, batchTask));
            Assert.assertEquals(Status.SCHEDULE_FAILED, exception.getStatus());
            Assert.assertEquals(2, rowBinlogTablet.getReplicas().size());
            Assert.assertEquals(0, batchTask.getTaskNum());
            Assert.assertFalse(baseReplica.isBinlogMissing());
        } finally {
            Config.tablet_binlog_missing_timeout_second = previousTimeout;
            Config.tablet_binlog_missing_max_times = previousMaxTimes;
        }
    }

    private TabletSchedCtx createTabletCtx(LocalTablet tablet, short replicaNum) {
        TabletSchedCtx tabletCtx = new TabletSchedCtx(TabletSchedCtx.Type.REPAIR,
                1L, 2L, 3L, 4L, tablet.getId(), new ReplicaAllocation(replicaNum), System.currentTimeMillis());
        tabletCtx.setTablet(tablet);
        tabletCtx.updateTabletSize();
        tabletCtx.setVersionInfo(10L, 10L);
        tabletCtx.setSchemaHash(100);
        tabletCtx.setStorageMedium(TStorageMedium.SSD);
        return tabletCtx;
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

        LoadStatisticForTag loadStatistic = Mockito.mock(LoadStatisticForTag.class);
        Mockito.when(loadStatistic.getBackendLoadStatistic(backendId)).thenReturn(backendLoadStatistic);
        Mockito.when(loadStatistic.getBackendLoadStatistics()).thenReturn(Lists.newArrayList(backendLoadStatistic));
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
}
