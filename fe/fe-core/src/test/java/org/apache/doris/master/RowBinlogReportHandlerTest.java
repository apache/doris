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

package org.apache.doris.master;

import org.apache.doris.catalog.BinlogConfig;
import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.LocalReplica;
import org.apache.doris.catalog.LocalTablet;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.persist.EditLog;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.DropReplicaTask;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TTablet;
import org.apache.doris.thrift.TTabletInfo;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RowBinlogReportHandlerTest {
    private static final long DB_ID = 1L;
    private static final long TABLE_ID = 2L;
    private static final long PARTITION_ID = 3L;
    private static final long BASE_INDEX_ID = 4L;
    private static final long ROW_BINLOG_INDEX_ID = 5L;
    private static final long ROLLUP_INDEX_ID = 6L;
    private static final long ORDINARY_TABLE_ID = 7L;
    private static final long ORDINARY_PARTITION_ID = 8L;
    private static final long ORDINARY_BASE_INDEX_ID = 9L;

    private MockedStatic<Env> mockedEnvStatic;
    private TabletInvertedIndex invertedIndex;

    @BeforeEach
    public void setUp() {
        invertedIndex = Mockito.mock(TabletInvertedIndex.class);

        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentInvertedIndex).thenReturn(invertedIndex);
    }

    @AfterEach
    public void tearDown() {
        mockedEnvStatic.close();
    }

    @Test
    public void storageMediumMigrationKeepsPairedBaseRollupAndOrdinaryTablets() {
        long baseTabletId = 100L;
        long rowBinlogTabletId = 101L;
        long rollupTabletId = 102L;
        long ordinaryTabletId = 103L;
        long missingTabletId = 104L;
        Mockito.when(invertedIndex.getTabletMeta(baseTabletId)).thenReturn(tabletMeta(TABLE_ID, PARTITION_ID,
                BASE_INDEX_ID));
        Mockito.when(invertedIndex.getTabletMeta(rowBinlogTabletId)).thenReturn(tabletMeta(TABLE_ID, PARTITION_ID,
                ROW_BINLOG_INDEX_ID));
        Mockito.when(invertedIndex.getTabletMeta(rollupTabletId)).thenReturn(tabletMeta(TABLE_ID, PARTITION_ID,
                ROLLUP_INDEX_ID));
        Mockito.when(invertedIndex.getTabletMeta(ordinaryTabletId)).thenReturn(tabletMeta(ORDINARY_TABLE_ID,
                ORDINARY_PARTITION_ID, ORDINARY_BASE_INDEX_ID));
        Mockito.when(invertedIndex.getTabletMeta(missingTabletId))
                .thenReturn(TabletInvertedIndex.NOT_EXIST_TABLET_META);

        ListMultimap<TStorageMedium, Long> migrationMap = LinkedListMultimap.create();
        migrationMap.put(TStorageMedium.SSD, baseTabletId);
        migrationMap.put(TStorageMedium.SSD, rowBinlogTabletId);
        migrationMap.put(TStorageMedium.SSD, rollupTabletId);
        migrationMap.put(TStorageMedium.SSD, ordinaryTabletId);
        migrationMap.put(TStorageMedium.SSD, missingTabletId);

        ReportHandler.filterRowBinlogTabletMigration(migrationMap, 10001L);

        Assertions.assertEquals(3, migrationMap.size());
        Assertions.assertTrue(migrationMap.containsEntry(TStorageMedium.SSD, baseTabletId));
        Assertions.assertTrue(migrationMap.containsEntry(TStorageMedium.SSD, rollupTabletId));
        Assertions.assertTrue(migrationMap.containsEntry(TStorageMedium.SSD, ordinaryTabletId));
        Assertions.assertFalse(migrationMap.containsEntry(TStorageMedium.SSD, rowBinlogTabletId));
        Assertions.assertFalse(migrationMap.containsEntry(TStorageMedium.SSD, missingTabletId));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void orphanRowReplicaWithoutInstalledConfigurationIsDropped(boolean missingGeneration) throws Exception {
        ReportFixture fixture = createReportFixture();
        if (missingGeneration) {
            fixture.report().unsetBinlogConfigVersion();
        } else {
            fixture.report().unsetBinlogTtlSeconds();
        }

        assertReportAdmission(fixture, false);
    }

    @ParameterizedTest
    @CsvSource({"10, 3600", "11, 60", "12, 3600"})
    public void orphanCloneWithDifferentInstalledTargetIsDropped(long generation, long ttlSeconds) throws Exception {
        ReportFixture fixture = createReportFixture();
        fixture.report().setBinlogConfigVersion(generation);
        fixture.report().setBinlogTtlSeconds(ttlSeconds);

        assertReportAdmission(fixture, false);
    }

    @Test
    public void orphanReplicaWithCurrentInstalledTargetCanBecomeNormal() throws Exception {
        assertReportAdmission(createReportFixture(), true);
    }

    @ParameterizedTest
    @CsvSource({"60, false", "3600, true"})
    public void orphanReplicaMustMatchPendingTargetInsteadOfVisibleTtl(long reportedTtl, boolean admitted)
            throws Exception {
        ReportFixture fixture = createReportFixture();
        fixture.table().getBinlogConfig().setTtlSeconds(60);
        fixture.table().getBinlogConfig().setPendingTtlSeconds(3600);
        fixture.report().setBinlogTtlSeconds(reportedTtl);

        assertReportAdmission(fixture, admitted);
        Assertions.assertEquals(60, fixture.table().getBinlogConfig().getTtlSeconds());
    }

    @Test
    public void ordinaryOrphanReplicaDoesNotRequireBinlogReportFields() throws Exception {
        ReportFixture fixture = createReportFixture();
        fixture.table().setBinlogConfig(new BinlogConfig());
        fixture.report().unsetBinlogConfigVersion();
        fixture.report().unsetBinlogTtlSeconds();

        assertReportAdmission(fixture, true);
    }

    @Test
    public void staleReportDoesNotDropReplicaAddedAfterReportSnapshot() throws Exception {
        ReportFixture fixture = createReportFixture();
        fixture.table().getPartitionInfo().setReplicaAllocation(PARTITION_ID, new ReplicaAllocation((short) 3));
        Replica currentReplica = new LocalReplica(fixture.report().getReplicaId(), 10002L, 10L, 100, 100L, 0L, 1L,
                ReplicaState.NORMAL, -1L, 10L);
        fixture.tablet().addReplica(currentReplica, true);
        fixture.report().setBinlogConfigVersion(10L);
        fixture.report().setBinlogTtlSeconds(60);

        try (MockedStatic<AgentTaskExecutor> executor = Mockito.mockStatic(AgentTaskExecutor.class)) {
            processOrphanReport(fixture.report());

            executor.verifyNoInteractions();
            Mockito.verifyNoInteractions(fixture.editLog());
            Assertions.assertSame(currentReplica, fixture.tablet().getReplicaByBackendId(10002L));
        }
    }

    private record ReportFixture(OlapTable table, LocalTablet tablet, TTabletInfo report, EditLog editLog) {
    }

    private ReportFixture createReportFixture() throws Exception {
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);

        SystemInfoService infoService = new SystemInfoService();
        Backend source = new Backend(10001L, "127.0.0.1", 9050);
        Backend destination = new Backend(10002L, "127.0.0.2", 9050);
        source.setAlive(true);
        destination.setAlive(true);
        infoService.addBackend(source);
        infoService.addBackend(destination);
        mockedEnvStatic.when(Env::getCurrentSystemInfo).thenReturn(infoService);
        mockedEnvStatic.when(Env::getCurrentColocateIndex).thenReturn(new ColocateTableIndex());

        List<Column> schema = Collections.singletonList(new Column("k1", PrimitiveType.INT));
        OlapTable table = new OlapTable(TABLE_ID, "tbl", schema, KeysType.DUP_KEYS,
                new SinglePartitionInfo(), new RandomDistributionInfo(1));
        BinlogConfig config = new BinlogConfig(true, 3600L, 1024L, 10L, BinlogConfig.BinlogFormat.ROW, false);
        config.setConfigVersion(11L);
        table.setBinlogConfig(config);
        table.setBaseIndexId(BASE_INDEX_ID);
        table.setIndexMeta(BASE_INDEX_ID, "tbl", schema, 0, 100,
                (short) 0, TStorageType.COLUMN, KeysType.DUP_KEYS);
        LocalTablet tablet = new LocalTablet(100L);
        tablet.addReplica(new LocalReplica(6L, source.getId(), 10L, 100, 100L, 0L, 1L,
                ReplicaState.NORMAL, -1L, 10L), true);
        MaterializedIndex index = new MaterializedIndex(BASE_INDEX_ID, MaterializedIndex.IndexState.NORMAL);
        index.addTablet(tablet, null, true);
        Partition partition = new Partition(PARTITION_ID, "p1", index, new RandomDistributionInfo(1));
        partition.updateVisibleVersion(10L);
        partition.setNextVersion(11L);
        table.addPartition(partition);
        table.getPartitionInfo().setReplicaAllocation(PARTITION_ID, new ReplicaAllocation((short) 2));
        Mockito.when(invertedIndex.getTabletMeta(100L)).thenReturn(new TabletMeta(DB_ID, TABLE_ID,
                PARTITION_ID, BASE_INDEX_ID, 100, TStorageMedium.HDD, false));

        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database db = Mockito.mock(Database.class);
        Mockito.when(catalog.getDbOrMetaException(DB_ID)).thenReturn(db);
        Mockito.when(db.getTableOrMetaException(TABLE_ID, Table.TableType.OLAP)).thenReturn(table);
        mockedEnvStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
        TTabletInfo report = new TTabletInfo(100L, 100, 10L, 0L, 1L, 100L);
        report.setReplicaId(11L);
        report.setBinlogConfigVersion(11L);
        report.setBinlogTtlSeconds(3600L);
        return new ReportFixture(table, tablet, report, editLog);
    }

    private void processOrphanReport(TTabletInfo report) {
        Deencapsulation.invoke(ReportHandler.class, "deleteFromBackend",
                Collections.singletonMap(report.getTabletId(), new TTablet(Collections.singletonList(report))),
                Collections.emptySet(), 10002L);
    }

    private void assertReportAdmission(ReportFixture fixture, boolean admitted) {
        List<AgentBatchTask> batches = new ArrayList<>();
        try (MockedStatic<AgentTaskExecutor> executor = Mockito.mockStatic(AgentTaskExecutor.class)) {
            executor.when(() -> AgentTaskExecutor.submit(Mockito.any())).thenAnswer(invocation -> {
                Assertions.assertFalse(fixture.table().isWriteLockHeldByCurrentThread());
                batches.add(invocation.getArgument(0));
                return null;
            });

            processOrphanReport(fixture.report());

            Replica replica = fixture.tablet().getReplicaByBackendId(10002L);
            if (admitted) {
                Assertions.assertNotNull(replica);
                Assertions.assertEquals(ReplicaState.NORMAL, replica.getState());
                Assertions.assertEquals(fixture.report().getReplicaId(), replica.getId());
                Assertions.assertTrue(batches.isEmpty());
                Mockito.verify(fixture.editLog()).logAddReplica(Mockito.any());
            } else {
                Assertions.assertNull(replica);
                Assertions.assertEquals(1, batches.size());
                Assertions.assertEquals(1, batches.get(0).getTaskNum());
                DropReplicaTask task = (DropReplicaTask) batches.get(0).getAllTasks().get(0);
                Assertions.assertEquals(fixture.report().getReplicaId(), task.getReplicaId());
                Assertions.assertEquals(fixture.report().getTabletId(), task.getTabletId());
                Mockito.verifyNoInteractions(fixture.editLog());
            }
        }
    }

    private TabletMeta tabletMeta(long tableId, long partitionId, long indexId) {
        return new TabletMeta(DB_ID, tableId, partitionId, indexId, 0, TStorageMedium.HDD,
                indexId == ROW_BINLOG_INDEX_ID);
    }
}
