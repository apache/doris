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

package org.apache.doris.statistics.analysis;

import org.apache.doris.analysis.TableSample;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.LocalTablet;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.common.Config;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.cache.StatisticsCache;
import org.apache.doris.statistics.repository.ColStatsData;
import org.apache.doris.statistics.repository.ResultRow;
import org.apache.doris.statistics.sampling.ColumnPhysicalLayout;
import org.apache.doris.statistics.sampling.HierarchicalFixedBudgetTabletSamplingMethod;
import org.apache.doris.statistics.sampling.IndexSamplingSnapshot;
import org.apache.doris.statistics.sampling.LegacyTabletSamplingMethod;
import org.apache.doris.statistics.sampling.TabletSampleMeta;
import org.apache.doris.statistics.sampling.TabletSamplingScope;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.IntStream;

class OlapSamplingIntegrationTest {
    @Test
    void admissionUsesTargetIndexModelColumnAndBackendCapability() {
        OlapAnalysisTask task = new OlapAnalysisTask();
        OlapTable table = Mockito.mock(OlapTable.class);
        task.tbl = table;
        task.info = new AnalysisInfoBuilder().setIndexId(99L).build();
        task.tableSample = new TableSample(false, 10L);
        task.job = Mockito.mock(AnalysisJob.class);
        var frame = Mockito.mock(IndexSamplingSnapshot.class);
        Mockito.when(task.job.samplingFrame(table, 99)).thenReturn(frame);
        var regular = new ColumnPhysicalLayout(false, false, false, false);
        try (MockedStatic<StatisticsUtil> util = Mockito.mockStatic(StatisticsUtil.class)) {
            util.when(StatisticsUtil::enableFixedBlockStatsSampling).thenReturn(true);
            Mockito.when(frame.allCandidatesSupportFixedBlock()).thenReturn(true);
            Assertions.assertInstanceOf(HierarchicalFixedBudgetTabletSamplingMethod.class,
                    task.chooseTabletSamplingMethod(table, KeysType.DUP_KEYS, false, regular, 123));
            Assertions.assertInstanceOf(LegacyTabletSamplingMethod.class,
                    task.chooseTabletSamplingMethod(table, KeysType.AGG_KEYS, false, regular, 123));
            Assertions.assertInstanceOf(LegacyTabletSamplingMethod.class,
                    task.chooseTabletSamplingMethod(table, KeysType.UNIQUE_KEYS, false, regular, 123));
            Assertions.assertInstanceOf(HierarchicalFixedBudgetTabletSamplingMethod.class,
                    task.chooseTabletSamplingMethod(table, KeysType.UNIQUE_KEYS, true,
                            new ColumnPhysicalLayout(true, false, true, true), 123));
            Assertions.assertInstanceOf(HierarchicalFixedBudgetTabletSamplingMethod.class,
                    task.chooseTabletSamplingMethod(
                            table, KeysType.DUP_KEYS, false, new ColumnPhysicalLayout(true, false, false, true), 123));
            Assertions.assertInstanceOf(LegacyTabletSamplingMethod.class,
                    task.chooseTabletSamplingMethod(table, KeysType.DUP_KEYS, false, regular, 0));
            Mockito.when(frame.allCandidatesSupportFixedBlock()).thenReturn(false);
            Assertions.assertInstanceOf(LegacyTabletSamplingMethod.class,
                    task.chooseTabletSamplingMethod(table, KeysType.DUP_KEYS, false, regular, 123));
            Mockito.when(frame.allCandidatesSupportFixedBlock()).thenReturn(true);
            util.when(StatisticsUtil::enableFixedBlockStatsSampling).thenReturn(false);
            Assertions.assertInstanceOf(LegacyTabletSamplingMethod.class,
                    task.chooseTabletSamplingMethod(table, KeysType.DUP_KEYS, false, regular, 123));
        }
    }

    @Test
    void capabilityRequiresEverySchedulableCandidateInTheStatisticsComputeGroup() throws Exception {
        Backend current = Mockito.mock(Backend.class);
        Backend old = Mockito.mock(Backend.class);
        Mockito.when(current.isMixNode()).thenReturn(true);
        Mockito.when(current.isQueryAvailable()).thenReturn(true);
        Mockito.when(current.isSupportsFixedBlockSamplingV1()).thenReturn(true);
        SystemInfoService system = Mockito.mock(SystemInfoService.class);
        Mockito.when(system.getBackend(7L)).thenReturn(current);
        Mockito.when(system.getBackend(8L)).thenReturn(old);
        Mockito.when(system.getBackendsByCurrentCluster()).thenReturn(ImmutableMap.of(7L, current, 8L, old));
        var frame = new IndexSamplingSnapshot(TabletSamplingScope.table(List.of()), Set.of(7L, 8L));
        try (var env = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS);
                var config = Mockito.mockStatic(Config.class, Mockito.CALLS_REAL_METHODS);
                var util = Mockito.mockStatic(StatisticsUtil.class)) {
            env.when(Env::getCurrentSystemInfo).thenReturn(system);
            util.when(() -> StatisticsUtil.buildConnectContext(false)).thenAnswer(
                    ignored -> new AutoCloseConnectContext(new ConnectContext()));
            for (boolean cloud : List.of(false, true)) {
                config.when(Config::isCloudMode).thenReturn(cloud);
                Mockito.when(old.isMixNode()).thenReturn(true);
                Mockito.when(old.isQueryAvailable()).thenReturn(true);
                Assertions.assertFalse(frame.allCandidatesSupportFixedBlock());
                Mockito.when(old.isMixNode()).thenReturn(false);
                Assertions.assertTrue(frame.allCandidatesSupportFixedBlock());
                Mockito.when(old.isMixNode()).thenReturn(true);
                Mockito.when(old.isQueryAvailable()).thenReturn(false);
                Assertions.assertTrue(frame.allCandidatesSupportFixedBlock());
            }
        }
    }

    @Test
    void zeroVisibleSqlResultSkipsWithoutPublishingToTheJobOrCache() throws Exception {
        OlapAnalysisTask task = Mockito.spy(new OlapAnalysisTask());
        task.info = new AnalysisInfoBuilder().setIndexId(99L).build();
        task.job = Mockito.mock(AnalysisJob.class);
        Mockito.doNothing().when(task).prepareExecution();
        Mockito.doAnswer(invocation -> {
            task.runQuery("SELECT fixed_block_statistics");
            return null;
        }).when(task).doExecute();
        var values = new ArrayList<>(Collections.nCopies(15, "0"));
        values.set(7, "-1");
        try (var util = Mockito.mockStatic(StatisticsUtil.class);
                var executor = Mockito.mockConstruction(StmtExecutor.class, (stmt, context) ->
                        Mockito.when(stmt.executeInternalQuery()).thenReturn(List.of(new ResultRow(values))))) {
            util.when(() -> StatisticsUtil.buildConnectContext(false)).thenAnswer(
                    ignored -> new AutoCloseConnectContext(new ConnectContext()));
            task.execute();
            Assertions.assertEquals(1, executor.constructed().size());
            Assertions.assertTrue(task.info.message.contains("zero visible rows"));
            Mockito.verify(task.job).taskDoneWithoutData(task);
            Mockito.verify(task.job, Mockito.never()).appendBuf(Mockito.any(), Mockito.any());
            Mockito.verify(task.job, Mockito.never()).getJobInfo();
        }
    }

    @Test
    void emptySampleSkipsBeforeCacheOrBufferAndConcurrentColumnsFreezeOneCount() {
        OlapAnalysisTask task = new OlapAnalysisTask();
        task.info = new AnalysisInfoBuilder().setIndexId(99L).build();
        task.job = new AnalysisJob(task.info, List.of());
        var values = new ArrayList<>(Collections.nCopies(15, "0"));
        values.set(7, "-1");
        Assertions.assertThrows(AnalyzeSkipException.class, () -> task.freezeResultCount(new ResultRow(values)));
        long[] frozen = IntStream.range(0, 64).parallel().mapToLong(i -> task.job.freezeIndexRows(99, () -> 1234 + i))
                .distinct().toArray();
        Assertions.assertEquals(1, frozen.length);
        values.set(7, "9000000000");
        Assertions.assertEquals(Long.toString(frozen[0]), task.freezeResultCount(new ResultRow(values)).get(7));
        values.set(7, "2000");
        values.set(8, "1400");
        values.set(9, "600");
        values.set(12, "8000");
        ResultRow census = task.freezeResultCount(new ResultRow(values));
        long nulls = Long.parseLong(census.get(9));
        Assertions.assertEquals(Math.round(frozen[0] * 0.3), nulls);
        Assertions.assertEquals(frozen[0] - nulls, Long.parseLong(census.get(8)));
        Assertions.assertEquals(frozen[0] * 4, Long.parseLong(census.get(12)));
        Assertions.assertEquals(72, task.job.freezeIndexRows(100, () -> 72));
        Assertions.assertEquals(17, task.job.samplingSeed(17));
        Assertions.assertTrue(task.job.samplingSeed(-1) >= 0);
        Assertions.assertEquals(task.job.samplingSeed(-1), task.job.samplingSeed(-1));
    }

    @Test
    void frozenCountsRoundDecimalAndScientificNotationWithoutLosingIntegerPrecision() {
        var task = new OlapAnalysisTask();
        task.info = new AnalysisInfoBuilder().setIndexId(99L).build();
        task.job = new AnalysisJob(task.info, List.of());
        var values = new ArrayList<>(Collections.nCopies(15, "0"));
        values.set(7, "123.5");
        Assertions.assertEquals("124", task.freezeResultCount(new ResultRow(values)).get(7));
        values.set(7, "1.23e4");
        values.set(8, "12.5");
        var row = task.freezeResultCount(new ResultRow(values));
        Assertions.assertEquals("124", row.get(7));
        Assertions.assertEquals("13", row.get(8));
        task.info = new AnalysisInfoBuilder().setIndexId(100L).build();
        values.set(7, "9007199254740992.5");
        Assertions.assertEquals("9007199254740993", task.freezeResultCount(new ResultRow(values)).get(7));
    }

    @Test
    void censusCanPublishZeroUnlessAnotherColumnObservedANonemptyIndex() {
        var task = new OlapAnalysisTask();
        task.info = new AnalysisInfoBuilder().setIndexId(99L).build();
        task.job = new AnalysisJob(task.info, List.of());
        var values = new ArrayList<>(Collections.nCopies(15, "0"));
        Assertions.assertEquals("0", task.freezeResultCount(new ResultRow(values)).get(7));
        values.set(7, "2");
        Assertions.assertThrows(AnalyzeSkipException.class, () -> task.freezeResultCount(new ResultRow(values)));
        task.info = new AnalysisInfoBuilder().setIndexId(100L).build();
        task.freezeResultCount(new ResultRow(values));
        values.set(7, "0");
        Assertions.assertThrows(AnalyzeSkipException.class, () -> task.freezeResultCount(new ResultRow(values)));
    }

    @Test
    void twoSqlResultsPublishTheSameIndexCountToTheCacheAndJob() {
        var info = new AnalysisInfoBuilder().setIndexId(99L).build();
        var cache = Mockito.mock(StatisticsCache.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getAnalysisManager()).thenReturn(Mockito.mock(AnalysisManager.class));
        Mockito.when(env.getStatisticsCache()).thenReturn(cache);
        var ctx = new ConnectContext();
        ctx.setQueryId(new org.apache.doris.thrift.TUniqueId(1, 2));
        try (var envStatic = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS);
                var util = Mockito.mockStatic(StatisticsUtil.class, Mockito.CALLS_REAL_METHODS);
                var sql = Mockito.mockConstruction(StmtExecutor.class, (stmt, construction) -> {
                    String n = construction.getCount() == 1 ? "100" : "90";
                    Mockito.when(stmt.getContext()).thenReturn(ctx);
                    Mockito.when(stmt.executeInternalQuery()).thenReturn(List.of(new ResultRow(List.of(
                            "id", "1", "2", "3", "99", "c", "0", n, "2", "1", "a", "b", "100", "now", ""))));
                })) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            util.when(() -> StatisticsUtil.buildConnectContext(false)).thenAnswer(
                    ignored -> new AutoCloseConnectContext(ctx));
            AnalysisJob job = Mockito.spy(new AnalysisJob(info, List.of()));
            Mockito.doNothing().when(job).appendBuf(Mockito.any(), Mockito.any());
            for (String col : List.of("a", "b")) {
                OlapAnalysisTask task = new OlapAnalysisTask();
                task.job = job;
                task.info = new AnalysisInfoBuilder().setIndexId(99L).setColName(col).build();
                task.runQuery("SELECT " + col);
            }
            var saved = ArgumentCaptor.forClass(ColStatsData.class);
            Mockito.verify(cache, Mockito.times(2)).syncColStats(saved.capture());
            Assertions.assertEquals(List.of(100L, 100L), saved.getAllValues().stream().map(v -> v.count).toList());
            Assertions.assertEquals(100L, info.indexesRowCount.get(99L));
            Assertions.assertEquals(2, sql.constructed().size());
        }
    }

    @Test
    void cloudSnapshotUsesIndexTabletReportsAtMsVersions() {
        try (var config = Mockito.mockStatic(Config.class, Mockito.CALLS_REAL_METHODS);
                var ms = Mockito.mockStatic(CloudPartition.class)) {
            config.when(Config::isCloudMode).thenReturn(true);
            CloudPartition fresh = Mockito.mock(CloudPartition.class);
            CloudPartition partial = Mockito.mock(CloudPartition.class);
            CloudPartition delayed = Mockito.mock(CloudPartition.class);
            var parts = List.of(fresh, partial, delayed);
            OlapTable table = Mockito.mock(OlapTable.class);
            var lock = new ReentrantReadWriteLock();
            Mockito.doAnswer(call -> {
                lock.readLock().lock();
                return null;
            }).when(table).readLock();
            Mockito.doAnswer(call -> {
                lock.readLock().unlock();
                return null;
            }).when(table).readUnlock();
            Mockito.doAnswer(call -> {
                Assertions.assertTrue(lock.getReadHoldCount() > 0);
                return parts;
            }).when(table).getPartitions();
            ms.when(() -> CloudPartition.getSnapshotVisibleVersionFromMs(parts)).thenAnswer(call -> {
                Assertions.assertEquals(0, lock.getReadHoldCount());
                return List.of(1L, 3L, 7L);
            });
            long indexId = 99;
            var local = Mockito.mock(MaterializedIndex.class);
            var remote = Mockito.mock(MaterializedIndex.class);
            Mockito.when(partial.getIndex(indexId)).thenAnswer(call -> {
                Assertions.assertTrue(lock.getReadHoldCount() > 0);
                return local;
            });
            Mockito.when(delayed.getIndex(indexId)).thenReturn(remote);
            var present = Mockito.mock(LocalTablet.class);
            var zero = Mockito.mock(LocalTablet.class);
            var late = Mockito.mock(LocalTablet.class);
            Mockito.when(present.getId()).thenReturn(101L);
            Mockito.when(zero.getId()).thenReturn(102L);
            Mockito.when(late.getId()).thenReturn(103L);
            Mockito.when(present.getMinReplicaRowCount(3)).thenReturn(55L);
            Mockito.when(local.getTablets()).thenAnswer(call -> {
                Assertions.assertTrue(lock.getReadHoldCount() > 0);
                return List.of(present, zero);
            });
            Mockito.when(remote.getTablets()).thenReturn(List.of(late));
            var frame = IndexSamplingSnapshot.read(table, indexId).scope().partitions();
            Assertions.assertEquals(List.of(new TabletSampleMeta(101, 55, false), new TabletSampleMeta(102, 55, true)),
                    frame.get(0).tablets());
            Assertions.assertEquals(List.of(new TabletSampleMeta(103, 55, true)), frame.get(1).tablets());
            Mockito.verify(fresh, Mockito.never()).getIndex(indexId);
            Mockito.verify(late).getMinReplicaRowCount(7);
            Mockito.verify(table, Mockito.never()).getRowCountForIndex(indexId, false);
            Assertions.assertEquals(IndexSamplingSnapshot.ReportState.READY,
                    IndexSamplingSnapshot.reportState(table, indexId));
            Mockito.when(table.getRowCountForIndex(indexId, false)).thenAnswer(call -> {
                Assertions.assertTrue(lock.getReadHoldCount() > 0);
                return 55L;
            });
            Assertions.assertEquals(55L, IndexSamplingSnapshot.reportedIndexRows(table, indexId, false));
            Assertions.assertEquals(0, lock.getReadHoldCount());
        }
    }

    @Test
    void lateZeroReportsKeepBothAutoAndManualStatistics() throws Exception {
        for (boolean cloud : List.of(false, true)) {
            try (MockedStatic<Config> config = Mockito.mockStatic(Config.class, Mockito.CALLS_REAL_METHODS);
                    MockedStatic<CloudPartition> ms = Mockito.mockStatic(CloudPartition.class)) {
                config.when(Config::isCloudMode).thenReturn(cloud);
                CloudPartition part = Mockito.mock(CloudPartition.class);
                Mockito.when(part.getVisibleVersion()).thenReturn(cloud ? 1L : 2L);
                ms.when(() -> CloudPartition.getSnapshotVisibleVersionFromMs(List.of(part))).thenReturn(List.of(2L));
                OlapTable table = Mockito.mock(OlapTable.class);
                Mockito.doReturn(List.of(part)).when(table).getPartitions();
                AnalysisManager manager = Mockito.mock(AnalysisManager.class);
                TableStatsMeta old = Mockito.mock(TableStatsMeta.class);
                Assertions.assertFalse(new StatisticsAutoCollector().readyToSample(table, 0, manager, old, true));
                Assertions.assertFalse(new StatisticsAutoCollector().readyToSample(table, 0, manager, old, false));
                Mockito.verifyNoInteractions(manager);

                OlapAnalysisTask task = Mockito.spy(new OlapAnalysisTask());
                task.tbl = table;
                task.info = new AnalysisInfoBuilder().setIndexId(42L).setRowCount(0).build();
                task.tableSample = new TableSample(false, 10L);
                task.job = Mockito.mock(AnalysisJob.class);
                Mockito.doNothing().when(task).prepareExecution();
                task.execute();
                Mockito.verify(task.job).taskDoneWithoutData(task);
                Mockito.verify(task.job, Mockito.never()).appendBuf(Mockito.any(), Mockito.any());
                Mockito.verify(task, Mockito.never()).runQuery(Mockito.anyString());
                Assertions.assertTrue(task.info.message.contains("row count"), task.info.message);

                // The same zero report has the original empty-table behavior at the initial version.
                Mockito.when(part.getVisibleVersion()).thenReturn(1L);
                ms.when(() -> CloudPartition.getSnapshotVisibleVersionFromMs(List.of(part))).thenReturn(List.of(1L));
                Assertions.assertFalse(new StatisticsAutoCollector().readyToSample(table, 0, manager, old, true));
                Mockito.verify(manager).dropStats(table, null);
                Mockito.when(task.job.getJobInfo()).thenReturn(task.info);
                task.execute();
                Mockito.verify(task.job).appendBuf(Mockito.eq(task), Mockito.argThat(
                        rows -> rows.size() == 1 && rows.get(0).count == 0));
                Assertions.assertEquals(0L, task.info.indexesRowCount.get(42L));
            }
        }
    }
}
