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
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.LocalTablet;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.statistics.repository.ColStatsData;
import org.apache.doris.statistics.repository.ResultRow;
import org.apache.doris.statistics.sampling.IndexSamplingSnapshot;
import org.apache.doris.statistics.sampling.PartitionSampleMeta;
import org.apache.doris.statistics.sampling.TabletSampleMeta;
import org.apache.doris.statistics.sampling.TabletSamplingScope;
import org.apache.doris.thrift.TStorageType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

class ColumnSampleBatchCollectorTest {
    private final InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
    private final Database db = Mockito.mock(Database.class);
    private final OlapTable table = Mockito.mock(OlapTable.class);
    private MaterializedIndexMeta meta;

    @BeforeEach
    void setUp() {
        Mockito.when(catalog.getId()).thenReturn(0L);
        Mockito.when(catalog.getName()).thenReturn("internal");
        Mockito.when(db.getId()).thenReturn(1L);
        Mockito.when(db.getFullName()).thenReturn("db");
        Mockito.when(table.getId()).thenReturn(2L);
        Mockito.when(table.getName()).thenReturn("wide_tbl");
        Mockito.when(table.getBaseIndexId()).thenReturn(10L);
        Column k = new Column("k", Type.BIGINT, true, null, "0", "");
        Column v1 = new Column("v1", Type.BIGINT, false, AggregateType.SUM, "0", "");
        Column v2 = new Column("v2", ScalarType.createVarchar(64), false, AggregateType.REPLACE, "", "");
        Column vLong = new Column("v_long", Type.STRING, false, AggregateType.REPLACE, "", "");
        meta = new MaterializedIndexMeta(10L, List.of(k, v1, v2, vLong), 1, 1, (short) 1,
                TStorageType.COLUMN, KeysType.AGG_KEYS, null);
        Mockito.when(table.getIndexMetaByIndexId(10L)).thenReturn(meta);
        Mockito.when(table.getKeysType()).thenReturn(KeysType.AGG_KEYS);
        Mockito.when(table.getDefaultDistributionInfo()).thenReturn(new HashDistributionInfo(4, List.of(k)));
        Mockito.when(table.getDistributionColumnNames()).thenReturn(Set.of("k"));
        Mockito.doReturn(List.of()).when(table).getPartitions();
    }

    private OlapAnalysisTask task(String colName, AnalysisInfo.JobType jobType) {
        OlapAnalysisTask t = Mockito.spy(new OlapAnalysisTask());
        t.info = new AnalysisInfoBuilder()
                .setJobId(99L)
                .setCatalogId(0L)
                .setDBId(1L)
                .setTblId(2L)
                .setIndexId(-1L)
                .setColName(colName)
                .setRowCount(100_000L)
                .setJobColumns(new HashSet<>())
                .setPartitionNames(new HashSet<>())
                .setPartitionUpdateRows(new ConcurrentHashMap<>())
                .setJobType(jobType)
                .build();
        t.tableSample = new TableSample(false, 1000L, 42L);
        t.catalog = catalog;
        t.db = db;
        t.tbl = table;
        t.col = meta.getColumnByName(colName);
        return t;
    }

    @Test
    void batchesMultipleColumnsIntoSingleSampleScanAndSkipsOnlyOversizedStringColumn() throws Exception {
        OlapAnalysisTask tk = task("k", AnalysisInfo.JobType.MANUAL);
        OlapAnalysisTask tv1 = task("v1", AnalysisInfo.JobType.MANUAL);
        OlapAnalysisTask tv2 = task("v2", AnalysisInfo.JobType.MANUAL);
        OlapAnalysisTask tvLong = task("v_long", AnalysisInfo.JobType.MANUAL);
        Mockito.doReturn(new ResultRow(List.of("1", "99999"))).when(tk).getSampleMinMax();

        IndexSamplingSnapshot frame = Mockito.mock(IndexSamplingSnapshot.class);
        Mockito.when(frame.scope()).thenReturn(TabletSamplingScope.table(List.of(
                new PartitionSampleMeta(1L, List.of(
                        new TabletSampleMeta(101L, 50_000L, false),
                        new TabletSampleMeta(102L, 50_000L, false))))));
        Mockito.when(frame.allCandidatesSupport(true)).thenReturn(true);

        List<OlapAnalysisTask> tasks = List.of(tk, tv1, tv2, tvLong);
        AnalysisJob job = Mockito.spy(new AnalysisJob(tk.info, tasks));
        tasks.forEach(t -> t.job = job);
        Mockito.doReturn(100_000L).when(job).reportedIndexRows(table, 10L);
        Mockito.doReturn(frame).when(job).samplingFrame(table, 10L);
        List<ColStatsData> published = new ArrayList<>();
        Mockito.doAnswer(inv -> {
            List<ColStatsData> data = inv.getArgument(1);
            published.addAll(data);
            return null;
        }).when(job).appendBuf(Mockito.any(), Mockito.anyList());
        Mockito.doNothing().when(job).taskDoneWithoutData(Mockito.any());
        for (OlapAnalysisTask t : tasks) {
            Mockito.doNothing().when(t).prepareExecution();
        }

        List<String> executedSqls = new ArrayList<>();
        List<Integer> observedParallelism = new ArrayList<>();
        try (MockedConstruction<StmtExecutor> ignored = Mockito.mockConstruction(StmtExecutor.class, (exec, ctx) -> {
            ConnectContext cc = (ConnectContext) ctx.arguments().get(0);
            String sql = (String) ctx.arguments().get(1);
            Mockito.when(exec.getContext()).thenReturn(cc);
            Mockito.when(exec.executeInternalQuery()).thenAnswer(inv -> {
                executedSqls.add(sql);
                observedParallelism.add(cc.getSessionVariable().parallelPipelineTaskNum);
                // Return 4 rows (ord 0..3): k, v1, v2, v_long (with __over = 1 on v_long)
                return List.of(
                        new ResultRow(Arrays.asList("0", "0", "2--1-k", "0", "1", "2", "-1", "k", null,
                                "1000", "1000", "0", null, null, "8000", "2026-11-25 00:00:00", null, "1000")),
                        new ResultRow(Arrays.asList("1", "0", "2--1-v1", "0", "1", "2", "-1", "v1", null,
                                "1000", "50", "0", "10", "500", "8000", "2026-11-25 00:00:00", "", "10")),
                        new ResultRow(Arrays.asList("2", "0", "2--1-v2", "0", "1", "2", "-1", "v2", null,
                                "1000", "20", "100", "a", "z", "4500", "2026-11-25 00:00:00", "'a':0.5", "5")),
                        new ResultRow(Arrays.asList("3", "1", "2--1-v_long", "0", "1", "2", "-1", "v_long", null,
                                "0", "0", "0", null, null, "0", "2026-11-25 00:00:00", "", "0")));
            });
        })) {
            for (OlapAnalysisTask t : tasks) {
                t.execute();
            }
        }

        // All 4 columns shared ONE single sampled scan SQL, and MANUAL job used parallel instance num!
        Assertions.assertEquals(1, executedSqls.size());
        Assertions.assertTrue(executedSqls.get(0).contains("WITH `sample_base` AS"));
        Assertions.assertEquals(Config.manual_statistics_sql_parallel_exec_instance_num, observedParallelism.get(0));

        // k, v1, v2 published stats; v_long was skipped via over_limit without failing sibling columns!
        Assertions.assertEquals(3, published.size());
        Assertions.assertEquals(Set.of("k", "v1", "v2"),
                Set.of(published.get(0).statsId.colId, published.get(1).statsId.colId, published.get(2).statsId.colId));
        ColStatsData kData = published.stream().filter(d -> d.statsId.colId.equals("k")).findFirst().orElseThrow();
        Assertions.assertEquals("1", kData.minLit);
        Assertions.assertEquals("99999", kData.maxLit);
        Assertions.assertTrue(tvLong.info.message.contains("statistics_max_string_column_length"), tvLong.info.message);
        Mockito.verify(job).taskDoneWithoutData(tvLong);
        // Value columns never invoked getSampleMinMax().
        Mockito.verify(tv1, Mockito.never()).getSampleMinMax();
        Mockito.verify(tv2, Mockito.never()).getSampleMinMax();
        Mockito.verify(tvLong, Mockito.never()).getSampleMinMax();
    }

    @Test
    void prefersInSampleBoundsForRandomAggRowTtlLongStringsAndHighTabletCounts() {
        Column key = meta.getColumnByName("k");
        Column shortStr = meta.getColumnByName("v2");
        Column longStr = meta.getColumnByName("v_long");
        AnalysisInfo emptyParts = new AnalysisInfoBuilder().setPartitionNames(Set.of()).build();

        // Hash AGG key with <= 512 tablets uses metadata min/max (prefersInSampleBounds == false).
        Assertions.assertFalse(OlapAnalysisTask.prefersInSampleBounds(table, meta, key, 10L, emptyParts));
        // Long STRING (>= 512B max length) uses in-sample bounds to avoid cut ZoneMaps.
        Assertions.assertTrue(OlapAnalysisTask.prefersInSampleBounds(table, meta, longStr, 10L, emptyParts));

        // DUP_KEYS table checks: row_ttl, partition scope, and > maxSampleTablets.
        OlapTable dup = Mockito.mock(OlapTable.class);
        MaterializedIndexMeta dupMeta = new MaterializedIndexMeta(10L, List.of(key, shortStr, longStr),
                1, 1, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS, null);
        Mockito.when(dup.getKeysType()).thenReturn(KeysType.DUP_KEYS);
        Mockito.when(dup.getBaseIndexId()).thenReturn(10L);
        Mockito.when(dup.getIndexMetaByIndexId(10L)).thenReturn(dupMeta);
        Mockito.doReturn(List.of()).when(dup).getPartitions();

        Assertions.assertFalse(OlapAnalysisTask.prefersInSampleBounds(dup, dupMeta, shortStr, 10L, emptyParts));
        Assertions.assertTrue(OlapAnalysisTask.prefersInSampleBounds(dup, dupMeta, longStr, 10L, emptyParts));

        AnalysisInfo scopedParts = new AnalysisInfoBuilder().setPartitionNames(Set.of("p1")).build();
        Assertions.assertTrue(OlapAnalysisTask.prefersInSampleBounds(dup, dupMeta, shortStr, 10L, scopedParts));

        Mockito.when(dup.hasRowTtl()).thenReturn(true);
        Assertions.assertTrue(OlapAnalysisTask.prefersInSampleBounds(dup, dupMeta, shortStr, 10L, emptyParts));
        Mockito.when(dup.hasRowTtl()).thenReturn(false);

        // > 512 tablets uses in-sample bounds instead of touching all tablets across the cluster.
        Partition part = Mockito.mock(Partition.class);
        MaterializedIndex idx = Mockito.mock(MaterializedIndex.class);
        List<org.apache.doris.catalog.Tablet> manyTablets = IntStream.range(0, 600)
                .mapToObj(i -> (org.apache.doris.catalog.Tablet) new LocalTablet(1000L + i))
                .toList();
        Mockito.when(part.getIndex(10L)).thenReturn(idx);
        Mockito.when(idx.getTablets()).thenReturn(manyTablets);
        Mockito.doReturn(List.of(part)).when(dup).getPartitions();
        Assertions.assertTrue(OlapAnalysisTask.prefersInSampleBounds(dup, dupMeta, key, 10L, emptyParts));

        // RANDOM AGG_KEYS key columns use in-sample bounds because Nereids GROUP BY blocks VStatisticsIterator.
        OlapTable randAgg = Mockito.mock(OlapTable.class);
        Mockito.when(randAgg.getKeysType()).thenReturn(KeysType.AGG_KEYS);
        Mockito.when(randAgg.getBaseIndexId()).thenReturn(10L);
        Mockito.when(randAgg.getDefaultDistributionInfo()).thenReturn(new RandomDistributionInfo(4));
        MaterializedIndexMeta randMeta = new MaterializedIndexMeta(10L,
                List.of(key, new Column("v1", Type.BIGINT, false, AggregateType.SUM, "0", "")),
                1, 1, (short) 1, TStorageType.COLUMN, KeysType.AGG_KEYS, null);
        Mockito.when(randAgg.getIndexMetaByIndexId(10L)).thenReturn(randMeta);
        Mockito.doReturn(List.of()).when(randAgg).getPartitions();
        Assertions.assertTrue(OlapAnalysisTask.prefersInSampleBounds(randAgg, randMeta, key, 10L, emptyParts));

        // Job parallelism decoupling: SYSTEM stays at 1, MANUAL uses manual_statistics_sql_parallel_exec_instance_num.
        SessionVariable sysVars = new SessionVariable();
        sysVars.parallelPipelineTaskNum = 1;
        BaseAnalysisTask.configureJobParallelism(sysVars,
                new AnalysisInfoBuilder().setJobType(AnalysisInfo.JobType.SYSTEM).build());
        Assertions.assertEquals(1, sysVars.parallelPipelineTaskNum);
        SessionVariable manualVars = new SessionVariable();
        manualVars.parallelPipelineTaskNum = 1;
        BaseAnalysisTask.configureJobParallelism(manualVars,
                new AnalysisInfoBuilder().setJobType(AnalysisInfo.JobType.MANUAL).build());
        Assertions.assertEquals(Config.manual_statistics_sql_parallel_exec_instance_num,
                manualVars.parallelPipelineTaskNum);
    }

    @Test
    void failedBatchFallsBackToIndividualSampleQueriesAndDisabledVariableSkipsBatching() throws Exception {
        OlapAnalysisTask tk = task("k", AnalysisInfo.JobType.SYSTEM);
        OlapAnalysisTask tv1 = task("v1", AnalysisInfo.JobType.SYSTEM);
        IndexSamplingSnapshot frame = Mockito.mock(IndexSamplingSnapshot.class);
        Mockito.when(frame.scope()).thenReturn(TabletSamplingScope.table(List.of(
                new PartitionSampleMeta(1L, List.of(new TabletSampleMeta(101L, 50_000L, false))))));
        Mockito.when(frame.allCandidatesSupport(true)).thenReturn(true);

        // 1. When statistics_sample_batch_columns <= 1, tasks do not wire sampleBatch.
        int prev = VariableMgr.getDefaultSessionVariable().statisticsSampleBatchColumns;
        try {
            VariableMgr.getDefaultSessionVariable().statisticsSampleBatchColumns = 1;
            new AnalysisJob(tk.info, List.of(tk, tv1));
            Assertions.assertNull(tk.sampleBatch);
            Assertions.assertNull(tv1.sampleBatch);
        } finally {
            VariableMgr.getDefaultSessionVariable().statisticsSampleBatchColumns = prev;
        }

        // 2. When batch SQL fails with an ordinary error, both columns fall back cleanly to individual runQuery.
        AnalysisJob job = Mockito.spy(new AnalysisJob(tk.info, List.of(tk, tv1)));
        tk.job = job;
        tv1.job = job;
        Assertions.assertNotNull(tk.sampleBatch);
        Mockito.doReturn(50_000L).when(job).reportedIndexRows(table, 10L);
        Mockito.doReturn(frame).when(job).samplingFrame(table, 10L);
        Mockito.doReturn(new ResultRow(List.of("1", "100"))).when(tk).getSampleMinMax();
        ArgumentCaptor<String> fallbackSqls = ArgumentCaptor.forClass(String.class);
        Mockito.doNothing().when(tk).runQuery(fallbackSqls.capture());
        Mockito.doNothing().when(tv1).runQuery(fallbackSqls.capture());

        try (MockedConstruction<StmtExecutor> ignored = Mockito.mockConstruction(StmtExecutor.class, (exec, ctx) -> {
            Mockito.when(exec.getContext()).thenReturn((ConnectContext) ctx.arguments().get(0));
            Mockito.when(exec.executeInternalQuery()).thenThrow(new IllegalStateException("simulated batch failure"));
        })) {
            tk.doSample();
            tv1.doSample();
        }
        Assertions.assertEquals(2, fallbackSqls.getAllValues().size());
    }
}
