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

import org.apache.doris.analysis.FixedBlockSampleSpec;
import org.apache.doris.analysis.TableSample;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.statistics.repository.ResultRow;
import org.apache.doris.statistics.sampling.ColumnPhysicalLayout;
import org.apache.doris.statistics.sampling.HierarchicalFixedBudgetTabletSamplingMethod;
import org.apache.doris.statistics.sampling.IndexSamplingSnapshot;
import org.apache.doris.statistics.sampling.LegacyTabletSamplingMethod;
import org.apache.doris.statistics.sampling.PartitionSampleMeta;
import org.apache.doris.statistics.sampling.RandomAggTabletSamplingMethod;
import org.apache.doris.statistics.sampling.TabletSampleMeta;
import org.apache.doris.statistics.sampling.TabletSamplingScope;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.thrift.TStorageType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class KeyRangeBlockSamplingTest {
    private static ResultRow sample(long n, long z, long d, long singletons, long bytes) {
        var values = new ArrayList<>(Collections.nCopies(16, "0"));
        values.set(7, Long.toString(n));
        values.set(8, Long.toString(d));
        values.set(9, Long.toString(z));
        values.set(12, Long.toString(bytes));
        values.set(15, Long.toString(singletons));
        return new ResultRow(values);
    }

    @ParameterizedTest
    @CsvSource({"true,0.1", "false,0.1", "true,0.5", "false,0.5"})
    void freezesFirstLogicalEstimateAndRecomputesBothColumns(boolean aFirst, double qB) throws Exception {
        AnalysisJob job = new AnalysisJob(new AnalysisInfoBuilder().build(), List.of());
        var start = new CyclicBarrier(2);
        var frozen = new CountDownLatch(1);
        var threads = Executors.newFixedThreadPool(2);
        try {
            var a = threads.submit(() -> {
                start.await(5, TimeUnit.SECONDS);
                if (!aFirst) {
                    Assertions.assertTrue(frozen.await(5, TimeUnit.SECONDS));
                }
                ResultRow result = OlapAnalysisTask.freezeLogicalSample(sample(20, 0, 20, 20, 80),
                        job, 7, 0.1, true);
                frozen.countDown();
                return result;
            });
            var b = threads.submit(() -> {
                start.await(5, TimeUnit.SECONDS);
                if (aFirst) {
                    Assertions.assertTrue(frozen.await(5, TimeUnit.SECONDS));
                }
                ResultRow result = OlapAnalysisTask.freezeLogicalSample(sample(100, 20, 60, 50, 480),
                        job, 7, qB, false);
                frozen.countDown();
                return result;
            });
            ResultRow ra = a.get(10, TimeUnit.SECONDS);
            ResultRow rb = b.get(10, TimeUnit.SECONDS);
            Assertions.assertEquals(ra.get(7), rb.get(7));
            Assertions.assertEquals(ra.get(7), ra.get(8));
            boolean frozenAt200 = aFirst || qB == 0.5;
            Assertions.assertEquals(frozenAt200 ? "200" : "1000", rb.get(7));
            Assertions.assertEquals(frozenAt200 ? "96" : "141", rb.get(8));
            Assertions.assertEquals(frozenAt200 ? "40" : "200", rb.get(9));
            Assertions.assertEquals(frozenAt200 ? "960" : "4800", rb.get(12));
            Assertions.assertEquals(frozenAt200 ? "800" : "4000", ra.get(12));
            Assertions.assertThrows(AnalyzeSkipException.class, () -> OlapAnalysisTask.freezeLogicalSample(
                    sample(0, 0, 0, 0, 0), job, 7, 0.1, false));
        } finally {
            threads.shutdownNow();
        }
    }

    @Test
    void emptySampleAndInvalidMomentsCannotWinTheFreeze() {
        AnalysisJob job = new AnalysisJob(new AnalysisInfoBuilder().build(), List.of());
        Assertions.assertThrows(AnalyzeSkipException.class, () -> OlapAnalysisTask.freezeLogicalSample(
                sample(0, 0, 0, 0, 0), job, 7, 0.1, false));
        Assertions.assertThrows(IllegalStateException.class, () -> OlapAnalysisTask.freezeLogicalSample(
                sample(1, 2, 1, 0, 8), job, 7, 0.1, false));
        Assertions.assertThrows(ArithmeticException.class, () -> OlapAnalysisTask.freezeLogicalSample(
                sample(1, 0, 1, 1, Long.MAX_VALUE), job, 7, 0.5, false));
        Assertions.assertEquals(81, job.freezeIndexRows(7, () -> 81));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void admitsOnlyHashRoutedIndexAndFreezesChoiceForAllItsTargets(boolean agg) {
        var table = Mockito.mock(OlapTable.class);
        var meta = Mockito.mock(MaterializedIndexMeta.class);
        var k = new Column("k", Type.INT, true, null, "0", "");
        var k2 = new Column("k2", Type.INT, true, null, "0", "");
        var v = new Column("v", Type.INT);
        var hash = new HashDistributionInfo(1, List.of(k));
        var rollupKey = new Column(k);
        rollupKey.setUniqueId(99);
        Mockito.when(table.getIndexMetaByIndexId(17L)).thenReturn(meta);
        Mockito.when(table.getDistributionColumnNames()).thenReturn(Set.of("k"));
        Mockito.when(table.getDefaultDistributionInfo()).thenReturn(hash);
        var part = Mockito.mock(Partition.class);
        Mockito.doReturn(List.of(part)).when(table).getPartitions();
        Mockito.when(meta.getKeysType()).thenReturn(agg ? KeysType.AGG_KEYS : KeysType.UNIQUE_KEYS);
        Mockito.when(meta.getSchema()).thenReturn(List.of(rollupKey, k2, v));
        Mockito.when(meta.getColumnByName("v")).thenReturn(v);
        Mockito.when(meta.getColumnByName("k")).thenReturn(rollupKey);
        Mockito.when(part.getDistributionInfo()).thenReturn(new RandomDistributionInfo(1));
        Assertions.assertFalse(FixedBlockSampleSpec.supportsKeyRange(table, 17));
        Mockito.when(part.getDistributionInfo()).thenReturn(hash);
        Assertions.assertTrue(FixedBlockSampleSpec.supportsKeyRange(table, 17));
        Mockito.when(meta.getSchema()).thenReturn(List.of(k2, v));
        Assertions.assertFalse(FixedBlockSampleSpec.supportsKeyRange(table, 17));
        Mockito.when(meta.getSchema()).thenReturn(List.of(rollupKey, k2, v));

        var value = new OlapAnalysisTask();
        value.info = new AnalysisInfoBuilder().setIndexId(17L).setColName("v").build();
        value.tbl = table;
        value.tableSample = new TableSample(false, 10L);
        var key = new OlapAnalysisTask();
        key.tableSample = value.tableSample;
        key.info = new AnalysisInfoBuilder().setIndexId(17L).setColName("k").build();
        var layout = new ColumnPhysicalLayout(false, false, false, false);
        var frame = Mockito.mock(IndexSamplingSnapshot.class);
        Mockito.when(frame.scope()).thenReturn(TabletSamplingScope.table(List.of(
                new PartitionSampleMeta(1, List.of(new TabletSampleMeta(7, 20000, false))))));
        Mockito.when(frame.allCandidatesSupport(true)).thenReturn(true);
        try (var util = Mockito.mockStatic(StatisticsUtil.class)) {
            util.when(StatisticsUtil::getStatisticsSampleUnitRows).thenReturn(8);
            util.when(StatisticsUtil::enableFixedBlockStatsSampling).thenReturn(true);
            value.job = Mockito.spy(new AnalysisJob(value.info, List.of(value)));
            Mockito.doReturn(frame).when(value.job).samplingFrame(table, 17);
            Assertions.assertInstanceOf(HierarchicalFixedBudgetTabletSamplingMethod.class,
                    value.chooseTabletSamplingMethod(table, meta.getKeysType(), false, layout, 20000));
            value.job = Mockito.spy(new AnalysisJob(value.info, List.of(key, value)));
            Mockito.doReturn(frame).when(value.job).samplingFrame(table, 17);
            Assertions.assertInstanceOf(HierarchicalFixedBudgetTabletSamplingMethod.class,
                    value.chooseTabletSamplingMethod(table, meta.getKeysType(), false, layout, 20000));
            Mockito.when(frame.allCandidatesSupport(true)).thenReturn(false);
            value.job = Mockito.spy(new AnalysisJob(value.info, List.of(key, value)));
            Mockito.doReturn(frame).when(value.job).samplingFrame(table, 17);
            Assertions.assertInstanceOf(LegacyTabletSamplingMethod.class,
                    value.chooseTabletSamplingMethod(table, meta.getKeysType(), false, layout, 20000));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void choosesMethodForSurvivingSampledColumnsByLiteralName(boolean sampleBucketColumn) {
        var table = Mockito.mock(OlapTable.class);
        var bucket = new Column("bu`cket", Type.INT, true, null, "0", "");
        var secondKey = new Column("k2", Type.INT, true, null, "0", "");
        var value = new Column("bucket", Type.INT);
        var meta = new MaterializedIndexMeta(17, List.of(bucket, secondKey, value), 1, 1, (short) 1,
                TStorageType.COLUMN, KeysType.AGG_KEYS, null);
        // Both names normalize to the same catalog entry, but only the literal bu`cket is a bucket column.
        Assertions.assertSame(value, meta.getColumnByName(bucket.getName()));
        Mockito.when(table.getBaseIndexId()).thenReturn(17L);
        Mockito.when(table.getIndexMetaByIndexId(17L)).thenReturn(meta);
        Mockito.when(table.getDefaultDistributionInfo()).thenReturn(new HashDistributionInfo(1, List.of(bucket)));
        Mockito.when(table.getDistributionColumnNames()).thenReturn(Set.of(bucket.getName()));
        var frame = Mockito.mock(IndexSamplingSnapshot.class);
        Mockito.when(frame.scope()).thenReturn(TabletSamplingScope.table(List.of(
                new PartitionSampleMeta(1, List.of(new TabletSampleMeta(7, 20000, false))))));
        Mockito.when(frame.allCandidatesSupport(true)).thenReturn(true);

        var sample = new TableSample(false, 10L);
        var survivor = analysisTask(table, value.getName(), sample);
        var dropped = analysisTask(table, "removed_by_ddl", sample);
        var sibling = analysisTask(table, bucket.getName(), sampleBucketColumn ? sample : null);
        try (var util = Mockito.mockStatic(StatisticsUtil.class)) {
            util.when(StatisticsUtil::getStatisticsSampleUnitRows).thenReturn(8);
            util.when(StatisticsUtil::enableFixedBlockStatsSampling).thenReturn(true);
            var job = Mockito.spy(new AnalysisJob(survivor.info, List.of(dropped, sibling, survivor)));
            dropped.job = job;
            survivor.job = job;
            Mockito.doReturn(frame).when(job).samplingFrame(table, 17);
            Mockito.doReturn(20000L).when(job).reportedIndexRows(table, 17);
            Assertions.assertThrows(AnalyzeSkipException.class, dropped::doSample);
            Assertions.assertEquals(sampleBucketColumn, job.sampleColumns(17, 17).contains(bucket.getName()));
            var method = survivor.chooseTabletSamplingMethod(table, KeysType.AGG_KEYS, false,
                    new ColumnPhysicalLayout(false, false, false, false), 20000);
            Assertions.assertInstanceOf(HierarchicalFixedBudgetTabletSamplingMethod.class, method);
        }
    }

    @Test
    void domainRateScalesBucketSubsampleMomentsAndRandomAggUsesKeyHashAndInSampleBounds() {
        AnalysisJob job = new AnalysisJob(new AnalysisInfoBuilder().build(), List.of());
        // Unique keys (f1 == m): domainRate cancels out, yielding exact frozen row count.
        ResultRow uniqueBuckets = OlapAnalysisTask.freezeLogicalSample(sample(100, 0, 100, 100, 400),
                job, 101, 0.1, false, 0.25);
        Assertions.assertEquals("1000", uniqueBuckets.get(7));
        Assertions.assertEquals("1000", uniqueBuckets.get(8));
        // Heavy duplicates per bucket (f1 == 0, d == 10 in 25% of buckets): recovers 10 / 0.25 = 40.
        ResultRow repeatedBuckets = OlapAnalysisTask.freezeLogicalSample(sample(100, 0, 10, 0, 400),
                job, 102, 0.1, false, 0.25);
        Assertions.assertEquals("1000", repeatedBuckets.get(7));
        Assertions.assertEquals("40", repeatedBuckets.get(8));
        var sem = new org.apache.doris.statistics.sampling.TabletSampleSpec.EqualProbabilitySamplingSemantics(
                0.05, 0.25, 0.5);
        Assertions.assertEquals(0.25, OlapAnalysisTask.subdomainRate(
                new ColumnPhysicalLayout(true, true, false, false), false, sem));
        Assertions.assertEquals(0.5, OlapAnalysisTask.subdomainRate(
                new ColumnPhysicalLayout(true, false, false, true), false, sem));
        Assertions.assertEquals(0.125, OlapAnalysisTask.subdomainRate(
                new ColumnPhysicalLayout(true, true, false, true), false, sem));

        var table = Mockito.mock(OlapTable.class);
        var k1 = new Column("k1", Type.INT, true, null, "0", "");
        var v = new Column("v", Type.BIGINT, false, AggregateType.SUM, "0", "");
        var meta = new MaterializedIndexMeta(17, List.of(k1, v), 1, 1, (short) 1,
                TStorageType.COLUMN, KeysType.AGG_KEYS, null);
        var part1 = Mockito.mock(Partition.class);
        var part2 = Mockito.mock(Partition.class);
        Mockito.when(part1.getDistributionInfo()).thenReturn(new RandomDistributionInfo(2));
        Mockito.when(part2.getDistributionInfo()).thenReturn(new RandomDistributionInfo(2));
        Mockito.when(table.getBaseIndexId()).thenReturn(17L);
        Mockito.when(table.getIndexNameById(17L)).thenReturn("tbl");
        Mockito.when(table.getName()).thenReturn("tbl");
        Mockito.when(table.getKeysType()).thenReturn(KeysType.AGG_KEYS);
        Mockito.when(table.getIndexMetaByIndexId(17L)).thenReturn(meta);
        Mockito.when(table.getDefaultDistributionInfo()).thenReturn(new RandomDistributionInfo(2));
        Mockito.doReturn(List.of(part1, part2)).when(table).getPartitions();
        Mockito.when(table.getPartitionInfo()).thenReturn(Mockito.mock(PartitionInfo.class));
        Assertions.assertTrue(FixedBlockSampleSpec.supportsRandomAgg(table, 17));
        Assertions.assertTrue(FixedBlockSampleSpec.supportsMergedSampling(table, 17));

        var frame = Mockito.mock(IndexSamplingSnapshot.class);
        Mockito.when(frame.scope()).thenReturn(TabletSamplingScope.table(List.of(
                new PartitionSampleMeta(1, List.of(new TabletSampleMeta(11, 50000, false),
                        new TabletSampleMeta(12, 50000, false))),
                new PartitionSampleMeta(2, List.of(new TabletSampleMeta(21, 50000, false),
                        new TabletSampleMeta(22, 50000, false))))));
        Mockito.when(frame.allCandidatesSupportFixedBlock()).thenReturn(true);

        var task = Mockito.spy(analysisTask(table, "v", new TableSample(false, 1000L)));
        task.catalog = Mockito.mock(CatalogIf.class);
        task.db = Mockito.mock(DatabaseIf.class);
        Mockito.when(task.catalog.getName()).thenReturn("internal");
        Mockito.when(task.db.getFullName()).thenReturn("db");
        var randomJob = Mockito.spy(new AnalysisJob(task.info, List.of(task)));
        task.job = randomJob;
        Mockito.doReturn(frame).when(randomJob).samplingFrame(table, 17);
        Mockito.doReturn(200000L).when(randomJob).reportedIndexRows(table, 17);
        var captured = org.mockito.ArgumentCaptor.forClass(String.class);
        Mockito.doNothing().when(task).runQuery(captured.capture());
        try (var util = Mockito.mockStatic(StatisticsUtil.class, Mockito.CALLS_REAL_METHODS)) {
            util.when(StatisticsUtil::enableFixedBlockStatsSampling).thenReturn(true);
            util.when(StatisticsUtil::getStatisticsSampleUnitRows).thenReturn(1024);
            util.when(StatisticsUtil::getStatisticsMaxSampleTablets).thenReturn(64);
            Assertions.assertInstanceOf(RandomAggTabletSamplingMethod.class,
                    task.chooseTabletSamplingMethod(table, KeysType.AGG_KEYS, false,
                            new ColumnPhysicalLayout(false, false, false, false), 200000));
            task.doSample();
        }
        // Value column on AGG_KEYS computes MIN/MAX inside the sample scan without calling getSampleMinMax().
        Mockito.verify(task, Mockito.never()).getSampleMinMax();
        String sql = captured.getValue();
        Assertions.assertTrue(sql.contains("TABLESAMPLE (FIXED_BLOCK"), sql);
        Assertions.assertTrue(sql.contains("unit_rate = 1.0"), sql);
        Assertions.assertTrue(sql.contains("WHERE (xxhash_64("), sql);
        Assertions.assertTrue(sql.contains("sample_min AS min, sample_max AS max"), sql);
    }

    private static OlapAnalysisTask analysisTask(OlapTable table, String name, TableSample sample) {
        var task = new OlapAnalysisTask();
        task.tbl = table;
        task.info = new AnalysisInfoBuilder().setIndexId(17L).setColName(name).setRowCount(20000).build();
        task.tableSample = sample;
        return task;
    }
}
