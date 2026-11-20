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

package org.apache.doris.datasource.hive.source;

import org.apache.doris.analysis.TableSample;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.blockrule.SqlBlockRuleMgr;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.FederationBackendPolicy;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.FileSplitter;
import org.apache.doris.datasource.SplitAssignment;
import org.apache.doris.datasource.SplitSource;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveExternalMetaCache;
import org.apache.doris.datasource.hive.HiveExternalMetaCache.FileCacheValue;
import org.apache.doris.datasource.hive.HiveExternalMetaCache.HiveFileStatus;
import org.apache.doris.datasource.hive.HivePartition;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class HiveSplitEstimateTest {
    @Test
    public void testUniformPartitionsAreIndependentOfBatchSize() throws Exception {
        for (int batchSize : new int[] {16, 2, 1}) {
            Assert.assertEquals(60, runBatchEstimate(batchSize, false, 10, 10, 10, 10, 10, 10));
        }
    }

    @Test
    public void testUnevenAndEmptyPartitionsKeepTheLargestPartitionEstimate() throws Exception {
        for (int batchSize : new int[] {16, 2, 1}) {
            Assert.assertEquals(100, runBatchEstimate(batchSize, false, 0, 1, 25, 3));
        }
    }

    @Test
    public void testSampledPartitionsKeepTheSameEstimate() throws Exception {
        for (int batchSize : new int[] {16, 2}) {
            Assert.assertEquals(60, runBatchEstimate(batchSize, true, 10, 10, 10, 10, 10, 10));
        }
    }

    @Test
    public void testUnpartitionedScanKeepsItsSplitCount() throws Exception {
        Assert.assertEquals(25, runBatchEstimate(16, false, 25));
    }

    @Test
    public void testRowsSampleIsSelectedOnceAcrossAllPartitions() throws Exception {
        assertTableWideSample(new TableSample(false, 256L, 0L));
    }

    @Test
    public void testPercentSampleIsSelectedOnceAcrossAllPartitions() throws Exception {
        assertTableWideSample(new TableSample(true, 1L, 0L));
    }

    private void assertTableWideSample(TableSample sample) throws Exception {
        int[] fileCounts = new int[32];
        Arrays.fill(fileCounts, 1);
        for (int batchSize : new int[] {16, 2, 1}) {
            Assert.assertEquals(1, runSplitPlanning(batchSize, sample, true, fileCounts));
        }
    }

    private int runBatchEstimate(int batchSize, boolean sample, int... fileCounts) throws Exception {
        return runSplitPlanning(batchSize, sample ? new TableSample(true, 100L, 0L) : null,
                false, fileCounts);
    }

    private int runSplitPlanning(int batchSize, TableSample sample, boolean choosePlanningMode,
            int... fileCounts) throws Exception {
        SessionVariable variables = new SessionVariable();
        variables.splitPartitionBatchSize = batchSize;
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(table.getFileFormatType(variables)).thenReturn(TFileFormatType.FORMAT_PARQUET);
        Mockito.when(catalog.bindBrokerName()).thenReturn("");
        TupleDescriptor descriptor = new TupleDescriptor(new TupleId(0));
        descriptor.setTable(table);
        HiveScanNode node = new HiveScanNode(
                new PlanNodeId(0), descriptor, false, variables, null, ScanContext.EMPTY);
        Field splitter = FileQueryScanNode.class.getDeclaredField("fileSplitter");
        splitter.setAccessible(true);
        splitter.set(node, new FileSplitter(variables.maxInitialSplitSize,
                variables.maxSplitSize, variables.maxInitialSplitNum));
        node.setTableSample(sample);
        Mockito.when(table.getFullSchema()).thenReturn(Collections.singletonList(new Column("v", Type.INT)));
        List<HivePartition> partitions = new ArrayList<>();
        Map<HivePartition, FileCacheValue> files = new HashMap<>();
        int totalFiles = 0;
        for (int partition = 0; partition < fileCounts.length; partition++) {
            List<String> values = fileCounts.length == 1
                    ? null : Collections.singletonList(Integer.toString(partition));
            String path = "hdfs://warehouse/t/p=" + partition;
            HivePartition hivePartition = new HivePartition(null, false, "parquet", path,
                    values, Collections.emptyMap());
            partitions.add(hivePartition);
            FileCacheValue cacheValue = new FileCacheValue();
            cacheValue.setPartitionValues(values);
            for (int file = 0; file < fileCounts[partition]; file++) {
                HiveFileStatus status = new HiveFileStatus();
                status.setPath(LocationPath.of(path + "/file" + file + ".parquet"));
                status.setLength(1024);
                cacheValue.getFiles().add(status);
                totalFiles++;
            }
            files.put(hivePartition, cacheValue);
        }
        Field pruned = HiveScanNode.class.getDeclaredField("prunedPartitions");
        pruned.setAccessible(true);
        pruned.set(node, partitions);
        Field initialized = HiveScanNode.class.getDeclaredField("partitionInit");
        initialized.setAccessible(true);
        initialized.set(node, true);
        Backend backend = new Backend(1, "127.0.0.1", 9050);
        FederationBackendPolicy policy = Mockito.mock(FederationBackendPolicy.class);
        Mockito.when(policy.numBackends()).thenReturn(1);
        Mockito.when(policy.computeScanRangeAssignment(Mockito.anyList())).thenAnswer(call -> {
            Multimap<Backend, Split> assigned = ArrayListMultimap.create();
            for (Split split : call.<List<Split>>getArgument(0)) {
                assigned.put(backend, split);
            }
            return assigned;
        });
        // Retain the complete small fixture so scheduling can finish before consumption.
        // Exercise the real producer/assignment path rather than the removed addToQueue call.
        SplitAssignment assignment = Mockito.spy(new SplitAssignment(policy, node,
                (be, properties, split, keys, admission) -> new TScanRangeLocations(),
                Collections.emptyMap(), Collections.emptyList(), true,
                variables.getSplitBatchSize(), Math.max(1, totalFiles)));
        Field assignmentField = ScanNode.class.getDeclaredField("splitAssignment");
        assignmentField.setAccessible(true);
        assignmentField.set(node, assignment);
        CompletableFuture<Void> finished = new CompletableFuture<>();
        Mockito.doAnswer(call -> {
            call.callRealMethod();
            finished.complete(null);
            return null;
        }).when(assignment).finishSchedule();
        Mockito.doAnswer(call -> {
            finished.completeExceptionally(call.getArgument(0));
            return null;
        }).when(assignment).setException(Mockito.any());
        HiveExternalMetaCache cache = Mockito.mock(HiveExternalMetaCache.class);
        Mockito.when(cache.getFilesByPartitions(Mockito.anyList(), Mockito.anyBoolean(), Mockito.anyBoolean(),
                Mockito.isNull(), Mockito.same(table))).thenAnswer(call -> call.<List<HivePartition>>getArgument(0)
                        .stream().map(files::get).collect(Collectors.toList()));
        Mockito.when(cache.openFiles(Mockito.any(), Mockito.anyBoolean(), Mockito.isNull(),
                Mockito.same(table), Mockito.any())).thenAnswer(call ->
                        org.apache.doris.datasource.hive.HiveFileIterator.of(files.get(call.getArgument(0))));
        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr manager = Mockito.mock(ExternalMetaCacheMgr.class);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(manager);
        Mockito.when(env.getSqlBlockRuleMgr()).thenReturn(Mockito.mock(SqlBlockRuleMgr.class));
        Mockito.when(manager.hive(Mockito.anyLong())).thenReturn(cache);
        Mockito.when(manager.getScheduleExecutor()).thenReturn(executor);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            if (choosePlanningMode) {
                ConnectContext context = new ConnectContext();
                context.setThreadLocalInfo();
                try {
                    if (!node.isBatchMode()) {
                        return node.getSplits(1).size();
                    }
                } finally {
                    ConnectContext.remove();
                }
            }
            node.startSplit(1);
            finished.get(5, TimeUnit.SECONDS);
            Mockito.verify(assignment, Mockito.never()).setException(Mockito.any());
            int delivered = 0;
            SplitSource source = new SplitSource(backend, assignment, 1000);
            while (!source.isLastBatch()) {
                delivered += source.getNextBatch(variables.getSplitBatchSize()).size();
            }
            if (choosePlanningMode) {
                return delivered;
            }
            Assert.assertEquals(totalFiles, delivered);
            return node.numApproximateSplits();
        } finally {
            assignment.stop();
            executor.shutdownNow();
            Assert.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }
}
