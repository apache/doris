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

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.FederationBackendPolicy;
import org.apache.doris.datasource.FileSplit;
import org.apache.doris.datasource.FileSplitter;
import org.apache.doris.datasource.SplitAssignment;
import org.apache.doris.datasource.SplitProducer;
import org.apache.doris.datasource.SplitSource;
import org.apache.doris.datasource.SplitSourceManager;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveExternalMetaCache;
import org.apache.doris.datasource.hive.HivePartition;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class HiveStreamingSplitTest {
    @Test
    void realModeGateAndDispatchPreservePartitionOverlapAndEveryRange() throws Exception {
        try (Fixture f = new Fixture(8, 2)) {
            // Both gates qualify: partition routing must win and preserve parallel listing.
            Mockito.when(f.cache.getCachedFileCount(Mockito.anyList())).thenReturn(10000L);
            Assertions.assertTrue(f.node.isBatchMode());
            Mockito.verify(f.cache, Mockito.never()).getCachedFileCount(Mockito.anyList());
            f.node.startSplit(1);
            Assertions.assertTrue(f.entered.await(10, TimeUnit.SECONDS),
                    "Two actual partition listings must overlap through startSplit");
            Assertions.assertEquals(2, f.listings.get());
            f.release.countDown();
            List<Long> actual = f.drain();
            Set<Long> expected = new HashSet<>();
            for (int p = 0; p < 8; p++) {
                for (int offset = 0; offset < 17; offset++) {
                    expected.add(p * 100L + offset);
                }
            }
            Assertions.assertEquals(expected.size(), actual.size());
            Assertions.assertEquals(expected, new HashSet<>(actual));
        }
    }

    @Test
    void fileCountGateUsesOneProducerAndHonorsSmallConsumerBatch() throws Exception {
        try (Fixture f = new Fixture(2, 1024)) {
            Mockito.when(f.cache.getCachedFileCount(Mockito.anyList())).thenReturn(10000L);
            Assertions.assertTrue(f.node.isBatchMode());
            f.node.startSplit(1);
            f.release.countDown();
            List<Long> actual = f.drain();
            Assertions.assertEquals(34, actual.size());
            Assertions.assertEquals(34, new HashSet<>(actual).size());
            Assertions.assertEquals(1, f.peak.get());
            Mockito.verify(f.cache).getCachedFileCount(Mockito.anyList());
        }
    }

    @Test
    void stoppingBeforeConsumptionClosesProducersWithoutListingAllPartitions() throws Exception {
        try (Fixture f = new Fixture(100, 2)) {
            Assertions.assertTrue(f.node.isBatchMode());
            f.node.startSplit(1);
            Assertions.assertTrue(f.entered.await(10, TimeUnit.SECONDS));
            f.node.stop();
            f.release.countDown();
            f.executor.shutdown();
            Assertions.assertTrue(f.executor.awaitTermination(10, TimeUnit.SECONDS));
            Assertions.assertEquals(2, f.listings.get());
        }
    }

    @Test
    void sampledTransactionalAndDisabledScansKeepSynchronousPlanning() throws Exception {
        try (Fixture f = new Fixture(2, 1)) {
            f.variables.enableExternalTableBatchMode = false;
            Assertions.assertFalse(f.node.isBatchMode());
            f.variables.enableExternalTableBatchMode = true;
            Mockito.when(f.table.isHiveTransactionalTable()).thenReturn(true);
            Assertions.assertFalse(f.node.isBatchMode());
            Mockito.when(f.table.isHiveTransactionalTable()).thenReturn(false);
            f.node.setTableSample(new org.apache.doris.analysis.TableSample(true, 10L, 0L));
            Assertions.assertFalse(f.node.isBatchMode());
            Mockito.verifyNoInteractions(f.cache);
        }
    }

    @Test
    void millionFilePartitionEnumeratesOnlyTheFirstConfiguredProducerBatch() throws Exception {
        try (Fixture f = new Fixture(1, 1)) {
            HiveExternalMetaCache.FileCacheValue files = Mockito.mock(HiveExternalMetaCache.FileCacheValue.class);
            HiveExternalMetaCache.HiveFileStatus file = Mockito.spy(f.file(0));
            Mockito.when(files.getFiles()).thenReturn(Collections.nCopies(1000000, file));
            Mockito.when(files.isSplittable()).thenReturn(false);
            Mockito.when(f.cache.getFilesByPartitions(Mockito.anyList(), Mockito.anyBoolean(), Mockito.anyBoolean(),
                    Mockito.isNull(), Mockito.eq(f.table))).thenReturn(Collections.singletonList(files));
            Assertions.assertTrue(f.node.isBatchMode());
            SplitProducer.BatchSource source = f.node.createSplitBatchSource(f.cache, new AtomicInteger(), "", 1);
            Assertions.assertEquals(7, source.getNextBatch().size());
            Mockito.verify(file, Mockito.times(7)).getPath();
            source.close();
        }
    }

    private static void field(Object target, String name, Object value) throws Exception {
        for (Class<?> type = target.getClass(); type != null; type = type.getSuperclass()) {
            try {
                Field field = type.getDeclaredField(name);
                field.setAccessible(true);
                field.set(target, value);
                return;
            } catch (NoSuchFieldException e) {
                // This test initializes inherited scan-level environment fields, not mode-selection logic.
            }
        }
        throw new NoSuchFieldException(name);
    }

    private static class TestNode extends HiveScanNode {
        private final List<HivePartition> partitions;

        TestNode(HMSExternalTable table, SessionVariable variables, List<HivePartition> partitions) {
            super(new PlanNodeId(0), descriptor(table), false, variables, null, ScanContext.EMPTY);
            this.partitions = partitions;
            fileSplitter = new FileSplitter(8, 8, 0);
        }

        private static TupleDescriptor descriptor(HMSExternalTable table) {
            TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
            desc.setTable(table);
            return desc;
        }

        @Override
        protected List<HivePartition> getPartitions() {
            return partitions;
        }

        void initializeAssignment() {
            splitAssignment = createSplitAssignment(Collections.emptyMap(), Collections.emptyList(), true);
        }

        @Override
        protected TScanRangeLocations splitToScanRange(Backend backend, java.util.Map<String, String> properties,
                Split split, List<String> keys, boolean admission) {
            FileSplit file = (FileSplit) split;
            int partition = Integer.parseInt(file.getPathString().substring(file.getPathString().lastIndexOf('/') + 1));
            return new TScanRangeLocations().setLocations(Collections.singletonList(
                    new TScanRangeLocation().setBackendId(partition * 100L + split.getStart())));
        }
    }

    private static class Fixture implements AutoCloseable {
        final SessionVariable variables = new SessionVariable();
        final HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        final HiveExternalMetaCache cache = Mockito.mock(HiveExternalMetaCache.class);
        final Backend backend = new Backend(1, "localhost", 0);
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        final CountDownLatch entered = new CountDownLatch(2);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger listings = new AtomicInteger();
        final AtomicInteger active = new AtomicInteger();
        final AtomicInteger peak = new AtomicInteger();
        final int previousParallelism = Config.max_external_cache_loader_thread_pool_size;
        final ConnectContext previousContext = ConnectContext.get();
        final ConnectContext context = new ConnectContext();
        final StatementContext statement = new StatementContext(context, null);
        final MockedStatic<org.apache.doris.catalog.Env> mockedEnv;
        final TestNode node;

        Fixture(int partitionCount, int partitionThreshold) throws Exception {
            Config.max_external_cache_loader_thread_pool_size = 2;
            variables.numPartitionsInBatchMode = partitionThreshold;
            variables.splitBatchSize = 7;
            variables.splitQueueCapacity = 1;
            variables.enableFileScannerV2 = false;
            variables.setFileSplitSize(1);
            context.setSessionVariable(variables);
            context.setStatementContext(statement);
            context.setThreadLocalInfo();
            HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
            Mockito.when(table.getCatalog()).thenReturn(catalog);
            Mockito.when(catalog.bindBrokerName()).thenReturn("");
            Mockito.when(table.getFileFormatType(Mockito.any())).thenReturn(TFileFormatType.FORMAT_PARQUET);
            org.apache.doris.catalog.Env env = Mockito.mock(org.apache.doris.catalog.Env.class);
            ExternalMetaCacheMgr mgr = Mockito.mock(ExternalMetaCacheMgr.class);
            Mockito.when(env.getExtMetaCacheMgr()).thenReturn(mgr);
            Mockito.when(env.getSplitSourceManager()).thenReturn(Mockito.mock(SplitSourceManager.class));
            Mockito.when(mgr.hive(Mockito.anyLong())).thenReturn(cache);
            Mockito.when(mgr.getScheduleExecutor()).thenReturn(executor);
            mockedEnv = Mockito.mockStatic(org.apache.doris.catalog.Env.class);
            mockedEnv.when(org.apache.doris.catalog.Env::getCurrentEnv).thenReturn(env);
            List<HivePartition> partitions = new ArrayList<>();
            for (int p = 0; p < partitionCount; p++) {
                partitions.add(new HivePartition(null, false, "parquet", "hdfs://test/" + p,
                        Collections.singletonList(Integer.toString(p)), Collections.emptyMap()));
            }
            node = new TestNode(table, variables, partitions);
            FederationBackendPolicy policy = Mockito.mock(FederationBackendPolicy.class);
            Mockito.when(policy.computeScanRangeAssignment(Mockito.anyList())).thenAnswer(call -> {
                Multimap<Backend, Split> assigned = LinkedListMultimap.create();
                for (Split split : call.<List<Split>>getArgument(0)) {
                    assigned.put(backend, split);
                }
                return assigned;
            });
            field(node, "backendPolicy", policy);
            node.initializeAssignment();
            Mockito.when(cache.getFilesByPartitions(Mockito.anyList(), Mockito.anyBoolean(), Mockito.anyBoolean(),
                    Mockito.isNull(), Mockito.eq(table))).thenAnswer(call -> {
                        int count = active.incrementAndGet();
                        peak.accumulateAndGet(count, Math::max);
                        listings.incrementAndGet();
                        entered.countDown();
                        try {
                            Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
                            HivePartition p = call.<List<HivePartition>>getArgument(0).get(0);
                            int number = Integer.parseInt(p.getPath().substring(p.getPath().lastIndexOf('/') + 1));
                            HiveExternalMetaCache.FileCacheValue value = new HiveExternalMetaCache.FileCacheValue();
                            value.setSplittable(true);
                            value.setPartitionValues(p.getPartitionValues());
                            value.getFiles().add(file(number));
                            return Collections.singletonList(value);
                        } finally {
                            active.decrementAndGet();
                        }
                    });
        }

        HiveExternalMetaCache.HiveFileStatus file(int partition) {
            HiveExternalMetaCache.HiveFileStatus status = new HiveExternalMetaCache.HiveFileStatus();
            status.setPath(LocationPath.of("hdfs://test/" + partition));
            status.setLength(17);
            return status;
        }

        List<Long> drain() throws Exception {
            SplitAssignment assignment = (SplitAssignment) getAssignment();
            SplitSource source = new SplitSource(backend, assignment, 10000);
            List<Long> identities = new ArrayList<>();
            while (true) {
                List<TScanRangeLocations> batch = source.getNextBatch(3);
                Assertions.assertTrue(batch.size() <= 3);
                if (batch.isEmpty()) {
                    return identities;
                }
                for (TScanRangeLocations range : batch) {
                    identities.add(range.getLocations().get(0).getBackendId());
                }
            }
        }

        Object getAssignment() throws Exception {
            for (Class<?> type = node.getClass(); type != null; type = type.getSuperclass()) {
                try {
                    Field field = type.getDeclaredField("splitAssignment");
                    field.setAccessible(true);
                    return field.get(node);
                } catch (NoSuchFieldException e) {
                    // Continue to the owning superclass.
                }
            }
            throw new NoSuchFieldException("splitAssignment");
        }

        @Override
        public void close() throws Exception {
            node.stop();
            release.countDown();
            executor.shutdown();
            Assertions.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
            try {
                statement.close();
            } finally {
                mockedEnv.close();
                Config.max_external_cache_loader_thread_pool_size = previousParallelism;
                ConnectContext.remove();
                if (previousContext != null) {
                    previousContext.setThreadLocalInfo();
                }
            }
        }
    }
}
