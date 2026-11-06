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

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.UserException;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.FederationBackendPolicy;
import org.apache.doris.datasource.FileSplit;
import org.apache.doris.datasource.FileSplitter;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.SplitAssignment;
import org.apache.doris.datasource.SplitSource;
import org.apache.doris.datasource.hive.HMSCachedClient;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveExternalMetaCache;
import org.apache.doris.datasource.hive.HiveExternalMetaCache.FileCacheValue;
import org.apache.doris.datasource.hive.HiveExternalMetaCache.HiveFileStatus;
import org.apache.doris.datasource.hive.HivePartition;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

class HivePartitionStreamingTest {
    @Test
    void choosingBatchModeDoesNotLoadAllPartitionObjects() throws Exception {
        try (Fixture fixture = new Fixture(1000, false, false)) {
            Assertions.assertTrue(fixture.node.isBatchMode());
            Assertions.assertEquals(1000, fixture.node.getSelectedPartitionNum());
            Assertions.assertTrue(fixture.node.numApproximateSplits() >= 1000);
            Mockito.verifyNoInteractions(fixture.cache);
            Mockito.verify(fixture.cache, Mockito.never()).getAllPartitionsWithCache(Mockito.any(), Mockito.anyList());
        }
    }

    @Test
    void oneWorkerCanRunBothMetadataAndSplitTasksWithoutWaitingForChildren() throws Exception {
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try (Fixture fixture = new Fixture(1000, false, false, worker)) {
            fixture.start();
            List<Long> identities = new ArrayList<>();
            while (!fixture.consumer.isLastBatch()) {
                for (TScanRangeLocations range : fixture.consumer.getNextBatch(128)) {
                    identities.add(range.getLocations().get(0).getBackendId());
                }
            }
            Assertions.assertEquals(1000, identities.size());
            Assertions.assertEquals(1000, new HashSet<>(identities).size());
            Assertions.assertEquals(ImmutableList.of(16, 984), fixture.requests);
        } finally {
            worker.shutdownNow();
            Assertions.assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    void firstSplitsArriveBeforeTheRemainingMetadataRequest() throws Exception {
        try (Fixture fixture = new Fixture(1000, false, false)) {
            fixture.start();
            fixture.splits.runAll();
            Assertions.assertEquals(1, fixture.metadata.pendingTasks());
            fixture.metadata.runNext();
            fixture.splits.runAll();
            Assertions.assertEquals(ImmutableList.of(16), fixture.requests);
            Assertions.assertFalse(fixture.consumer.getNextBatch(1).isEmpty());
            Assertions.assertEquals(ImmutableList.of(16), fixture.requests,
                    "first split must not depend on the next large metadata load");
            Assertions.assertEquals(1, fixture.metadata.pendingTasks());
        }
    }

    @Test
    void fullScanAndCountPolicyVisitEveryPartitionExactlyOnce() throws Exception {
        for (boolean count : new boolean[] {false, true}) {
            for (boolean warm : new boolean[] {false, true}) {
                try (Fixture fixture = new Fixture(1000, warm, count)) {
                    fixture.start();
                    List<Long> identities = fixture.drain();
                    Assertions.assertEquals(1000, identities.size());
                    Assertions.assertEquals(1000, new HashSet<>(identities).size());
                    Assertions.assertEquals(1000, fixture.listed.size());
                    Assertions.assertTrue(fixture.listed.values().stream().allMatch(visits -> visits == 1));
                    Assertions.assertEquals(warm ? ImmutableList.of() : ImmutableList.of(16, 984), fixture.requests);
                }
            }
        }
    }

    @Test
    void earlyStopCancelsQueuedMetadataAndDirectoryWork() throws Exception {
        try (Fixture fixture = new Fixture(1000, false, false)) {
            fixture.start();
            fixture.splits.runAll();
            fixture.metadata.runNext();
            fixture.splits.runAll();
            Assertions.assertFalse(fixture.consumer.getNextBatch(10).isEmpty());
            int listed = fixture.listed.size();
            fixture.assignment.stop();
            fixture.metadata.runAll();
            fixture.splits.runAll();
            Assertions.assertEquals(ImmutableList.of(16), fixture.requests);
            Assertions.assertEquals(listed, fixture.listed.size());
            Assertions.assertTrue(fixture.assignment.isStop());
        }
    }

    @Test
    void remoteFailureWakesSplitReadersAndPreservesRootCause() throws Exception {
        try (Fixture fixture = new Fixture(1000, false, false)) {
            Mockito.when(fixture.cache.getExistingPartitionsWithCache(Mockito.any(), Mockito.anyList()))
                    .thenThrow(new IllegalStateException("HMS request failed"));
            fixture.start();
            fixture.splits.runAll();
            fixture.metadata.runAll();
            fixture.splits.runAll();
            UserException failure = Assertions.assertThrows(UserException.class,
                    () -> fixture.consumer.getNextBatch(10));
            Assertions.assertTrue(failure.getMessage().contains("HMS request failed"));
            Assertions.assertTrue(failure.getMessage().contains("streaming_table"));
            Assertions.assertTrue(fixture.listed.isEmpty());
        }
    }

    private static class ManualExecutor implements Executor {
        private final Queue<Runnable> tasks = new ArrayDeque<>();

        @Override
        public void execute(Runnable task) {
            tasks.add(task);
        }

        void runNext() {
            tasks.remove().run();
        }

        long pendingTasks() {
            return tasks.stream().filter(task -> !(task instanceof Future<?>) || !((Future<?>) task).isDone()).count();
        }

        void runAll() {
            int runs = 0;
            while (!tasks.isEmpty() && runs++ < 10000) {
                runNext();
            }
            Assertions.assertTrue(tasks.isEmpty(), "blocked producers must release the executor");
        }
    }

    private static class TestNode extends HiveScanNode {
        private final Executor splits;
        private final Executor metadata;
        private final boolean count;

        TestNode(TupleDescriptor descriptor, SessionVariable session, Executor splits,
                Executor metadata, boolean count) {
            super(new PlanNodeId(0), descriptor, false, session, null, ScanContext.EMPTY);
            this.splits = splits;
            this.metadata = metadata;
            this.count = count;
            fileSplitter = new FileSplitter(128, 128, 0);
        }

        @Override
        protected boolean isBatchModeEnabled() {
            return true;
        }

        @Override
        protected boolean isTableLevelCountStarPushdown() {
            return count;
        }

        @Override
        protected Executor getSplitTaskExecutor() {
            return splits;
        }

        @Override
        protected Executor getMetadataTaskExecutor() {
            return metadata;
        }

        @Override
        protected SummaryProfile getSummaryProfile() {
            return null;
        }

        void attach(SplitAssignment assignment) {
            splitAssignment = assignment;
        }
    }

    private static class Fixture implements AutoCloseable {
        final ManualExecutor splits = new ManualExecutor();
        final ManualExecutor metadata = new ManualExecutor();
        final HiveExternalMetaCache cache = Mockito.mock(HiveExternalMetaCache.class);
        final HMSCachedClient client = Mockito.mock(HMSCachedClient.class);
        final Backend backend = new Backend(1, "backend", 9050);
        final List<Integer> requests = new ArrayList<>();
        final Map<String, Integer> listed = new HashMap<>();
        final TestNode node;
        final SplitAssignment assignment;
        final SplitSource consumer;
        final Env env = Mockito.mock(Env.class);
        private MockedStatic<Env> environment;

        Fixture(int count, boolean warm, boolean countPolicy) throws Exception {
            this(count, warm, countPolicy, null);
        }

        Fixture(int count, boolean warm, boolean countPolicy, Executor worker) throws Exception {
            HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
            HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
            NameMapping mapping = NameMapping.createForTest("db", "streaming_table");
            Mockito.when(table.getName()).thenReturn("streaming_table");
            Mockito.when(table.getCatalog()).thenReturn(catalog);
            Mockito.when(table.getOrBuildNameMapping()).thenReturn(mapping);
            Mockito.when(table.getPartitionColumnTypes(Mockito.any())).thenReturn(ImmutableList.of(Type.INT));
            Mockito.when(catalog.getClient()).thenReturn(client);
            Mockito.when(client.getPartitionBatchSize()).thenReturn(1024);
            Mockito.when(catalog.bindBrokerName()).thenReturn("");
            TupleDescriptor descriptor = new TupleDescriptor(new TupleId(0));
            descriptor.setTable(table);
            SessionVariable session = new SessionVariable();
            session.parallelPipelineTaskNum = 1;
            node = new TestNode(descriptor, session,
                    worker == null ? splits : worker, worker == null ? metadata : worker, countPolicy);
            Map<String, PartitionItem> items = new LinkedHashMap<>();
            for (int i = 0; i < count; i++) {
                PartitionKey key = PartitionKey.createListPartitionKeyWithTypes(
                        ImmutableList.of(new PartitionValue(Integer.toString(i))), ImmutableList.of(Type.INT), true);
                items.put("p=" + i, new ListPartitionItem(ImmutableList.of(key)));
            }
            node.setSelectedPartitions(new SelectedPartitions(count, items, true, false));
            Mockito.when(cache.getCachedPartitions(Mockito.same(mapping), Mockito.anyList())).thenAnswer(call ->
                    warm ? Optional.of(partitions(mapping, call.getArgument(1))) : Optional.empty());
            Mockito.when(cache.getExistingPartitionsWithCache(Mockito.same(mapping), Mockito.anyList()))
                    .thenAnswer(call -> {
                        List<List<String>> values = call.getArgument(1);
                        requests.add(values.size());
                        return partitions(mapping, values);
                    });
            Mockito.when(cache.getFilesByPartitions(Mockito.anyList(), Mockito.anyBoolean(), Mockito.anyBoolean(),
                    Mockito.isNull(), Mockito.same(table))).thenAnswer(call -> {
                        List<FileCacheValue> files = new ArrayList<>();
                        for (HivePartition partition : call.<List<HivePartition>>getArgument(0)) {
                            String identity = partition.getPartitionValues().get(0);
                            listed.merge(identity, 1, Integer::sum);
                            FileCacheValue value = new FileCacheValue();
                            value.setPartitionValues(partition.getPartitionValues());
                            HiveFileStatus file = new HiveFileStatus();
                            file.setPath(LocationPath.of(partition.getPath() + "/data.parquet"));
                            file.setLength(1);
                            value.getFiles().add(file);
                            files.add(value);
                        }
                        return files;
                    });
            ExternalMetaCacheMgr manager = Mockito.mock(ExternalMetaCacheMgr.class);
            Mockito.when(env.getExtMetaCacheMgr()).thenReturn(manager);
            Mockito.when(manager.hive(Mockito.anyLong())).thenReturn(cache);
            FederationBackendPolicy policy = Mockito.mock(FederationBackendPolicy.class);
            Mockito.when(policy.numBackends()).thenReturn(1);
            Mockito.when(policy.computeScanRangeAssignment(Mockito.anyList())).thenAnswer(call -> {
                Multimap<Backend, Split> assigned = LinkedListMultimap.create();
                assigned.putAll(backend, call.<List<Split>>getArgument(0));
                return assigned;
            });
            assignment = new SplitAssignment(policy, node, (owner, properties, split, keys, admission) ->
                    new TScanRangeLocations().setLocations(ImmutableList.of(new TScanRangeLocation()
                            .setBackendId(Long.parseLong(((FileSplit) split).getPartitionValues().get(0))))),
                    Collections.emptyMap(), Collections.emptyList(), true, 8, 128);
            node.attach(assignment);
            consumer = new SplitSource(backend, assignment, 5000);
        }

        void start() throws Exception {
            environment = Mockito.mockStatic(Env.class);
            environment.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertTrue(node.isBatchMode());
            node.startSplit(1);
        }

        List<Long> drain() throws Exception {
            List<Long> values = new ArrayList<>();
            for (int rounds = 0; rounds < 10000 && !consumer.isLastBatch(); rounds++) {
                splits.runAll();
                metadata.runAll();
                splits.runAll();
                if (assignment.getAssignedSplits(backend) != null
                        && assignment.getAssignedSplits(backend).isEmpty() && metadata.pendingTasks() > 0) {
                    continue;
                }
                for (TScanRangeLocations range : consumer.getNextBatch(128)) {
                    values.add(range.getLocations().get(0).getBackendId());
                }
            }
            Assertions.assertTrue(consumer.isLastBatch());
            return values;
        }

        @Override
        public void close() {
            assignment.stop();
            metadata.runAll();
            splits.runAll();
            if (environment != null) {
                environment.close();
            }
        }

        private static List<HivePartition> partitions(NameMapping mapping, List<List<String>> values) {
            List<HivePartition> result = new ArrayList<>();
            for (List<String> value : values) {
                result.add(new HivePartition(mapping, false, "parquet", "s3://bucket/p=" + value.get(0),
                        value, Collections.emptyMap()));
            }
            return result;
        }
    }
}
