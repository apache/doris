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

package org.apache.doris.datasource.scan;

import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.connector.hive.HiveCatalogProperties;
import org.apache.doris.connector.hive.HiveFileListingCache;
import org.apache.doris.connector.hive.HiveReadTransactionManager;
import org.apache.doris.connector.hive.HiveScanPlanProvider;
import org.apache.doris.connector.hive.HiveScanRange;
import org.apache.doris.connector.hive.HiveTableHandle;
import org.apache.doris.connector.hive.HiveTableType;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.ConnectorStorageContext;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.split.PluginDrivenSplit;
import org.apache.doris.datasource.split.SplitAssignment;
import org.apache.doris.datasource.split.SplitGenerator;
import org.apache.doris.datasource.split.SplitSource;
import org.apache.doris.datasource.split.SplitSourceManager;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileIterator;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.Location;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TPushAggOp;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class PluginDrivenScanNodeHiveStreamingTest {
    @Test
    @Timeout(30)
    void hiveModeSelectionPreservesBoundedPartitionConcurrencyAndUniqueRanges() throws Exception {
        Set<Integer> emptyPartitions = new HashSet<>(Arrays.asList(0, 4, 7));
        try (Fixture fixture = new Fixture(emptyPartitions)) {
            fixture.start();
            Assertions.assertTrue(fixture.entered.await(10, TimeUnit.SECONDS),
                    "Two partition batches must reach cold listing concurrently through startSplit");
            Assertions.assertEquals(2, fixture.listings.get());
            fixture.release.countDown();

            List<Long> actual = fixture.drain();
            Set<Long> expected = new HashSet<>();
            for (int partition = 0; partition < 9; partition++) {
                if (!emptyPartitions.contains(partition)) {
                    for (int offset = 0; offset < 17; offset++) {
                        expected.add(partition * 100L + offset);
                    }
                }
            }
            Assertions.assertEquals(expected.size(), actual.size(), "Every range is emitted once");
            Assertions.assertEquals(expected, new HashSet<>(actual));
            Assertions.assertEquals(9, fixture.listings.get(), "No partition batch is omitted or loaded twice");
            Assertions.assertEquals(2, fixture.peakListings.get(), "The producer count bounds remote listing");
        }
    }

    @Test
    @Timeout(30)
    void cancellationStopsHivePartitionBatchesBeforeFurtherListing() throws Exception {
        try (Fixture fixture = new Fixture(Collections.emptySet())) {
            fixture.start();
            Assertions.assertTrue(fixture.entered.await(10, TimeUnit.SECONDS));
            fixture.assignment.stop();
            fixture.release.countDown();
            fixture.executor.shutdown();
            Assertions.assertTrue(fixture.executor.awaitTermination(10, TimeUnit.SECONDS));
            Assertions.assertEquals(2, fixture.listings.get());
        }
    }

    private static class Fixture implements AutoCloseable {
        private static final AtomicInteger NEXT_QUERY = new AtomicInteger();
        final CountDownLatch entered = new CountDownLatch(2);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger listings = new AtomicInteger();
        final AtomicInteger activeListings = new AtomicInteger();
        final AtomicInteger peakListings = new AtomicInteger();
        final ExecutorService executor = Executors.newFixedThreadPool(4);
        final Backend backend = new Backend(0, "127.0.0.1", 9050);
        final TUniqueId queryId = new TUniqueId(69133, NEXT_QUERY.incrementAndGet());
        final int previousParallelism = Config.max_external_cache_loader_thread_pool_size;
        final MockedStatic<Env> envStatic;
        final PluginDrivenScanNode node;
        final SplitAssignment assignment;

        Fixture(Set<Integer> emptyPartitions) throws Exception {
            FileSystem fs = Mockito.mock(FileSystem.class);
            Mockito.when(fs.forLocation(Mockito.any())).thenReturn(fs);
            Mockito.when(fs.list(Mockito.any())).thenAnswer(call -> {
                Location location = call.getArgument(0);
                listings.incrementAndGet();
                peakListings.accumulateAndGet(activeListings.incrementAndGet(), Math::max);
                entered.countDown();
                try {
                    if (!release.await(20, TimeUnit.SECONDS)) {
                        throw new IOException("Timed out waiting for concurrent directory listing");
                    }
                } finally {
                    activeListings.decrementAndGet();
                }
                int partition = Integer.parseInt(location.uri().substring(location.uri().lastIndexOf('=') + 1));
                List<FileEntry> files = emptyPartitions.contains(partition) ? Collections.emptyList()
                        : Collections.singletonList(new FileEntry(Location.of(location.uri() + "/data.parquet"),
                                17, false, 1, Collections.emptyList()));
                Iterator<FileEntry> entries = files.iterator();
                return new FileIterator() {
                    @Override
                    public boolean hasNext() {
                        return entries.hasNext();
                    }

                    @Override
                    public FileEntry next() {
                        return entries.next();
                    }

                    @Override
                    public void close() {
                    }
                };
            });
            ConnectorStorageContext storage = Mockito.mock(ConnectorStorageContext.class, Mockito.CALLS_REAL_METHODS);
            Mockito.when(storage.getFileSystem(Mockito.any())).thenReturn(fs);
            ConnectorContext context = Mockito.mock(ConnectorContext.class);
            Mockito.when(context.getStorageContext()).thenReturn(storage);
            HmsClient hms = Mockito.mock(HmsClient.class, Mockito.CALLS_REAL_METHODS);
            List<String> partitionNames = new ArrayList<>();
            for (int partition = 0; partition < 9; partition++) {
                partitionNames.add("pt=" + partition);
            }
            Mockito.when(hms.listPartitionNames(Mockito.anyString(), Mockito.anyString(), Mockito.anyInt()))
                    .thenReturn(partitionNames);
            Mockito.when(hms.getPartitions(Mockito.anyString(), Mockito.anyString(), Mockito.anyList()))
                    .thenAnswer(call -> {
                        List<HmsPartitionInfo> partitions = new ArrayList<>();
                        for (String name : call.<List<String>>getArgument(2)) {
                            partitions.add(new HmsPartitionInfo(Collections.singletonList(name.substring(3)),
                                    "s3://bucket/" + name, null, null, null, Collections.emptyMap()));
                        }
                        return partitions;
                    });
            HiveCatalogProperties properties = HiveCatalogProperties.of(Collections.singletonMap(
                    HiveCatalogProperties.HIVE_METASTORE_URIS, "thrift://localhost:9083"));
            HiveScanPlanProvider provider = new HiveScanPlanProvider(hms, properties, context,
                    new HiveReadTransactionManager(), new HiveFileListingCache(properties));
            HiveTableHandle handle = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                    .partitionKeyNames(Collections.singletonList("pt")).location("s3://bucket/")
                    .inputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat")
                    .serializationLib("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe").build();
            ConnectorSession session = Mockito.mock(ConnectorSession.class, Mockito.CALLS_REAL_METHODS);
            Mockito.when(session.getQueryId()).thenReturn(DebugUtil.printId(queryId));
            Mockito.when(session.getStatementScope()).thenReturn(ConnectorStatementScope.NONE);
            Mockito.when(session.getProperty("file_split_size", String.class)).thenReturn("1");
            Mockito.when(session.getProperty("num_partitions_in_batch_mode", Integer.class)).thenReturn(2);
            // The old implementation chooses its single-source streaming path for this same Hive scan.
            Assertions.assertTrue(provider.streamingSplitEstimate(session, handle, Optional.empty(), false) >= 0);

            node = Mockito.mock(PluginDrivenScanNode.class, Mockito.CALLS_REAL_METHODS);
            Connector connector = Mockito.mock(Connector.class);
            Mockito.when(connector.getScanPlanProvider(Mockito.any())).thenReturn(provider);
            TupleDescriptor desc = Mockito.mock(TupleDescriptor.class);
            Mockito.when(desc.getSlots()).thenReturn(new ArrayList<>(Collections.singletonList(
                    Mockito.mock(SlotDescriptor.class))));
            SessionVariable variables = Mockito.mock(SessionVariable.class);
            Mockito.when(variables.getNumPartitionsInBatchMode()).thenReturn(2);
            Mockito.when(variables.getSplitBatchSize()).thenReturn(2);
            Mockito.when(variables.getSplitQueueCapacity()).thenReturn(1);
            Deencapsulation.setField(node, "connector", connector);
            Deencapsulation.setField(node, "connectorSession", session);
            Deencapsulation.setField(node, "currentHandle", handle);
            Deencapsulation.setField(node, "cachedMetadata", Mockito.mock(ConnectorMetadata.class));
            Deencapsulation.setField(node, "sessionVariable", variables);
            Deencapsulation.setField(node, "desc", desc);
            Mockito.doReturn(Mockito.mock(TableIf.class)).when(node).getTargetTable();
            Mockito.doReturn(TPushAggOp.NONE).when(node).getPushDownAggNoGroupingOp();
            Map<String, PartitionItem> selected = new LinkedHashMap<>();
            for (int partition = 0; partition < 9; partition++) {
                selected.put("pt=" + partition, Mockito.mock(PartitionItem.class));
            }
            node.setSelectedPartitions(new SelectedPartitions(9, selected, false));
            FederationBackendPolicy policy = Mockito.mock(FederationBackendPolicy.class);
            Mockito.when(policy.computeScanRangeAssignment(Mockito.any())).thenAnswer(call -> {
                Multimap<Backend, Split> assigned = ArrayListMultimap.create();
                assigned.putAll(backend, call.<List<Split>>getArgument(0));
                return assigned;
            });
            assignment = new SplitAssignment(policy, Mockito.mock(SplitGenerator.class),
                    (be, props, split, keys, admission) -> {
                        HiveScanRange range = (HiveScanRange) ((PluginDrivenSplit) split).getConnectorScanRange();
                        long identity = Long.parseLong(range.getPartitionValues().get("pt")) * 100 + range.getStart();
                        return new TScanRangeLocations().setLocations(Collections.singletonList(
                                new TScanRangeLocation().setBackendId(identity)));
                    }, Collections.emptyMap(), Collections.emptyList(), true, 2, 1);
            Deencapsulation.setField(node, "splitAssignment", assignment);
            ExternalMetaCacheMgr manager = Mockito.mock(ExternalMetaCacheMgr.class);
            Mockito.when(manager.getScheduleExecutor()).thenReturn(executor);
            Env env = Mockito.mock(Env.class);
            Mockito.when(env.getExtMetaCacheMgr()).thenReturn(manager);
            Mockito.when(env.getSplitSourceManager()).thenReturn(Mockito.mock(SplitSourceManager.class));
            envStatic = Mockito.mockStatic(Env.class);
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Config.max_external_cache_loader_thread_pool_size = 2;
        }

        void start() {
            Assertions.assertTrue(node.isBatchMode());
            node.startSplit(1);
        }

        List<Long> drain() throws Exception {
            SplitSource consumer = new SplitSource(backend, assignment, 1000);
            List<Long> actual = new ArrayList<>();
            for (int request = 0; request < 1000; request++) {
                List<TScanRangeLocations> batch = consumer.getNextBatch(3);
                Assertions.assertTrue(batch.size() <= 3);
                if (batch.isEmpty()) {
                    return actual;
                }
                for (TScanRangeLocations range : batch) {
                    actual.add(range.getLocations().get(0).getBackendId());
                }
            }
            throw new AssertionError("Hive streaming scan did not finish");
        }

        @Override
        public void close() throws Exception {
            try {
                assignment.stop();
                release.countDown();
                QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
                executor.shutdown();
                Assertions.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
            } finally {
                executor.shutdownNow();
                envStatic.close();
                Config.max_external_cache_loader_thread_pool_size = previousParallelism;
            }
        }
    }
}
