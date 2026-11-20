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
import org.apache.doris.backup.Status;
import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.FileSplitter;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.SplitProducer;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveExternalMetaCache;
import org.apache.doris.datasource.hive.HivePartition;
import org.apache.doris.fs.FileSystemCache;
import org.apache.doris.fs.FileSystemDirectoryLister;
import org.apache.doris.fs.RemoteIterator;
import org.apache.doris.fs.remote.RemoteFile;
import org.apache.doris.fs.remote.RemoteFileSystem;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileScanRangeParams;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

class HiveStreamingListingIntegrationTest {
    @Test
    void retiredEntryRefreshCannotFillItsReplacementGroup() throws Exception {
        try (Fixture f = new Fixture(3000, 128, 0, 128)) {
            org.apache.doris.datasource.metacache.MetaCacheEntry<HiveExternalMetaCache.FileCacheKey,
                    HiveExternalMetaCache.FileCacheValue> retired = f.cache.entry(0, HiveExternalMetaCache.ENTRY_FILE,
                            HiveExternalMetaCache.FileCacheKey.class, HiveExternalMetaCache.FileCacheValue.class);
            HiveExternalMetaCache.FileCacheKey key = HiveExternalMetaCache.FileCacheKey.createDummyCacheKey(
                    0, org.apache.doris.common.util.Util.genIdByName("db", "files"),
                    "hdfs://test/data", "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
            Assertions.assertEquals(3000, retired.get(key).getFiles().size());
            Assertions.assertEquals(1, retired.stats().getLoadSuccessCount(),
                    "Managed loads must not count their inner streaming publication twice");
            int opens = f.opens.get();
            f.cache.invalidateCatalog(0);
            f.cache.initCatalog(0, Collections.emptyMap());
            // MetaCacheEntry.get on a retired owner invokes its loader, as a late background load can.
            Assertions.assertNull(retired.get(key));
            Assertions.assertEquals(opens, f.opens.get());
            Assertions.assertEquals(-1, f.cache.getCachedFileCount(f.partitions));
            Assertions.assertEquals(-1, f.cache.getCachedFileSize(f.partitions));
        }
    }

    @Test
    void singlePartitionProducesFirstBatchBeforeListingCompletesThenCachesAllFiles() throws Exception {
        try (Fixture f = new Fixture(3000, 128, 0, 128)) {
            try (SplitProducer.BatchSource source = f.source()) {
                List<Split> first = source.getNextBatch();
                Assertions.assertEquals(128, first.size());
                Assertions.assertEquals(128, f.consumed.get());
                Assertions.assertEquals(-1, f.cache.getCachedFileSize(f.partitions));
                Set<String> files = new HashSet<>();
                for (Split split : first) {
                    files.add(((org.apache.doris.datasource.FileSplit) split).getPathString());
                }
                List<Split> batch;
                while (!(batch = source.getNextBatch()).isEmpty()) {
                    for (Split split : batch) {
                        Assertions.assertTrue(files.add(((org.apache.doris.datasource.FileSplit) split).getPathString()));
                    }
                }
                Assertions.assertEquals(3000, files.size());
                Assertions.assertEquals(30000, f.cache.getCachedFileSize(f.partitions));
            }
            int opens = f.opens.get();
            try (SplitProducer.BatchSource source = f.source()) {
                int count = 0;
                List<Split> batch;
                while (!(batch = source.getNextBatch()).isEmpty()) {
                    count += batch.size();
                }
                Assertions.assertEquals(3000, count);
            }
            Assertions.assertEquals(opens, f.opens.get(), "Warm scan cannot re-list the partition");
        }
    }

    @Test
    void abandoningProducerDoesNotPublishPrefixAndRefreshClearsCompleteSummary() throws Exception {
        try (Fixture f = new Fixture(3000, 128, 0, 128)) {
            try (SplitProducer.BatchSource source = f.source()) {
                source.getNextBatch();
            }
            Assertions.assertEquals(0, f.cache.entry(0, HiveExternalMetaCache.ENTRY_FILE,
                    HiveExternalMetaCache.FileCacheKey.class, HiveExternalMetaCache.FileCacheValue.class)
                    .stats().getLoadSuccessCount(), "Early close is not a completed load");
            Assertions.assertEquals(-1, f.cache.getCachedFileSize(f.partitions));
            Assertions.assertEquals(-1, f.cache.getCachedFileCount(f.partitions));
            try (SplitProducer.BatchSource source = f.source()) {
                while (!source.getNextBatch().isEmpty()) {
                    // Complete a fresh listing.
                }
            }
            Assertions.assertEquals(30000, f.cache.getCachedFileSize(f.partitions));
            f.cache.invalidateTable(0, "db", "files");
            Assertions.assertEquals(-1, f.cache.getCachedFileSize(f.partitions));
        }
    }

    // Shared by the task-local benchmark, which also runs this fixture against the pristine 4.2 jar.
    static class Fixture implements AutoCloseable {
        final AtomicInteger consumed = new AtomicInteger();
        final AtomicInteger opens = new AtomicInteger();
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final HiveExternalMetaCache cache = new HiveExternalMetaCache(executor, executor);
        final List<HivePartition> partitions = Collections.singletonList(new HivePartition(
                NameMapping.createForTest("db", "files"), true,
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat", "hdfs://test/data",
                null, Collections.emptyMap()));
        final ConnectContext previous = ConnectContext.get();
        final MockedStatic<Env> mockedEnv;
        final Node node;

        Fixture(int fileCount, int pageSize, long pageNanos, int pathLength) throws Exception {
            SessionVariable variables = new SessionVariable();
            // Exercise the real cold file-count gate for a single partition, as ordinary queries do.
            variables.numPartitionsInBatchMode = 1024;
            variables.numFilesInBatchMode = 1024;
            variables.splitBatchSize = 128;
            variables.splitQueueCapacity = 1;
            variables.enableFileScannerV2 = false;
            variables.setFileSplitSize(1024);
            ConnectContext context = new ConnectContext();
            context.setSessionVariable(variables);
            context.setStatementContext(new StatementContext(context, null));
            context.setThreadLocalInfo();
            Map<String, String> properties = Collections.emptyMap();
            HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
            Mockito.when(catalog.getId()).thenReturn(0L);
            Mockito.when(catalog.bindBrokerName()).thenReturn("");
            Mockito.when(catalog.getProperties()).thenReturn(properties);
            Mockito.when(catalog.getCatalogProperty()).thenReturn(new CatalogProperty(null, properties));
            HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
            Mockito.when(table.getCatalog()).thenReturn(catalog);
            Mockito.when(table.getFileFormatType(Mockito.any())).thenReturn(TFileFormatType.FORMAT_PARQUET);
            Env env = Mockito.mock(Env.class);
            Mockito.when(env.getSqlBlockRuleMgr()).thenReturn(Mockito.mock(
                    org.apache.doris.blockrule.SqlBlockRuleMgr.class));
            CatalogMgr catalogs = Mockito.mock(CatalogMgr.class);
            Mockito.doReturn(catalog).when(catalogs).getCatalog(0L);
            Mockito.when(env.getCatalogMgr()).thenReturn(catalogs);
            ExternalMetaCacheMgr manager = Mockito.mock(ExternalMetaCacheMgr.class);
            Mockito.when(env.getExtMetaCacheMgr()).thenReturn(manager);
            Mockito.when(manager.hive(0L)).thenReturn(cache);
            FileSystemCache fsCache = Mockito.mock(FileSystemCache.class);
            Mockito.when(manager.getFsCache()).thenReturn(fsCache);
            RemoteFileSystem fs = Mockito.mock(RemoteFileSystem.class, call -> {
                String name = call.getMethod().getName();
                if (name.equals("listFilesIterator")) {
                    opens.incrementAndGet();
                    return new Files(fileCount, pageSize, pageNanos, pathLength);
                }
                if (name.equals("listFiles")) {
                    opens.incrementAndGet();
                    Files files = new Files(fileCount, pageSize, pageNanos, pathLength);
                    List<RemoteFile> result = call.getArgument(2);
                    while (files.hasNext()) {
                        result.add(files.next());
                    }
                    return Status.OK;
                }
                return Mockito.RETURNS_DEFAULTS.answer(call);
            });
            Mockito.when(fsCache.getRemoteFileSystem(Mockito.any())).thenReturn(fs);
            mockedEnv = Mockito.mockStatic(Env.class);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            cache.initCatalog(0, properties);
            node = new Node(table, variables, partitions);
            Assertions.assertTrue(node.isBatchMode());
        }

        SplitProducer.BatchSource source() {
            return node.createSplitBatchSource(cache, new AtomicInteger(), "", 1);
        }

        class Files implements RemoteIterator<RemoteFile> {
            final int count;
            final int pageSize;
            final long pageNanos;
            final String base;
            int index;
            int lastPage = -1;

            Files(int count, int pageSize, long pageNanos, int pathLength) {
                this.count = count;
                this.pageSize = pageSize;
                this.pageNanos = pageNanos;
                base = "hdfs://test/data/" + com.google.common.base.Strings.repeat("x", Math.max(1, pathLength - 30));
            }

            @Override
            public boolean hasNext() {
                if (index < count && index / pageSize != lastPage) {
                    lastPage = index / pageSize;
                    if (pageNanos > 0) {
                        LockSupport.parkNanos(pageNanos);
                    }
                }
                return index < count;
            }

            @Override
            public RemoteFile next() {
                consumed.incrementAndGet();
                return new RemoteFile(new Path(base + index++), false, 10, 128, 99, null);
            }
        }

        @Override
        public void close() {
            cache.invalidateCatalog(0);
            executor.shutdownNow();
            mockedEnv.close();
            if (previous == null) {
                ConnectContext.remove();
            } else {
                previous.setThreadLocalInfo();
            }
        }
    }

    static class Node extends HiveScanNode {
        final List<HivePartition> partitions;

        Node(HMSExternalTable table, SessionVariable variables, List<HivePartition> partitions) {
            super(new PlanNodeId(0), descriptor(table), false, variables,
                    new FileSystemDirectoryLister(), ScanContext.EMPTY);
            this.partitions = partitions;
            fileSplitter = new FileSplitter(1024, 1024, 0);
            params = new TFileScanRangeParams();
            splitAssignment = createSplitAssignment(Collections.emptyMap(), Collections.emptyList(), true);
        }

        static TupleDescriptor descriptor(HMSExternalTable table) {
            TupleDescriptor descriptor = new TupleDescriptor(new TupleId(0));
            descriptor.setTable(table);
            return descriptor;
        }

        @Override
        protected List<HivePartition> getPartitions() {
            return partitions;
        }
    }
}
