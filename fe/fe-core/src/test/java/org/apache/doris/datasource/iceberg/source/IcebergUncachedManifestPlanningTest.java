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

package org.apache.doris.datasource.iceberg.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.common.util.FairTaskExecutor;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.FederationBackendPolicy;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergPartitionInfo;
import org.apache.doris.datasource.iceberg.IcebergSnapshot;
import org.apache.doris.datasource.iceberg.IcebergSnapshotCacheValue;
import org.apache.doris.datasource.iceberg.IcebergTableCacheValue;
import org.apache.doris.datasource.iceberg.cache.IcebergManifestLoader;
import org.apache.doris.datasource.iceberg.cache.ManifestCacheValue;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DorisDataTableScan;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Timeout(60)
class IcebergUncachedManifestPlanningTest {
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "p", Types.StringType.get()));

    @TempDir
    Path tempDir;

    @Test
    void canBufferManifestFilesEnforcesSingleOversizedDeletedAndNullBounds() {
        ManifestFile small = manifestWithFileCounts(10, 100, 0);
        ManifestFile boundary = manifestWithFileCounts(750, 3000, 0);
        ManifestFile oversized = manifestWithFileCounts(751, 3000, 0);
        Assertions.assertFalse(IcebergScanNode.canBufferManifestFiles(Collections.singletonList(small), 8),
                "a single SDK manifest already streams without ParallelIterable polling");
        Assertions.assertTrue(IcebergScanNode.canBufferManifestFiles(Arrays.asList(small, boundary), 8));
        Assertions.assertFalse(IcebergScanNode.canBufferManifestFiles(Arrays.asList(small, oversized), 8));
        Assertions.assertTrue(IcebergScanNode.canBufferManifestFiles(
                Arrays.asList(small, manifestWithFileCounts(1, 0, 3749)), 8));
        Assertions.assertFalse(IcebergScanNode.canBufferManifestFiles(
                Arrays.asList(small, manifestWithFileCounts(1, 0, 3750)), 8),
                "deleted entries also count against the whole-manifest read bound");
        Assertions.assertFalse(IcebergScanNode.canBufferManifestFiles(
                Arrays.asList(small, manifestWithFileCounts(null, 0, 0)), 8));
        Assertions.assertFalse(IcebergScanNode.canBufferManifestFiles(
                Arrays.asList(small, manifestWithFileCounts(1, 0, null)), 8));
        Assertions.assertFalse(IcebergScanNode.canBufferManifestFiles(
                Arrays.asList(small, manifestWithFileCounts(Integer.MAX_VALUE, Integer.MAX_VALUE, 0)), 8));
    }

    @Test
    void uncachedBatchScanUsesBoundedPrefetcherAssociatesDeletesAndDropsUnrequestedStats() throws Exception {
        Map<String, String> props = Collections.singletonMap("format-version", "2");
        Table table = createTable("uncached_batch_deletes", SCHEMA, PartitionSpec.unpartitioned(), props);

        Map<Integer, ByteBuffer> bounds1 = Collections.singletonMap(
                1, Conversions.toByteBuffer(Types.IntegerType.get(), 1));
        Map<Integer, ByteBuffer> bounds2 = Collections.singletonMap(
                1, Conversions.toByteBuffer(Types.IntegerType.get(), 2));
        table.newFastAppend().appendFile(DataFiles.builder(table.spec())
                .withPath("/d/file_1.parquet").withFormat(FileFormat.PARQUET).withFileSizeInBytes(100)
                .withMetrics(new Metrics(10L, null, null, null, null, bounds1, bounds1)).build()).commit();
        table.newFastAppend().appendFile(DataFiles.builder(table.spec())
                .withPath("/d/file_2.parquet").withFormat(FileFormat.PARQUET).withFileSizeInBytes(100)
                .withMetrics(new Metrics(10L, null, null, null, null, bounds2, bounds2)).build()).commit();

        // Position delete targets file_1; equality delete targets id=2 (file_2).
        Map<Integer, ByteBuffer> posBounds = Collections.singletonMap(
                MetadataColumns.DELETE_FILE_PATH.fieldId(),
                Conversions.toByteBuffer(Types.StringType.get(), "/d/file_1.parquet"));
        DeleteFile posDelete = FileMetadata.deleteFileBuilder(table.spec()).ofPositionDeletes()
                .withPath("/d/pos_del.parquet").withFormat(FileFormat.PARQUET)
                .withReferencedDataFile("/d/file_1.parquet").withFileSizeInBytes(64)
                .withMetrics(new Metrics(1L, null, null, null, null, posBounds, posBounds)).build();
        DeleteFile eqDelete = FileMetadata.deleteFileBuilder(table.spec()).ofEqualityDeletes(1)
                .withPath("/d/eq_del.parquet").withFormat(FileFormat.PARQUET).withFileSizeInBytes(64)
                .withMetrics(new Metrics(1L, null, null, null, null, bounds2, bounds2)).build();
        table.newRowDelta().addDeletes(posDelete).addDeletes(eqDelete).commit();

        AtomicInteger submissions = new AtomicInteger();
        try (FairTaskExecutor pool = newPool(2)) {
            Executor recordingExecutor = task -> {
                submissions.incrementAndGet();
                pool.newGroup(16).execute(task);
            };
            IcebergScanNode node = createScanNode(table, true, recordingExecutor);
            TableScan scan = DorisDataTableScan.wrap(table.newScan());

            List<FileScanTask> tasks = new ArrayList<>();
            try (CloseableIterable<FileScanTask> planned = node.planFileScanTaskWithoutReuse(scan)) {
                for (FileScanTask task : planned) {
                    tasks.add(task);
                }
            }

            Assertions.assertTrue(submissions.get() > 0, "uncached batch scan must use bounded manifest executor");
            Assertions.assertEquals(2, tasks.size());
            Map<String, FileScanTask> byPath = new HashMap<>();
            for (FileScanTask task : tasks) {
                byPath.put(task.file().path().toString(), task);
            }

            FileScanTask task1 = byPath.get("/d/file_1.parquet");
            FileScanTask task2 = byPath.get("/d/file_2.parquet");
            Assertions.assertNotNull(task1);
            Assertions.assertNotNull(task2);
            Assertions.assertEquals(1, task1.deletes().size());
            Assertions.assertEquals("/d/pos_del.parquet", task1.deletes().get(0).path().toString());
            Assertions.assertEquals(1, task2.deletes().size());
            Assertions.assertEquals("/d/eq_del.parquet", task2.deletes().get(0).path().toString());

            // Unrequested column stats (including equality key bounds) must be stripped from final tasks.
            Assertions.assertNull(task1.file().lowerBounds());
            Assertions.assertNull(task2.file().lowerBounds());
            Assertions.assertNull(task2.file().upperBounds());
        }
    }

    @Test
    void uncachedSingleManifestAndNonBatchFallbackToSdkWithoutUsingManifestExecutor() throws Exception {
        Table singleManifestTable = createTable("single_manifest", SCHEMA, PartitionSpec.unpartitioned(),
                Collections.singletonMap("format-version", "2"));
        singleManifestTable.newFastAppend().appendFile(DataFiles.builder(singleManifestTable.spec())
                .withPath("/d/only.parquet").withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(100).withRecordCount(1).build()).commit();

        AtomicInteger submissions = new AtomicInteger();
        Executor recordingExecutor = task -> {
            submissions.incrementAndGet();
            task.run();
        };

        // Single manifest in batch mode -> stays on SDK streaming.
        IcebergScanNode singleNode = createScanNode(singleManifestTable, true, recordingExecutor);
        try (CloseableIterable<FileScanTask> planned = singleNode.planFileScanTaskWithoutReuse(
                DorisDataTableScan.wrap(singleManifestTable.newScan()))) {
            List<FileScanTask> tasks = new ArrayList<>();
            planned.forEach(tasks::add);
            Assertions.assertEquals(1, tasks.size());
        }
        Assertions.assertEquals(0, submissions.get());

        // Multiple manifests but non-batch mode -> stays on SDK path.
        singleManifestTable.newFastAppend().appendFile(DataFiles.builder(singleManifestTable.spec())
                .withPath("/d/second.parquet").withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(100).withRecordCount(1).build()).commit();
        IcebergScanNode nonBatchNode = createScanNode(singleManifestTable, false, recordingExecutor);
        try (CloseableIterable<FileScanTask> planned = nonBatchNode.planFileScanTaskWithoutReuse(
                DorisDataTableScan.wrap(singleManifestTable.newScan()))) {
            List<FileScanTask> tasks = new ArrayList<>();
            planned.forEach(tasks::add);
            Assertions.assertEquals(2, tasks.size());
        }
        Assertions.assertEquals(0, submissions.get());
    }

    @Test
    void uncachedProjectionDoesNotMutateCachedReadStatistics() throws Exception {
        Table table = createTable("projection_isolation", SCHEMA, PartitionSpec.unpartitioned(),
                Collections.singletonMap("format-version", "2"));
        Map<Integer, ByteBuffer> bounds = Collections.singletonMap(
                1, Conversions.toByteBuffer(Types.IntegerType.get(), 42));
        table.newFastAppend().appendFile(DataFiles.builder(table.spec())
                .withPath("/d/proj.parquet").withFormat(FileFormat.PARQUET).withFileSizeInBytes(100)
                .withMetrics(new Metrics(5L, null, null, null, null, bounds, bounds)).build()).commit();

        ManifestFile manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
        ManifestCacheValue full = IcebergManifestLoader.readForCache(manifest, table, manifest.content(), false);
        DorisDataTableScan.DataFileProjection projection = DorisDataTableScan.dataFileProjection(
                table.newScan(), Collections.emptySet(), false);
        ManifestCacheValue projected = IcebergManifestLoader.uncachedLoader(
                table, projection.manifestColumns(), null, projection::copyForManifest, () -> { }).apply(manifest);

        Assertions.assertEquals(bounds, full.getDataFiles().get(0).lowerBounds());
        Assertions.assertNull(projected.getDataFiles().get(0).lowerBounds());
        Assertions.assertEquals(full.getDataFiles().get(0).path(), projected.getDataFiles().get(0).path());
        Assertions.assertEquals(full.getDataFiles().get(0).recordCount(), projected.getDataFiles().get(0).recordCount());
    }

    @Test
    void rewrittenManifestAfterDroppingPartitionSourcePreservesDeletesInUncachedScan() throws Exception {
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("p").build();
        Table table = createTable("dropped_partition_source_rewrite", SCHEMA, spec,
                Collections.singletonMap("format-version", "2"));
        table.newFastAppend().appendFile(DataFiles.builder(spec)
                .withPath("/d/p1_a.parquet").withFormat(FileFormat.PARQUET)
                .withPartitionPath("p=alpha").withFileSizeInBytes(100).withRecordCount(2).build()).commit();
        table.newFastAppend().appendFile(DataFiles.builder(spec)
                .withPath("/d/p1_b.parquet").withFormat(FileFormat.PARQUET)
                .withPartitionPath("p=beta").withFileSizeInBytes(100).withRecordCount(2).build()).commit();

        // Add a position delete referencing the historical partition file before dropping the source column.
        Map<Integer, ByteBuffer> delBounds = Collections.singletonMap(
                MetadataColumns.DELETE_FILE_PATH.fieldId(),
                Conversions.toByteBuffer(Types.StringType.get(), "/d/p1_a.parquet"));
        DeleteFile posDelete = FileMetadata.deleteFileBuilder(spec).ofPositionDeletes()
                .withPath("/d/p1_a_del.parquet").withFormat(FileFormat.PARQUET)
                .withPartitionPath("p=alpha").withFileSizeInBytes(64)
                .withMetrics(new Metrics(1L, null, null, null, null, delBounds, delBounds)).build();
        table.newRowDelta().addDeletes(posDelete).commit();

        // Drop the partition field and its source column, then rewrite manifests so the header carries
        // the new schema alongside the historical spec.
        table.updateSpec().removeField("p").commit();
        table.updateSchema().deleteColumn("p").commit();
        table.rewriteManifests().rewriteIf(m -> true).clusterBy(f -> "all").commit();
        long rewriteSnapshotId = table.currentSnapshot().snapshotId();
        List<ManifestFile> rewrittenDataManifests = table.currentSnapshot().dataManifests(table.io());
        Assertions.assertEquals(1, rewrittenDataManifests.size());
        Assertions.assertEquals(rewriteSnapshotId, rewrittenDataManifests.get(0).snapshotId());

        // Add another data manifest so the scan has >1 data manifests and enters the bounded uncached path.
        table.newFastAppend().appendFile(DataFiles.builder(table.spec())
                .withPath("/d/unpart.parquet").withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(100).withRecordCount(3).build()).commit();

        AtomicInteger submissions = new AtomicInteger();
        try (FairTaskExecutor pool = newPool(2)) {
            Executor recordingExecutor = task -> {
                submissions.incrementAndGet();
                pool.newGroup(16).execute(task);
            };
            IcebergScanNode node = createScanNode(table, true, recordingExecutor);
            TableScan scan = DorisDataTableScan.wrap(table.newScan());
            Map<String, FileScanTask> byPath = new HashMap<>();
            try (CloseableIterable<FileScanTask> planned = node.planFileScanTaskWithoutReuse(scan)) {
                for (FileScanTask task : planned) {
                    byPath.put(task.file().path().toString(), task);
                }
            }
            Assertions.assertTrue(submissions.get() > 0);
            Assertions.assertEquals(3, byPath.size());
            Assertions.assertEquals(1, byPath.get("/d/p1_a.parquet").deletes().size());
            Assertions.assertEquals("/d/p1_a_del.parquet",
                    byPath.get("/d/p1_a.parquet").deletes().get(0).path().toString());
            Assertions.assertEquals(0, byPath.get("/d/p1_b.parquet").deletes().size());
        }
    }

    @Test
    void uncachedLoaderPropagatesCancellationImmediately() throws Exception {
        Table table = createTable("cancel_check", SCHEMA, PartitionSpec.unpartitioned(),
                Collections.singletonMap("format-version", "2"));
        table.newFastAppend().appendFile(DataFiles.builder(table.spec())
                .withPath("/d/c1.parquet").withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(100).withRecordCount(1).build()).commit();

        ManifestFile manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
        AtomicBoolean cancelled = new AtomicBoolean(true);
        Assertions.assertThrows(CancellationException.class, () -> IcebergManifestLoader.uncachedLoader(
                table, null, null, () -> {
                    if (cancelled.get()) {
                        throw new CancellationException("cancelled");
                    }
                }).apply(manifest));
    }

    @Test
    void uncachedLoaderClosesStreamWhenCancelledBetweenNewStreamAndGetLength() throws Exception {
        Table table = createTable("cancel_after_open", SCHEMA, PartitionSpec.unpartitioned(),
                Collections.singletonMap("format-version", "2"));
        table.newFastAppend().appendFile(DataFiles.builder(table.spec())
                .withPath("/d/c2.parquet").withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(100).withRecordCount(1).build()).commit();

        ManifestFile manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
        FileIO delegateIo = table.io();
        AtomicBoolean cancelled = new AtomicBoolean(false);
        AtomicInteger streamsOpened = new AtomicInteger();
        AtomicInteger streamsClosed = new AtomicInteger();
        AtomicBoolean delegateIoClosed = new AtomicBoolean(false);

        FileIO trackingIo = new FileIO() {
            @Override
            public InputFile newInputFile(String path) {
                return wrap(delegateIo.newInputFile(path));
            }

            @Override
            public InputFile newInputFile(String path, long length) {
                return wrap(delegateIo.newInputFile(path, length));
            }

            @Override
            public InputFile newInputFile(ManifestFile file) {
                return wrap(delegateIo.newInputFile(file));
            }

            private InputFile wrap(InputFile delegateInput) {
                return new InputFile() {
                    @Override
                    public long getLength() {
                        return delegateInput.getLength();
                    }

                    @Override
                    public SeekableInputStream newStream() {
                        SeekableInputStream delegateStream = delegateInput.newStream();
                        streamsOpened.incrementAndGet();
                        // Cancel immediately after the stream opens, before AvroIterable calls getLength().
                        cancelled.set(true);
                        return new SeekableInputStream() {
                            @Override
                            public long getPos() throws IOException {
                                return delegateStream.getPos();
                            }

                            @Override
                            public void seek(long newPos) throws IOException {
                                delegateStream.seek(newPos);
                            }

                            @Override
                            public int read() throws IOException {
                                return delegateStream.read();
                            }

                            @Override
                            public int read(byte[] b, int off, int len) throws IOException {
                                return delegateStream.read(b, off, len);
                            }

                            @Override
                            public void close() throws IOException {
                                streamsClosed.incrementAndGet();
                                delegateStream.close();
                            }
                        };
                    }

                    @Override
                    public String location() {
                        return delegateInput.location();
                    }

                    @Override
                    public boolean exists() {
                        return delegateInput.exists();
                    }
                };
            }

            @Override
            public OutputFile newOutputFile(String path) {
                return delegateIo.newOutputFile(path);
            }

            @Override
            public void deleteFile(String path) {
                delegateIo.deleteFile(path);
            }

            @Override
            public Map<String, String> properties() {
                return delegateIo.properties();
            }

            @Override
            public void close() {
                delegateIoClosed.set(true);
            }
        };

        Table mockedTable = Mockito.spy(table);
        Mockito.doReturn(trackingIo).when(mockedTable).io();

        Assertions.assertThrows(CancellationException.class, () -> IcebergManifestLoader.uncachedLoader(
                mockedTable, null, null, () -> {
                    if (cancelled.get()) {
                        throw new CancellationException("cancelled after stream open");
                    }
                }).apply(manifest));
        Assertions.assertEquals(1, streamsOpened.get(), "stream should have been opened before cancellation");
        Assertions.assertEquals(1, streamsClosed.get(), "opened stream must be closed on cancellation");
        Assertions.assertFalse(delegateIoClosed.get(), "shared catalog FileIO must never be closed");
    }

    private Table createTable(String name, Schema schema, PartitionSpec spec, Map<String, String> properties) {
        HadoopTables tables = new HadoopTables(new Configuration());
        return tables.create(schema, spec, properties, tempDir.resolve(name).toString());
    }

    private IcebergScanNode createScanNode(Table table, boolean batchMode, Executor manifestExecutor)
            throws Exception {
        SessionVariable sv = new SessionVariable();
        sv.setMaxSplitSize(64L * 1024 * 1024);

        IcebergScanNode node = new IcebergScanNode(
                new PlanNodeId(1), new TupleDescriptor(new TupleId(1)), sv, ScanContext.EMPTY) {
            @Override
            public boolean isBatchMode() {
                return batchMode;
            }

            @Override
            protected Executor getManifestReadExecutor() {
                return manifestExecutor;
            }
        };

        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        Mockito.when(catalog.getProperties()).thenReturn(Collections.emptyMap());
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        Mockito.when(dorisTable.getCatalog()).thenReturn(catalog);

        IcebergSource source = Mockito.mock(IcebergSource.class);
        Mockito.when(source.getCatalog()).thenReturn(catalog);
        Mockito.when(source.getTargetTable()).thenReturn(dorisTable);

        setField(node, "icebergTable", table);
        setField(node, "source", source);
        setField(node, "preExecutionAuthenticator", new ExecutionAuthenticator() { });
        setField(node, "conjuncts", new ArrayList<>());
        IcebergSnapshotCacheValue frozenSource = new IcebergSnapshotCacheValue(
                new IcebergPartitionInfo(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()),
                new IcebergSnapshot(table.currentSnapshot().snapshotId(), table.schema().schemaId()))
                .bindSourceGeneration(new IcebergTableCacheValue(table));
        setField(node, "frozenGenerationSource", frozenSource);
        FederationBackendPolicy policy = Mockito.mock(FederationBackendPolicy.class);
        Mockito.when(policy.getBackends()).thenReturn(Collections.emptyList());
        Mockito.when(policy.numBackends()).thenReturn(0);
        setField(node, "backendPolicy", policy);
        return node;
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Class<?> cls = target.getClass();
        while (cls != null) {
            try {
                Field field = cls.getDeclaredField(name);
                field.setAccessible(true);
                field.set(target, value);
                return;
            } catch (NoSuchFieldException ignored) {
                cls = cls.getSuperclass();
            }
        }
        throw new NoSuchFieldException(name);
    }

    private static FairTaskExecutor newPool(int workers) {
        return new FairTaskExecutor(workers, task -> {
            Thread t = new Thread(task);
            t.setDaemon(true);
            return t;
        });
    }

    private static ManifestFile manifestWithFileCounts(Integer added, Integer existing, Integer deleted) {
        return (ManifestFile) Proxy.newProxyInstance(ManifestFile.class.getClassLoader(),
                new Class<?>[] {ManifestFile.class}, (proxy, method, args) -> {
                    if (method.getName().equals("addedFilesCount")) {
                        return added;
                    }
                    if (method.getName().equals("existingFilesCount")) {
                        return existing;
                    }
                    if (method.getName().equals("deletedFilesCount")) {
                        return deleted;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }
}
