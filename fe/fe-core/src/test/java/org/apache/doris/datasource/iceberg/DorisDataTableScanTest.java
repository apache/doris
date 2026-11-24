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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.SplitAssignment;
import org.apache.doris.datasource.SplitScanContext;
import org.apache.doris.datasource.iceberg.source.IcebergScanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.SessionVariable;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DorisDataTableScan;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ThreadPools;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class DorisDataTableScanTest {
    @Test
    public void fileIoViewKeepsProjectionFilterSnapshotOptionsAndReporter() throws Exception {
        try (InMemoryCatalog catalog = new InMemoryCatalog()) {
            catalog.initialize("test", Collections.emptyMap());
            catalog.createNamespace(Namespace.of("db"));
            Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "value", Types.StringType.get()));
            Table table = catalog.createTable(TableIdentifier.of("db", "io_view"), schema);
            table.newAppend().appendFile(file(table, "first.parquet")).commit();
            long pinnedSnapshot = table.currentSnapshot().snapshotId();
            table.newAppend().appendFile(file(table, "second.parquet")).commit();
            AtomicInteger reports = new AtomicInteger();
            TableScan original = table.newScan().useSnapshot(pinnedSnapshot).select("value")
                    .filter(Expressions.equal("id", 1)).option(TableProperties.SPLIT_SIZE, "123")
                    .metricsReporter(report -> reports.incrementAndGet());
            TableScan guarded = DorisDataTableScan.withPlanningTable(original,
                    view -> IcebergSnapshotCacheValue.withFileIO(view, io -> io),
                    ThreadPools.getWorkerPool()).orElseThrow(AssertionError::new);
            Assertions.assertNotSame(original.table(), guarded.table());
            Assertions.assertSame(table.io(), guarded.table().io());
            Assertions.assertEquals(original.schema().asStruct(), guarded.schema().asStruct());
            Assertions.assertEquals(original.filter(), guarded.filter());
            Assertions.assertEquals(pinnedSnapshot, guarded.snapshot().snapshotId());
            Assertions.assertEquals(123, guarded.targetSplitSize());
            List<String> paths = new ArrayList<>();
            try (CloseableIterable<FileScanTask> tasks = guarded.planFiles()) {
                tasks.forEach(task -> paths.add(task.file().location()));
            }
            Assertions.assertEquals(Collections.singletonList("first.parquet"), paths);
            Assertions.assertEquals(1, reports.get());
        }
    }

    @Test
    public void customScansKeepTheirSemanticsWithoutInvokingFileIoWrapper() throws Exception {
        try (InMemoryCatalog catalog = new InMemoryCatalog()) {
            catalog.initialize("test", Collections.emptyMap());
            catalog.createNamespace(Namespace.of("db"));
            Table table = catalog.createTable(TableIdentifier.of("db", "custom"),
                    new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())));
            TableScan nativeScan = table.newScan();
            TableScan custom = (TableScan) Proxy.newProxyInstance(TableScan.class.getClassLoader(),
                    new Class<?>[] {TableScan.class}, (proxy, method, args) -> method.invoke(nativeScan, args));
            Assertions.assertFalse(DorisDataTableScan.withPlanningTable(custom, view -> {
                throw new AssertionError("unsupported scans must not acquire a request-local IO");
            }, ThreadPools.getWorkerPool()).isPresent());
        }
    }

    @Test
    public void wrappedNativeScanRejectsSdkReadsAfterItsQueryIsCancelled() throws Exception {
        try (InMemoryCatalog catalog = new InMemoryCatalog()) {
            catalog.initialize("test", Collections.emptyMap());
            catalog.createNamespace(Namespace.of("db"));
            Table table = catalog.createTable(TableIdentifier.of("db", "cancelled"),
                    new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())));
            table.newAppend().appendFile(file(table, "first.parquet")).commit();
            SplitScanContext context = new SplitScanContext();
            try (IcebergScanPlanningScope scope = new IcebergScanPlanningScope(context)) {
                TableScan guarded = DorisDataTableScan.withPlanningTable(DorisDataTableScan.wrap(table.newScan()),
                        view -> IcebergSnapshotCacheValue.withFileIO(view,
                                io -> new IcebergScanFileIO(io, scope::checkCancelled)), scope)
                        .orElseThrow(AssertionError::new);
                context.cancel();
                Assertions.assertThrows(java.util.concurrent.CancellationException.class, () -> {
                    try (CloseableIterable<FileScanTask> tasks = guarded.planFiles()) {
                        tasks.iterator().hasNext();
                    }
                });
            }
        }
    }

    @Test
    public void weightedSnapshotCacheKeepsIsolationAndRejectsCancelledManifestReads() throws Exception {
        try (InMemoryCatalog catalog = new InMemoryCatalog()) {
            catalog.initialize("test", Collections.emptyMap());
            catalog.createNamespace(Namespace.of("db"));
            Table table = catalog.createTable(TableIdentifier.of("db", "weighted"),
                    new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())));
            table.newAppend().appendFile(file(table, "first.parquet")).commit();
            long firstSnapshot = table.currentSnapshot().snapshotId();
            table.newAppend().appendFile(file(table, "second.parquet")).commit();
            IcebergSnapshotCacheValue cached = new IcebergSnapshotCacheValue(IcebergPartitionInfo.empty(),
                    new IcebergSnapshot(table.currentSnapshot().snapshotId(), table.schema().schemaId()),
                    Optional.empty(), table);
            IcebergSnapshotEntryKey key = IcebergSnapshotEntryKey.tryCreate(
                    NameMapping.createForTest(1L, "db", "weighted"), table).orElseThrow(AssertionError::new);
            Assertions.assertTrue(cached.prepareForCachePublication(key).isComplete());
            Table queryTable = cached.getIcebergTable().orElseThrow(AssertionError::new);
            Table retained = cached.getRetainedIcebergTable().orElseThrow(AssertionError::new);
            Snapshot selectedSnapshot = queryTable.snapshot(firstSnapshot);
            Assertions.assertNotSame(retained.snapshot(firstSnapshot), selectedSnapshot);
            SplitScanContext context = new SplitScanContext();
            try (IcebergScanPlanningScope scope = new IcebergScanPlanningScope(context)) {
                TableScan guarded = DorisDataTableScan.withPlanningTable(
                        DorisDataTableScan.wrap(queryTable.newScan().useSnapshot(firstSnapshot)),
                        view -> IcebergSnapshotCacheValue.withFileIO(view,
                                io -> new IcebergScanFileIO(io, scope::checkCancelled)), scope)
                        .orElseThrow(AssertionError::new);
                Assertions.assertSame(selectedSnapshot, guarded.snapshot());
                Assertions.assertNotSame(retained.currentSnapshot(), guarded.table().currentSnapshot());
                try (CloseableIterable<FileScanTask> tasks = guarded.planFiles()) {
                    List<String> paths = new ArrayList<>();
                    tasks.forEach(task -> paths.add(task.file().location()));
                    Assertions.assertEquals(Collections.singletonList("first.parquet"), paths);
                }
                context.cancel();
                Assertions.assertThrows(java.util.concurrent.CancellationException.class, () -> {
                    try (CloseableIterable<FileScanTask> tasks = guarded.planFiles()) {
                        tasks.iterator().hasNext();
                    }
                });
            }
            ThreadPoolExecutor workers = (ThreadPoolExecutor) Executors.newFixedThreadPool(2);
            try {
                IcebergScanNode node = new IcebergScanNode(new PlanNodeId(0), new TupleDescriptor(new TupleId(0)),
                        new SessionVariable(), ScanContext.EMPTY);
                IcebergRuntimeContext runtime = Mockito.mock(IcebergRuntimeContext.class);
                Mockito.when(runtime.getPlanningExecutor()).thenReturn(workers);
                Field runtimeField = IcebergScanNode.class.getDeclaredField("runtimeContext");
                runtimeField.setAccessible(true);
                runtimeField.set(node, runtime);
                SplitAssignment assignment = new SplitAssignment(
                        null, null, null, Collections.emptyMap(), Collections.emptyList(), false);
                Field assignmentField = ScanNode.class.getDeclaredField("splitAssignment");
                assignmentField.setAccessible(true);
                assignmentField.set(node, assignment);
                Method planFiles = IcebergScanNode.class.getDeclaredMethod("planSdkFiles", TableScan.class);
                planFiles.setAccessible(true);
                @SuppressWarnings("unchecked")
                CloseableIterable<FileScanTask> tasks = (CloseableIterable<FileScanTask>) planFiles.invoke(
                        node, DorisDataTableScan.wrap(queryTable.newScan().useSnapshot(firstSnapshot)));
                try (CloseableIterable<FileScanTask> ownedTasks = tasks) {
                    assignment.getScanContext().cancel();
                    Assertions.assertThrows(java.util.concurrent.CancellationException.class,
                            () -> ownedTasks.iterator().hasNext());
                }
            } finally {
                workers.shutdownNow();
            }
        }
    }

    private static DataFile file(Table table, String path) {
        return DataFiles.builder(table.spec()).withPath(path).withFormat(FileFormat.PARQUET)
                .withRecordCount(1).withFileSizeInBytes(100).build();
    }
}
