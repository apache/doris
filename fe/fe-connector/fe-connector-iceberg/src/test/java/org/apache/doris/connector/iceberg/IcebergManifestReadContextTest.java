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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScopes;
import org.apache.doris.connector.spi.ConnectorTaskScheduler;

import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;

@Timeout(30)
class IcebergManifestReadContextTest {
    @ParameterizedTest
    @CsvSource({"false, false", "true, false", "false, true", "true, true"})
    void deleteManifestReadsShareTheDataReadBudget(boolean cancel, boolean equality) throws Exception {
        CountDownLatch waiting = new CountDownLatch(1);
        Semaphore budget = new Semaphore(1) {
            @Override
            public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
                waiting.countDown();
                return super.tryAcquire(timeout, unit);
            }
        };
        TestStatementScope scope = new TestStatementScope();
        AtomicBoolean closed = new AtomicBoolean();
        ConnectorTaskScheduler scheduler = (ConnectorTaskScheduler) Proxy.newProxyInstance(
                ConnectorTaskScheduler.class.getClassLoader(), new Class<?>[] {ConnectorTaskScheduler.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("isClosed")) {
                        return closed.get();
                    }
                    throw new AssertionError("unexpected scheduler operation: " + method.getName());
                });
        ConnectorSession delegate = new IcebergStatementScopeTest.ScopeSession(7, "union", scope);
        ConnectorSession session = (ConnectorSession) Proxy.newProxyInstance(
                ConnectorSession.class.getClassLoader(), new Class<?>[] {ConnectorSession.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getSessionProperties")) {
                        return Map.of("iceberg_manifest_load_concurrency", "1");
                    }
                    if (method.getName().equals("getTaskScheduler")) {
                        return scheduler;
                    }
                    return method.invoke(delegate, args);
                });
        scope.computeIfAbsent(IcebergStatementScope.MANIFEST_READ_BUDGET_NAMESPACE + ":union", () -> budget);
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("delete-budget", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        Table original = catalog.createTable(TableIdentifier.of("db1", "t1"), schema,
                PartitionSpec.unpartitioned(), Map.of("format-version", "2"));
        original.newAppend().appendFile(DataFiles.builder(original.spec())
                .withPath("s3://bucket/data.parquet").withFileSizeInBytes(128).withRecordCount(1).build()).commit();
        org.apache.iceberg.DeleteFile delete = equality
                ? FileMetadata.deleteFileBuilder(original.spec()).ofEqualityDeletes(1)
                        .withPath("s3://bucket/delete.parquet").withFileSizeInBytes(128).withRecordCount(1).build()
                : FileMetadata.deleteFileBuilder(original.spec()).ofPositionDeletes()
                        .withPath("s3://bucket/delete.parquet").withFileSizeInBytes(128).withRecordCount(1).build();
        original.newRowDelta().addDeletes(delete).commit();
        String deletePath = original.currentSnapshot().deleteManifests(original.io()).get(0).path();
        AtomicInteger deleteReads = new AtomicInteger();
        FileIO io = (FileIO) Proxy.newProxyInstance(FileIO.class.getClassLoader(), new Class<?>[] {FileIO.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("newInputFile")) {
                        Object file = args[0];
                        String path = file instanceof ManifestFile ? ((ManifestFile) file).path() : file.toString();
                        if (deletePath.equals(path)) {
                            deleteReads.incrementAndGet();
                        }
                    }
                    return method.invoke(original.io(), args);
                });
        Table table = (Table) Proxy.newProxyInstance(Table.class.getClassLoader(), new Class<?>[] {Table.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("io")) {
                        return io;
                    }
                    return method.invoke(original, args);
                });
        ConnectorStatementScopes.resolveInStatement(session, IcebergStatementScope.TABLE_NAMESPACE,
                "db1", "t1", () -> table);
        TableScan scan = original.newScan();
        IcebergManifestCache cache = new IcebergManifestCache();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(
                IcebergCatalogProperties.of(Map.of(IcebergConnector.MANIFEST_CACHE_ENABLE, "true")),
                new RecordingIcebergCatalogOps(), null, cache);
        Method plan = IcebergScanPlanProvider.class.getDeclaredMethod("planFileScanTask",
                TableScan.class, ConnectorSession.class, Table.class, Optional.class, IcebergTableHandle.class);
        plan.setAccessible(true);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        budget.acquire(); // The first scan is currently loading a cold data manifest.
        try {
            Future<?> secondScan = worker.submit(() -> equality
                    ? provider.getScanNodeProperties(session, new IcebergTableHandle("db1", "t1"),
                            Collections.emptyList(), Optional.empty())
                    : plan.invoke(provider, scan, session, table, Optional.empty(),
                            new IcebergTableHandle("db1", "t1")));
            Assertions.assertTrue(waiting.await(5, TimeUnit.SECONDS));
            Assertions.assertEquals(0, deleteReads.get(), "delete reads cannot bypass the shared query budget");
            if (cancel) {
                closed.set(true);
                ExecutionException failure = Assertions.assertThrows(ExecutionException.class,
                        () -> secondScan.get(5, TimeUnit.SECONDS));
                Throwable cause = failure;
                while (cause.getCause() != null) {
                    cause = cause.getCause();
                }
                Assertions.assertInstanceOf(CancellationException.class, cause);
                Assertions.assertEquals(0, deleteReads.get(), "cancellation must not restart SDK planning");
                Assertions.assertEquals(0, budget.availablePermits(), "the other scan still owns its permit");
                budget.release();
            } else {
                budget.release();
                secondScan.get(5, TimeUnit.SECONDS);
                Assertions.assertTrue(deleteReads.get() > 0);
                Assertions.assertEquals(1, budget.availablePermits());
            }
        } finally {
            worker.shutdownNow();
            scope.closeAll();
        }
    }

    @ParameterizedTest
    @CsvSource({"streaming, acquisition", "materialized, acquisition", "representative, acquisition",
            "count, acquisition", "streaming, failure", "materialized, failure",
            "representative, failure", "count, failure"})
    void cancellationBeforeSchedulerAcquisitionCannotRestartSdkPlanning(String path, String trigger) throws Exception {
        AtomicBoolean closed = new AtomicBoolean();
        ConnectorTaskScheduler scheduler = (ConnectorTaskScheduler) Proxy.newProxyInstance(
                ConnectorTaskScheduler.class.getClassLoader(), new Class<?>[] {ConnectorTaskScheduler.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("isClosed")) {
                        return closed.get();
                    }
                    throw new AssertionError("unexpected scheduler operation: " + method.getName());
                });
        ConnectorSession delegate = new IcebergStatementScopeTest.ScopeSession(7, "cancelled", new TestStatementScope());
        ConnectorSession session = (ConnectorSession) Proxy.newProxyInstance(
                ConnectorSession.class.getClassLoader(), new Class<?>[] {ConnectorSession.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getTaskScheduler")) {
                        if (trigger.equals("acquisition")) {
                            // QueryTaskGroups.get reports cancellation after the captured query was closed.
                            throw new CancellationException("Query task groups are closed");
                        }
                        return scheduler;
                    }
                    return method.invoke(delegate, args);
                });
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("cancel-before-acquisition", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        Table table = catalog.createTable(TableIdentifier.of("db1", "t1"), schema,
                PartitionSpec.unpartitioned(), Map.of("format-version", "2"));
        table.newAppend().appendFile(DataFiles.builder(table.spec()).withPath("s3://bucket/data.parquet")
                .withFileSizeInBytes(128).withRecordCount(1).build()).commit();
        table.newRowDelta().addDeletes(FileMetadata.deleteFileBuilder(table.spec()).ofPositionDeletes()
                .withPath("s3://bucket/delete.parquet").withFileSizeInBytes(128).withRecordCount(1).build()).commit();
        AtomicInteger sdkPlans = new AtomicInteger();
        TableScan original = table.newScan();
        TableScan scan = (TableScan) Proxy.newProxyInstance(TableScan.class.getClassLoader(),
                new Class<?>[] {TableScan.class}, (proxy, method, args) -> {
                    if (method.getName().equals("planFiles")) {
                        sdkPlans.incrementAndGet();
                    }
                    if (method.getName().equals("snapshot") && trigger.equals("failure")) {
                        closed.set(true);
                        throw new IllegalStateException("cache setup failed during cancellation");
                    }
                    return method.invoke(original, args);
                });
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(
                IcebergCatalogProperties.of(Map.of(IcebergConnector.MANIFEST_CACHE_ENABLE, "true")),
                new RecordingIcebergCatalogOps(), null, new IcebergManifestCache());
        IcebergTableHandle handle = new IcebergTableHandle("db1", "t1");
        String methodName;
        Object[] arguments;
        switch (path) {
            case "streaming":
                methodName = "streamingFileScanTasks";
                arguments = new Object[] {scan, session, table, Optional.empty(), 128L, null, handle};
                break;
            case "materialized":
                methodName = "planFileScanTask";
                arguments = new Object[] {scan, session, table, Optional.empty(), handle};
                break;
            case "representative":
                methodName = "planManifestCountRange";
                arguments = new Object[] {table, scan, 1L, 2, false, Collections.emptyList(),
                        ZoneId.of("UTC"), UnaryOperator.identity(),
                        session, Optional.empty(), true, handle};
                break;
            case "count":
                methodName = "planCountPushdownFromFileTasks";
                arguments = new Object[] {table, scan, 2, false, Collections.emptyList(),
                        ZoneId.of("UTC"), UnaryOperator.identity(),
                        session, Optional.empty(), true, 0L, handle};
                break;
            default:
                throw new AssertionError(path);
        }
        Method entry = Arrays.stream(IcebergScanPlanProvider.class.getDeclaredMethods())
                .filter(method -> method.getName().equals(methodName)).findFirst().orElseThrow();
        entry.setAccessible(true);
        InvocationTargetException error = Assertions.assertThrows(
                InvocationTargetException.class, () -> entry.invoke(provider, arguments));
        Assertions.assertInstanceOf(CancellationException.class, error.getCause());
        Assertions.assertEquals(0, sdkPlans.get(), "cancelled cache planning must not open a fresh SDK delete index");
    }

    @Test
    void offlineEmptyScanDoesNotAcquireAQueryScheduler() throws Exception {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("offline-empty", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        Table table = catalog.createTable(TableIdentifier.of("db1", "t1"),
                new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())));
        RecordingConnectorContext context = new RecordingConnectorContext();
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(
                IcebergCatalogProperties.of(Map.of(IcebergConnector.MANIFEST_CACHE_ENABLE, "true")),
                new RecordingIcebergCatalogOps(), context, new IcebergManifestCache());
        Method plan = IcebergScanPlanProvider.class.getDeclaredMethod("cacheBackedFileScanTasks",
                TableScan.class, ConnectorSession.class, Table.class, Optional.class,
                String.class, IcebergTableHandle.class);
        plan.setAccessible(true);
        try (CloseableIterable<?> tasks = (CloseableIterable<?>) plan.invoke(provider, table.newScan(), null,
                table, Optional.empty(), null, new IcebergTableHandle("db1", "t1"))) {
            Assertions.assertFalse(tasks.iterator().hasNext());
        }
        Assertions.assertEquals(0, context.metadataSubmissions.get());
    }

    private static ManifestFile sourceManifest() {
        return (ManifestFile) Proxy.newProxyInstance(ManifestFile.class.getClassLoader(),
                new Class<?>[] {ManifestFile.class}, (proxy, method, args) -> "delete.avro");
    }

    @Test
    void failedReadReleasesPermitAndOwner() {
        Semaphore budget = new Semaphore(1);
        AtomicInteger owners = new AtomicInteger();
        IcebergManifestReadContext reads = new IcebergManifestReadContext(null, budget, () -> false,
                () -> {
                    owners.incrementAndGet();
                    return owners::decrementAndGet;
                });
        IllegalStateException failure = new IllegalStateException("broken delete manifest");
        Assertions.assertSame(failure, Assertions.assertThrows(IllegalStateException.class,
                () -> reads.read(sourceManifest(), m -> {
                    throw failure;
                })));
        Assertions.assertEquals(0, owners.get());
        Assertions.assertEquals(1, budget.availablePermits());
    }
}
