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

package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.deltalake.DeltaLakeSnapshotCache.SnapshotKey;
import org.apache.doris.connector.deltalake.DeltaLakeSnapshotCache.TableKey;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorTableStatistics;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StructType;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Timeout(30)
class DeltaLakeStatisticsCacheTest {
    @TempDir
    Path warehouse;
    private final Engine engine = DefaultEngine.create(new Configuration());
    private final SnapshotKey key = key("alice", "table-id", 0);
    private final ConnectorTableStatistics exact = new ConnectorTableStatistics(30, 300);
    private final Optional<ConnectorTableStatistics> fallback = Optional.of(new ConnectorTableStatistics(-1, 300));

    @Test
    void coldLookupReturnsImmediatelyAndConcurrentLookupsShareOneLoad() throws Exception {
        QueueExecutor executor = new QueueExecutor();
        ExecutorService callers = Executors.newFixedThreadPool(8);
        CyclicBarrier start = new CyclicBarrier(8);
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            DeltaLakeStatisticsCache cache = new DeltaLakeStatisticsCache(owner, Map.of(), executor);
            List<Future<?>> requests = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                requests.add(callers.submit(() -> {
                    start.await(10, TimeUnit.SECONDS);
                    Assertions.assertEquals(fallback, cache.get(key, fallback, () -> exact));
                    return null;
                }));
            }
            for (Future<?> request : requests) {
                request.get(10, TimeUnit.SECONDS);
            }
            Assertions.assertEquals(1, executor.tasks.size());
            executor.runNext();
            Assertions.assertEquals(Optional.of(exact), cache.get(key, fallback, () -> Assertions.fail("reloaded")));
        } finally {
            callers.shutdownNow();
        }
    }

    @Test
    void isolatesUsersVersionsTableIdentityAndPath() {
        QueueExecutor executor = new QueueExecutor();
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            DeltaLakeStatisticsCache cache = new DeltaLakeStatisticsCache(owner, Map.of(), executor);
            List<SnapshotKey> keys = List.of(key, key("bob", "table-id", 0), key("alice", "table-id", 1),
                    key("alice", "replacement-id", 0),
                    new SnapshotKey(new TableKey("alice", "db", "table", "/moved"), "table-id", 0));
            for (int i = 0; i < keys.size(); i++) {
                ConnectorTableStatistics value = new ConnectorTableStatistics(i, 100);
                Assertions.assertEquals(fallback, cache.get(keys.get(i), fallback, () -> value));
                executor.runNext();
            }
            for (int i = 0; i < keys.size(); i++) {
                Assertions.assertEquals(i, cache.get(keys.get(i), fallback, () -> Assertions.fail()).orElseThrow()
                        .getRowCount());
            }
            owner.invalidateDatabase("db");
            Assertions.assertEquals(fallback, cache.get(key, fallback, () -> exact));
        }
    }

    @Test
    void refreshAndCloseCancelQueuedWork() {
        QueueExecutor executor = new QueueExecutor();
        AtomicInteger loads = new AtomicInteger();
        CatalogMetaCache owner = CatalogMetaCache.unmanaged();
        DeltaLakeStatisticsCache cache = new DeltaLakeStatisticsCache(owner, Map.of(), executor);
        Callable<ConnectorTableStatistics> loader = () -> {
            loads.incrementAndGet();
            return exact;
        };
        try {
            cache.get(key, fallback, loader);
            owner.invalidateTable("db", "table");
            cache.get(key, fallback, loader);
            executor.runNext(); // The pre-REFRESH task was cancelled before it opened storage.
            Assertions.assertEquals(0, loads.get());
            executor.runNext();
            Assertions.assertEquals(1, loads.get());
            owner.invalidateCatalog();
            cache.get(key, fallback, loader);
        } finally {
            owner.close();
        }
        executor.runNext();
        Assertions.assertEquals(1, loads.get());
    }

    @Test
    void lateCompletionAfterRefreshCannotReplaceNewStatistics() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            DeltaLakeStatisticsCache cache = new DeltaLakeStatisticsCache(owner, Map.of(), executor);
            cache.get(key, fallback, () -> {
                started.countDown();
                // Model remote IO that finishes even after the caller cancels it.
                boolean waiting = true;
                while (waiting) {
                    try {
                        waiting = !release.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException ignored) {
                        // Deliberate: the result still must not reappear after REFRESH.
                    }
                }
                finished.countDown();
                return new ConnectorTableStatistics(999, 999);
            });
            Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));
            owner.invalidateTable("db", "table");
            Assertions.assertEquals(exact, await(() -> cache.get(key, fallback, () -> exact), 30));
            release.countDown();
            Assertions.assertTrue(finished.await(10, TimeUnit.SECONDS));
            Assertions.assertEquals(exact, cache.get(key, fallback, () -> Assertions.fail()).orElseThrow());
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void disabledAndWeightRejectedCachesDoNotStartUnretainedLoads() {
        for (Map<String, String> properties : List.of(
                Map.of("meta.cache.deltalake.statistics.enable", "false"),
                Map.of("meta.cache.deltalake.statistics.ttl-second", "0"),
                Map.of("meta.cache.deltalake.statistics.max-weight", "1"))) {
            QueueExecutor executor = new QueueExecutor();
            try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
                DeltaLakeStatisticsCache cache = new DeltaLakeStatisticsCache(owner, properties, executor);
                Assertions.assertEquals(fallback, cache.get(key, fallback, () -> Assertions.fail()));
                Assertions.assertTrue(executor.tasks.isEmpty());
            }
        }
    }

    @Test
    void queueRejectionAndLoadFailureRemainRetryable() {
        QueueExecutor executor = new QueueExecutor();
        AtomicInteger submissions = new AtomicInteger();
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            DeltaLakeStatisticsCache cache = new DeltaLakeStatisticsCache(owner, Map.of(), task -> {
                if (submissions.getAndIncrement() == 0) {
                    throw new RejectedExecutionException("full");
                }
                executor.execute(task);
            });
            Assertions.assertEquals(fallback, cache.get(key, fallback, () -> exact));
            cache.get(key, fallback, () -> {
                throw new IOException("unavailable log");
            });
            executor.runNext();
            Assertions.assertEquals(fallback, cache.get(key, fallback, () -> Assertions.fail()));
            cache.get(key, fallback, () -> exact);
            executor.runNext();
            Assertions.assertEquals(exact, cache.get(key, fallback, () -> Assertions.fail()).orElseThrow());
        }
    }

    @Test
    void countsActiveFilesAtEachVersionAndUsesOnlyMatchingCrc() throws Exception {
        Path table = createTable();
        add(table, "a.parquet", 100, 10L);
        add(table, "b.parquet", 200, 20L);
        checksum(table, 0, 2, 300);
        Snapshot first = snapshot(table, 0);
        Assertions.assertEquals(exact, DeltaLakeStatisticsCache.collect(first, engine));
        Assertions.assertEquals(fallback, DeltaKernelCompat.tableStatistics(first));
        Files.writeString(table.resolve("_delta_log/00000000000000000001.json"),
                "{\"remove\":{\"path\":\"a.parquet\",\"deletionTimestamp\":1,\"dataChange\":true}}\n");
        Snapshot second = snapshot(table, 1);
        Assertions.assertEquals(new ConnectorTableStatistics(20, 200), DeltaLakeStatisticsCache.collect(second, engine));
        Assertions.assertTrue(DeltaKernelCompat.tableStatistics(second).isEmpty(), "old CRC is not current statistics");
    }

    @Test
    void emptyMissingAndInvalidStatisticsStayDistinct() throws Exception {
        Path table = createTable();
        checksum(table, 0, 0, 0);
        Assertions.assertEquals(new ConnectorTableStatistics(0, 0),
                DeltaKernelCompat.tableStatistics(snapshot(table, 0)).orElseThrow());
        Assertions.assertEquals(new ConnectorTableStatistics(0, 0),
                DeltaLakeStatisticsCache.collect(snapshot(table, 0), engine));
        Files.delete(table.resolve("_delta_log/00000000000000000000.crc"));
        add(table, "known.parquet", 100, 10L);
        add(table, "unknown.parquet", 200, null);
        Assertions.assertEquals(new ConnectorTableStatistics(-1, 300),
                DeltaLakeStatisticsCache.collect(snapshot(table, 0), engine));
        DeltaLakeTestTables.commit(table, 0, "table-id", new StructType().add("id", LongType.LONG));
        add(table, "invalid.parquet", 100, -1L);
        Assertions.assertThrows(DorisConnectorException.class,
                () -> DeltaLakeStatisticsCache.collect(snapshot(table, 0), engine));
    }

    @Test
    void subtractsDeletionVectorsAndReplaysBothCheckpointFormats() throws Exception {
        for (String fixture : List.of("dv-partitioned-with-checkpoint", "v2-checkpoint-json", "v2-checkpoint-parquet")) {
            Path table = Path.of(getClass().getResource("/deltalake/" + fixture).toURI());
            Snapshot snapshot = TableManager.loadSnapshot(table.toString()).build(engine);
            ConnectorTableStatistics result = DeltaLakeStatisticsCache.collect(snapshot, engine);
            Assertions.assertEquals(fixture.startsWith("dv-") ? 35 : 10, result.getRowCount());
            Assertions.assertTrue(result.getDataSize() > 0);
        }
    }

    @Test
    void metadataReportsCachedCountsAtThePinnedVersionAfterAnAppend() throws Exception {
        Path table = createTable();
        add(table, "a.parquet", 100, 10L);
        AtomicInteger backgroundAuthentication = new AtomicInteger();
        RecordingConnectorContext context = new RecordingConnectorContext() {
            @Override
            public <T> T executeAuthenticated(Callable<T> action) throws Exception {
                if (Thread.currentThread().getName().equals("deltalake-statistics")) {
                    backgroundAuthentication.incrementAndGet();
                    Assertions.assertSame(DeltaLakeRuntime.class.getClassLoader(),
                            Thread.currentThread().getContextClassLoader());
                }
                return super.executeAuthenticated(action);
            }
        };
        try (Connector connector = new DeltaLakeConnectorProvider().create(Map.of("warehouse", warehouse.toString()),
                context); ConnectorMetadata metadata = connector.getMetadata(null)) {
            TestingConnectorSession first = new TestingConnectorSession("first");
            ConnectorTableHandle handle = metadata.getTableHandle(first, "db", "table").orElseThrow();
            ConnectorMvccSnapshot pin = metadata.beginQuerySnapshot(first, handle).orElseThrow();
            Assertions.assertEquals(10, await(() -> metadata.getTableStatistics(first, handle), 10).getRowCount());
            Assertions.assertTrue(backgroundAuthentication.get() > 0);
            Files.writeString(table.resolve("_delta_log/00000000000000000001.json"),
                    "{\"remove\":{\"path\":\"a.parquet\",\"deletionTimestamp\":1,\"dataChange\":true}}\n");
            connector.invalidateTable("db", "table");
            TestingConnectorSession next = new TestingConnectorSession("next");
            ConnectorTableHandle latest = metadata.getTableHandle(next, "db", "table").orElseThrow();
            Assertions.assertEquals(0, await(() -> metadata.getTableStatistics(next, latest), 0).getRowCount());
            Assertions.assertEquals(10, await(() -> metadata.getTableStatistics(next, latest, pin), 10).getRowCount());
        }
    }

    private static SnapshotKey key(String user, String id, long version) {
        return new SnapshotKey(new TableKey(user, "db", "table", "/table"), id, version);
    }

    private Path createTable() throws IOException {
        Path table = warehouse.resolve("db/table");
        DeltaLakeTestTables.commit(table, 0, "table-id", new StructType().add("id", LongType.LONG));
        return table;
    }

    private Snapshot snapshot(Path table, long version) {
        return TableManager.loadSnapshot(table.toString()).atVersion(version).build(engine);
    }

    private void add(Path table, String path, long size, Long records) throws IOException {
        String stats = records == null ? "" : ",\"stats\":\"{\\\"numRecords\\\":" + records + "}\"";
        Files.writeString(table.resolve("_delta_log/00000000000000000000.json"),
                "{\"add\":{\"path\":\"" + path + "\",\"partitionValues\":{},\"size\":" + size
                        + ",\"modificationTime\":0,\"dataChange\":true" + stats + "}}\n", StandardOpenOption.APPEND);
    }

    private void checksum(Path table, long version, long files, long bytes) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        List<String> actions = Files.readAllLines(table.resolve("_delta_log/00000000000000000000.json"));
        Files.writeString(table.resolve(String.format("_delta_log/%020d.crc", version)), mapper.writeValueAsString(Map.of(
                "tableSizeBytes", bytes, "numFiles", files, "numMetadata", 1, "numProtocol", 1,
                "metadata", mapper.readTree(actions.get(1)).get("metaData"),
                "protocol", mapper.readTree(actions.get(0)).get("protocol"))));
    }

    private static ConnectorTableStatistics await(Callable<Optional<ConnectorTableStatistics>> action, long rows)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        do {
            Optional<ConnectorTableStatistics> result = action.call();
            if (result.isPresent() && result.get().getRowCount() == rows) {
                return result.get();
            }
            Thread.sleep(10);
        } while (System.nanoTime() < deadline);
        return Assertions.fail("statistics did not become ready for row count " + rows);
    }

    private static final class QueueExecutor implements Executor {
        private final Queue<Runnable> tasks = new ConcurrentLinkedQueue<>();

        @Override
        public void execute(Runnable task) {
            tasks.add(task);
        }

        void runNext() {
            tasks.remove().run();
        }
    }
}
