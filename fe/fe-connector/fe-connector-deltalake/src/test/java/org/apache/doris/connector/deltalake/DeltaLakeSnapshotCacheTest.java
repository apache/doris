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

import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

class DeltaLakeSnapshotCacheTest {
    @TempDir
    Path table;

    private final Engine engine = DefaultEngine.create(new Configuration());
    private final DeltaLakeSnapshotCache.TableKey key =
            new DeltaLakeSnapshotCache.TableKey("alice", "db", "table", "/table");

    @Test
    void fixedTtlStartsAfterLoadingAndHitsNeverExtendIt() throws Exception {
        Snapshot first = snapshot(0);
        Snapshot second = snapshot(1);
        AtomicLong clock = new AtomicLong();
        AtomicInteger loads = new AtomicInteger();
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            DeltaLakeSnapshotCache cache = new DeltaLakeSnapshotCache(owner, Map.of(), clock::get);
            Supplier<Snapshot> loader = () -> {
                clock.addAndGet(TimeUnit.SECONDS.toNanos(5));
                return loads.incrementAndGet() == 1 ? first : second;
            };
            Assertions.assertSame(first, cache.latest(key, loader, version -> Assertions.fail("snapshot missed")));
            for (int seconds = 6; seconds < 35; seconds++) {
                clock.set(TimeUnit.SECONDS.toNanos(seconds));
                Assertions.assertSame(first, cache.latest(key, loader, version -> Assertions.fail("snapshot missed")));
            }
            Assertions.assertEquals(1, loads.get());
            clock.set(TimeUnit.SECONDS.toNanos(35));
            Assertions.assertSame(second, cache.latest(key, loader, version -> Assertions.fail("snapshot missed")));
            Assertions.assertEquals(2, loads.get());
            Assertions.assertEquals(0, first.getVersion());
        }
    }

    @Test
    void disabledLatestAlwaysLoadsAndARejectedSnapshotDoesNotDoubleLoadTheMiss() throws Exception {
        Snapshot snapshot = snapshot(0);
        AtomicInteger loads = new AtomicInteger();
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            DeltaLakeSnapshotCache cache = new DeltaLakeSnapshotCache(owner,
                    Map.of("meta.cache.deltalake.latest.ttl-second", "0",
                            "meta.cache.deltalake.snapshot.max-weight", "1"));
            for (int i = 0; i < 3; i++) {
                Assertions.assertSame(snapshot, cache.latest(key, () -> {
                    loads.incrementAndGet();
                    return snapshot;
                }, version -> Assertions.fail("duplicate load")));
            }
            Assertions.assertEquals(3, loads.get());
        }
    }

    @Test
    void usersScopesAndWeightAdmissionRemainIndependent() throws Exception {
        Snapshot first = snapshot(0);
        Snapshot second = snapshot(1);
        AtomicInteger versions = new AtomicInteger();
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            DeltaLakeSnapshotCache cache = new DeltaLakeSnapshotCache(owner,
                    Map.of("meta.cache.deltalake.snapshot.max-weight", "1"));
            Assertions.assertSame(first, cache.latest(key, () -> first, version -> Assertions.fail()));
            Assertions.assertSame(first, cache.latest(key, () -> Assertions.fail("latest missed"), version -> {
                versions.incrementAndGet();
                Assertions.assertEquals(0, version);
                return first;
            }));
            Assertions.assertEquals(1, versions.get());
            DeltaLakeSnapshotCache.TableKey bob = new DeltaLakeSnapshotCache.TableKey("bob", "db", "table", "/table");
            Assertions.assertSame(second, cache.latest(bob, () -> second, version -> Assertions.fail()));
            owner.invalidateTable("db", "table");
            Assertions.assertSame(second, cache.latest(key, () -> second, version -> Assertions.fail()));
            owner.invalidateDatabase("db");
            Assertions.assertSame(first, cache.latest(key, () -> first, version -> Assertions.fail()));
            owner.invalidateCatalog();
            Assertions.assertSame(second, cache.latest(key, () -> second, version -> Assertions.fail()));
        }
    }

    @Test
    void refreshFencesAConcurrentLoadWithoutChangingItsStatementsSnapshot() throws Exception {
        Snapshot first = snapshot(0);
        Snapshot second = snapshot(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch loading = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            DeltaLakeSnapshotCache cache = new DeltaLakeSnapshotCache(owner, Map.of());
            Future<Snapshot> pending = executor.submit(() -> cache.latest(key, () -> {
                loading.countDown();
                try {
                    Assertions.assertTrue(finish.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                return first;
            }, version -> Assertions.fail()));
            Assertions.assertTrue(loading.await(10, TimeUnit.SECONDS));
            owner.invalidateTable("db", "table");
            Assertions.assertSame(second, cache.latest(key, () -> second, version -> Assertions.fail()));
            finish.countDown();
            Assertions.assertSame(first, pending.get(10, TimeUnit.SECONDS));
            Assertions.assertSame(second, cache.latest(key, () -> Assertions.fail("stale load republished"),
                    version -> Assertions.fail()));
        } finally {
            finish.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void weightBoundLimitsAdmissionButKeepsTheVersionPin() throws Exception {
        Snapshot snapshot = snapshot(0);
        long bytes = DeltaKernelCompat.snapshotRetainedBytes(snapshot);
        AtomicInteger reloads = new AtomicInteger();
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            DeltaLakeSnapshotCache cache = new DeltaLakeSnapshotCache(owner,
                    Map.of("meta.cache.deltalake.snapshot.max-weight", String.valueOf(bytes + 2048)));
            cache.latest(key, () -> snapshot, version -> Assertions.fail());
            DeltaLakeSnapshotCache.TableKey another =
                    new DeltaLakeSnapshotCache.TableKey("alice", "db", "another", "/another");
            cache.latest(another, () -> snapshot, version -> Assertions.fail());
            cache.latest(key, () -> Assertions.fail("latest pin evicted"), version -> {
                reloads.incrementAndGet();
                return snapshot;
            });
            cache.latest(another, () -> Assertions.fail("latest pin evicted"), version -> {
                reloads.incrementAndGet();
                return snapshot;
            });
            Assertions.assertEquals(1, reloads.get());
            Assertions.assertEquals(1, owner.entries().get("deltalake-snapshot").size());
        }
    }

    @Test
    void concurrentScansOfOneCachedCheckpointSnapshotHaveIndependentIterators() throws Exception {
        Path fixture = Path.of(getClass().getResource("/deltalake/dv-partitioned-with-checkpoint").toURI());
        ExecutorService executor = Executors.newFixedThreadPool(8);
        CyclicBarrier opened = new CyclicBarrier(8);
        try (DeltaLakeFileIO io = new DeltaLakeFileIO(new Configuration(), new RecordingConnectorContext());
                CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            Engine sharedEngine = DefaultEngine.create(io);
            DeltaLakeSnapshotCache cache = new DeltaLakeSnapshotCache(owner, Map.of());
            Snapshot cached = cache.latest(key, () -> TableManager.loadSnapshot(fixture.toString()).build(sharedEngine),
                    version -> Assertions.fail("unexpected reload"));
            List<Future<?>> scans = new ArrayList<>();
            for (int worker = 0; worker < 8; worker++) {
                boolean earlyClose = worker % 2 == 0;
                scans.add(executor.submit(() -> {
                    for (int iteration = 0; iteration < 3; iteration++) {
                        Snapshot shared = cache.latest(key, () -> Assertions.fail("latest missed"),
                                version -> Assertions.fail("snapshot missed"));
                        Assertions.assertSame(cached, shared);
                        try (CloseableIterator<Row> files = DeltaLakeScanFiles.open(
                                shared, sharedEngine, !earlyClose, Optional.empty())) {
                            Assertions.assertTrue(files.hasNext());
                            files.next();
                            // All scans have opened and consumed a row before half close their own iterators.
                            opened.await(10, TimeUnit.SECONDS);
                            if (!earlyClose) {
                                int count = 1;
                                while (files.hasNext()) {
                                    files.next();
                                    count++;
                                }
                                Assertions.assertEquals(15, count);
                            }
                        }
                        Assertions.assertEquals(35,
                                DeltaLakeStatisticsCache.collect(shared, sharedEngine).tableStatistics().getRowCount());
                    }
                    return null;
                }));
            }
            for (Future<?> scan : scans) {
                scan.get(30, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private Snapshot snapshot(long version) throws Exception {
        DeltaLakeTestTables.commit(table, version, "cached-table", new StructType().add("id", LongType.LONG));
        return TableManager.loadSnapshot(table.toString()).atVersion(version).build(engine);
    }
}
