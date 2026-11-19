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

import org.apache.doris.connector.cache.CatalogMetaCache;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class IcebergEncryptedCacheTest {
    @Test
    void encryptedManifestWithPlaintextFilesCannotBypassTheCurrentDescriptor() {
        try (EncryptedManifestFixture fixture = new EncryptedManifestFixture();
                CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            Table table = fixture.queryTable();
            ManifestFile manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
            Assertions.assertNotNull(manifest.keyMetadata());
            IcebergManifestCache cache = new IcebergManifestCache(owner, Collections.emptyMap());
            ManifestCacheValue first = cache.getManifestCacheValue(manifest, table, "warm");
            Assertions.assertEquals(1, first.getDataFiles().size());
            Assertions.assertNull(first.getDataFiles().get(0).keyMetadata());
            Assertions.assertNull(cache.getCachedManifestValue(manifest, "next"),
                    "decrypted metadata must not replace this caller's manifest key validation");
            for (ByteBuffer invalid : new ByteBuffer[] {null, ByteBuffer.wrap(new byte[] {1, 2, 3})}) {
                ManifestFile rejected = withKeyMetadata(manifest, invalid);
                Assertions.assertThrows(RuntimeException.class, () -> cache.getManifestCacheValue(rejected, table));
                Assertions.assertThrows(RuntimeException.class, () -> cache.getManifestCacheValueFuture(
                        rejected, table, "retry").toCompletableFuture().join());
            }
            Assertions.assertEquals(1, cache.getManifestCacheValue(manifest, table).getDataFiles().size());
            Assertions.assertEquals(0, cache.size());
        }
    }

    @Test
    void encryptedStatementSnapshotDoesNotInheritTheSdkManifestByteCache() {
        try (EncryptedManifestFixture fixture = new EncryptedManifestFixture()) {
            Table raw = fixture.queryTable();
            IcebergTableCache cache = new IcebergTableCache(100, 10);
            try (IcebergTableCache.TableLease lease = cache.borrow(
                    TableIdentifier.of("db", "sdk-cache"), () -> raw)) {
                Table snapshot = lease.snapshotReadTable();
                Assertions.assertEquals("false", snapshot.io().properties()
                        .get(CatalogProperties.IO_MANIFEST_CACHE_ENABLED));
                Assertions.assertEquals("true", raw.io().properties()
                        .get(CatalogProperties.IO_MANIFEST_CACHE_ENABLED), "catalog policy must not be mutated");
                ManifestFile manifest = snapshot.currentSnapshot().dataManifests(snapshot.io()).get(0);
                try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, snapshot.io(), snapshot.specs())) {
                    Assertions.assertEquals(1, reader.iterator().next().recordCount());
                } catch (java.io.IOException failure) {
                    throw new java.io.UncheckedIOException(failure);
                }
            } finally {
                cache.close();
            }
        }
    }

    @Test
    void revokedKmsAfterWarmStatementCannotReuseTheSdkKeyCache() {
        AtomicInteger loads = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        TableIdentifier id = TableIdentifier.of("db", "encrypted");
        try (EncryptedManifestFixture fixture = new EncryptedManifestFixture()) {
            IcebergTableCache cache = new IcebergTableCache(100, 10, ignored -> closes::incrementAndGet, null);
            try {
                try (IcebergTableCache.TableLease lease = cache.borrow(id, () -> {
                    loads.incrementAndGet();
                    return fixture.queryTable();
                })) {
                    Table table = lease.snapshotReadTable();
                    Assertions.assertEquals(1, table.currentSnapshot().dataManifests(table.io()).size());
                }
                Assertions.assertTrue(fixture.unwraps.get() > 0);
                fixture.denied.set(true);
                try (IcebergTableCache.TableLease lease = cache.borrow(id, () -> {
                    loads.incrementAndGet();
                    return fixture.queryTable();
                })) {
                    Table table = lease.snapshotReadTable();
                    SecurityException failure = Assertions.assertThrows(SecurityException.class,
                            () -> table.currentSnapshot().dataManifests(table.io()));
                    Assertions.assertEquals("fixture KMS access revoked", failure.getMessage());
                }
                Assertions.assertEquals(2, loads.get());
                Assertions.assertEquals(2, closes.get());
                Assertions.assertEquals(0, cache.size());
            } finally {
                cache.close();
            }
        }
        Assertions.assertEquals(2, closes.get());
    }

    @Test
    void simultaneousStatementsOwnDistinctManagersUntilTheirLastLeaseCloses() throws Exception {
        AtomicInteger closes = new AtomicInteger();
        CountDownLatch loaders = new CountDownLatch(2);
        CountDownLatch acquired = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        Table[] tables = new Table[2];
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try (EncryptedManifestFixture fixture = new EncryptedManifestFixture()) {
            IcebergTableCache cache = new IcebergTableCache(100, 10, ignored -> closes::incrementAndGet, null);
            CompletableFuture<?>[] tasks = new CompletableFuture<?>[2];
            try {
                for (int i = 0; i < tasks.length; i++) {
                    final int slot = i;
                    tasks[i] = CompletableFuture.runAsync(() -> {
                        try (IcebergTableCache.TableLease lease = cache.borrow(
                                TableIdentifier.of("db", "concurrent"), () -> {
                                    loaders.countDown();
                                    await(loaders);
                                    return fixture.queryTable();
                                })) {
                            tables[slot] = lease.snapshotReadTable();
                            Assertions.assertEquals(1, tables[slot].currentSnapshot()
                                    .dataManifests(tables[slot].io()).size());
                            acquired.countDown();
                            await(release);
                        }
                    }, workers);
                }
                Assertions.assertTrue(acquired.await(10, TimeUnit.SECONDS),
                        "both encrypted statements must load independently instead of sharing in-flight keys");
                Assertions.assertNotSame(tables[0].encryption(), tables[1].encryption());
                Assertions.assertEquals(0, cache.size());
                cache.invalidateAll();
                cache.close();
                Assertions.assertEquals(0, closes.get(), "catalog retirement must preserve active statement leases");
            } finally {
                release.countDown();
                cache.close();
                CompletableFuture.allOf(tasks).get(10, TimeUnit.SECONDS);
            }
            Assertions.assertEquals(2, closes.get());
        } finally {
            release.countDown();
            workers.shutdownNow();
            Assertions.assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("fixture interrupted", interrupted);
        }
    }

    private static ManifestFile withKeyMetadata(ManifestFile delegate, ByteBuffer metadata) {
        return (ManifestFile) Proxy.newProxyInstance(ManifestFile.class.getClassLoader(),
                new Class<?>[] {ManifestFile.class}, (proxy, method, args) -> {
                    if (method.getName().equals("keyMetadata")) {
                        return metadata;
                    }
                    try {
                        return method.invoke(delegate, args);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                });
    }
}
