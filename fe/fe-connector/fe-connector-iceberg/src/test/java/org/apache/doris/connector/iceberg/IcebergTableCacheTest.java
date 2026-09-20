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

import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.cache.JvmSizeUtils;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.inmemory.InMemoryInputFile;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for {@link IcebergTableCache} (PERF-01). The cross-query RAW-table cache mirrors
 * {@link IcebergLatestSnapshotCache} exactly (same {@link org.apache.doris.connector.cache.MetaCache}
 * backing) but stores the whole {@link Table} instead of the {@code (snapshotId, schemaId)} pin, restoring the
 * table-caching half of the legacy {@code IcebergExternalMetaCache}. These tests cover the adapter's contract —
 * within-TTL stability, the {@code ttl <= 0} disable, invalidation, and the exception-propagation guarantee the
 * partition-view readers depend on. Timed-expiry mechanics are the framework's responsibility (unit-tested in
 * the framework module), so they are not re-proven here.
 */
public class IcebergTableCacheTest {

    private static TableIdentifier id() {
        return TableIdentifier.of("db", "t");
    }

    @Test
    public void v3DefaultsAndEncryptionKeysCountUnsampledPayloads() throws Exception {
        String large = "x".repeat(1024 * 1024);
        long stringGrowth = JvmSizeUtils.stringSize(large) - JvmSizeUtils.stringSize("x");
        for (String payload : new String[] {"initial-default", "write-default", "encryption-keys"}) {
            BaseTable baseline = tableWithV3Payload(payload, 998, "x");
            long baselineBytes = IcebergCacheSizeEstimator.estimateTable(baseline);
            for (int position : new int[] {0, 333, 998, 999}) {
                BaseTable table = tableWithV3Payload(payload, position, large);
                long growth = IcebergCacheSizeEstimator.estimateTable(table) - baselineBytes;
                // The sampled reflective maximum may overestimate a sampled outlier. The typed estimate
                // must also include an outlier outside that sample, rather than returning the small baseline.
                Assertions.assertTrue(growth > stringGrowth / 2L,
                        payload + " must count the retained payload at position " + position + ": " + growth);
            }

            BaseTable table = tableWithV3Payload(payload, 998, large);
            String json = TableMetadataParser.toJson(table.operations().current());
            long maxWeight = baselineBytes + IcebergCacheSizeEstimator.estimateSerializedTableMetadata(json)
                    + stringGrowth / 2L;
            AtomicInteger loads = new AtomicInteger();
            try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
                IcebergTableCache cache = new IcebergTableCache(owner,
                        CacheSpec.ofWeight(true, 100L, 1000L, maxWeight),
                        ignored -> () -> { }, new IcebergCatalogResourceTracker());
                for (int i = 0; i < 2; i++) {
                    cache.getOrLoad(id(), () -> {
                        loads.incrementAndGet();
                        return table;
                    });
                }
                Assertions.assertEquals(2, loads.get(), payload + " must include raw metadata as well as JSON");
                Assertions.assertEquals(0, cache.size());
            }
        }
    }

    private static BaseTable tableWithV3Payload(String payload, int position, String value) throws Exception {
        TableMetadata base = TableMetadata.newTableMetadata(
                new Schema(Types.NestedField.optional(1, "id", Types.StringType.get())),
                PartitionSpec.unpartitioned(), "file:///tmp/v3-table",
                Collections.singletonMap(TableProperties.FORMAT_VERSION, "3"));
        ObjectNode json = (ObjectNode) JsonUtil.mapper().readTree(TableMetadataParser.toJson(base));
        if (payload.equals("encryption-keys")) {
            ArrayNode keys = json.putArray("encryption-keys");
            for (int i = 0; i < 1000; i++) {
                ObjectNode key = keys.addObject();
                key.put("key-id", "k" + i);
                key.put("encrypted-key-metadata", "AA==");
                key.putObject("properties").put("p", i == position ? value : "x");
            }
        } else {
            ArrayNode fields = ((ObjectNode) json.get("schemas").get(0)).putArray("fields");
            for (int i = 0; i < 1000; i++) {
                ObjectNode field = fields.addObject();
                field.put("id", i + 1);
                field.put("name", "c" + i);
                field.put("required", false);
                field.put("type", "string");
                field.put(payload, i == position ? value : "x");
            }
            json.put("last-column-id", 1000);
        }
        TableMetadata metadata = TableMetadataParser.fromJson("file:///tmp/v3.metadata.json", json.toString());
        return new BaseTable(new StaticTableOperations(metadata), "weighted-v3");
    }

    /** A distinct fake table, distinguishable by {@link Table#name()}. */
    private static Table table(String name) {
        return new FakeIcebergTable(name,
                new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())),
                PartitionSpec.unpartitioned(), "s3://b/" + name, Collections.emptyMap());
    }

    @Test
    public void cachesWithinTtlAndServesTheSameTable() {
        AtomicInteger loads = new AtomicInteger();
        IcebergTableCache c = new IcebergTableCache(100, 1000);

        Table first = c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return table("first");
        });
        // Second read within TTL must return the CACHED table (first), NOT a freshly-loaded one -> this is what
        // lets consecutive queries (and one query's analysis/planning phases) reuse a single load. MUTATION:
        // loading live every call -> returns "second" / loads==2 -> red.
        Table second = c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return table("second");
        });
        Assertions.assertEquals("first", first.name());
        Assertions.assertSame(first, second, "within TTL the cached table instance must be served");
        Assertions.assertEquals(1, loads.get(), "the live loader must run exactly once within TTL");
        Assertions.assertTrue(c.isEnabled());
    }

    @Test
    public void weightBoundedTableIsEstimatedAndCached() {
        AtomicInteger loads = new AtomicInteger();
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            IcebergTableCache cache = new IcebergTableCache(
                    owner, CacheSpec.ofWeight(true, 100L, 1000L, 1024L * 1024L),
                    ignored -> () -> {
                    }, new IcebergCatalogResourceTracker());

            Table first = cache.getOrLoad(id(), () -> {
                loads.incrementAndGet();
                return table("first");
            });
            Table second = cache.getOrLoad(id(), () -> {
                loads.incrementAndGet();
                return table("second");
            });

            Assertions.assertSame(first, second);
            Assertions.assertEquals(1, loads.get());
        }
    }

    @Test
    public void weightedBorrowUsesAnIndependentSnapshotGenerationPerStatement() {
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            IcebergTableCache cache = new IcebergTableCache(
                    owner, CacheSpec.ofWeight(true, 100L, 1000L, 10L * 1024L * 1024L),
                    ignored -> () -> {
                    }, new IcebergCatalogResourceTracker());

            try (IcebergTableCache.TableLease lease = cache.borrow(id(),
                    IcebergTableCacheTest::tableWithSnapshot)) {
                BaseTable cached = (BaseTable) lease.table();
                BaseTable firstStatement = (BaseTable) lease.snapshotReadTable();
                BaseTable secondStatement = (BaseTable) lease.snapshotReadTable();

                Snapshot cachedSnapshot = cached.operations().current().currentSnapshot();
                Snapshot firstSnapshot = firstStatement.operations().current().currentSnapshot();
                Snapshot secondSnapshot = secondStatement.operations().current().currentSnapshot();
                Assertions.assertNotSame(cachedSnapshot, firstSnapshot,
                        "manifest lazy fields must not be written into the weighted cache generation");
                Assertions.assertNotSame(firstSnapshot, secondSnapshot,
                        "each statement must own its snapshot lazy-loading state");
                Assertions.assertEquals(cachedSnapshot.snapshotId(), firstSnapshot.snapshotId());
            }
            Assertions.assertEquals(1, cache.size());
        }
    }

    @Test
    public void disabledBoundedCacheDoesNotPrepareSnapshotGeneration() {
        for (CacheSpec spec : new CacheSpec[] {
                CacheSpec.ofWeight(false, 100L, 1000L, 1024L * 1024L),
                CacheSpec.ofWeight(true, 0L, 1000L, 1024L * 1024L)}) {
            AtomicInteger cleanups = new AtomicInteger();
            try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
                IcebergTableCache cache = new IcebergTableCache(owner, spec,
                        ignored -> cleanups::incrementAndGet, new IcebergCatalogResourceTracker());
                Table table = tableWithSnapshot();
                try (IcebergTableCache.TableLease lease = cache.borrow(id(), () -> table)) {
                    Assertions.assertSame(((BaseTable) table).operations().current(),
                            ((BaseTable) lease.snapshotReadTable()).operations().current(),
                            "an uncached table must not serialize and reparse the metadata generation");
                    Assertions.assertEquals(0, cache.size());
                    Assertions.assertEquals(0, cleanups.get());
                }
                Assertions.assertEquals(1, cleanups.get());
            }
        }
    }

    @Test
    public void weightedV1TableIsMeasuredAfterSerializationMaterializesEmbeddedManifests() {
        Table probe = tableWithV1EmbeddedManifests(256);
        long beforeSerialization = IcebergCacheSizeEstimator.estimateTable(probe);
        String metadataJson = TableMetadataParser.toJson(((BaseTable) probe).operations().current());
        long afterSerialization = IcebergCacheSizeEstimator.estimateTable(probe);
        long materializedBytes = afterSerialization - beforeSerialization;
        Assertions.assertTrue(materializedBytes > 8192L,
                "the fixture must materialize enough v1 manifest state to distinguish the two weigh orders");

        long oldOrderPayload = beforeSerialization
                + IcebergCacheSizeEstimator.estimateSerializedTableMetadata(metadataJson);
        long maxWeight = oldOrderPayload + materializedBytes / 2L;
        AtomicInteger loads = new AtomicInteger();
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            IcebergTableCache cache = new IcebergTableCache(
                    owner, CacheSpec.ofWeight(true, 100L, 1000L, maxWeight),
                    ignored -> () -> { }, new IcebergCatalogResourceTracker());

            cache.getOrLoad(id(), () -> {
                loads.incrementAndGet();
                return tableWithV1EmbeddedManifests(256);
            });
            cache.getOrLoad(id(), () -> {
                loads.incrementAndGet();
                return tableWithV1EmbeddedManifests(256);
            });

            Assertions.assertEquals(2, loads.get(),
                    "the post-serialization retained graph exceeds the budget and must not be cached");
            Assertions.assertEquals(0, cache.size());
        }
    }

    @Test
    public void weightedV1HistoricalSnapshotCannotHideEmbeddedManifests() throws Exception {
        BaseTable small = tableWithV1History(16, 14, 1);
        TableMetadataParser.toJson(small.operations().current());
        long smallBytes = IcebergCacheSizeEstimator.estimateTable(small);
        BaseTable large = tableWithV1History(16, 14, 1000);
        String largeJson = TableMetadataParser.toJson(large.operations().current());
        long maxWeight = smallBytes + IcebergCacheSizeEstimator.estimateSerializedTableMetadata(largeJson) + 16384L;
        Assertions.assertTrue(IcebergCacheSizeEstimator.estimateTable(large) - smallBytes > 16384L,
                "raw embedded manifests at historical index 14 must be counted independently of JSON");
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            IcebergTableCache cache = new IcebergTableCache(owner,
                    CacheSpec.ofWeight(true, 100L, 1000L, maxWeight),
                    ignored -> () -> { }, new IcebergCatalogResourceTracker());
            AtomicInteger loads = new AtomicInteger();
            for (int i = 0; i < 2; i++) {
                cache.getOrLoad(id(), () -> {
                    loads.incrementAndGet();
                    return large;
                });
            }
            Assertions.assertEquals(2, loads.get());
            Assertions.assertEquals(0, cache.size());
            cache.getOrLoad(id(), () -> small);
            Assertions.assertEquals(1, cache.size(), "ordinary v1 history still fits and must be cached");
        }
    }

    @Test
    public void excessiveV1ManifestTraversalDeclinesRetentionWithoutFailingManifestResolution() throws Exception {
        BaseTable table = tableWithV1History(100, 98, 2000);
        InputFile input = new InMemoryInputFile("file:///tmp/manifest-99-0.avro", new byte[512]);
        FileIO io = new FileIO() {
            @Override
            public InputFile newInputFile(String path) {
                Assertions.assertEquals(input.location(), path);
                return input;
            }

            @Override
            public OutputFile newOutputFile(String path) {
                throw new UnsupportedOperationException("read only");
            }

            @Override
            public void deleteFile(String path) {
                throw new UnsupportedOperationException("read only");
            }
        };
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            IcebergTableCache cache = new IcebergTableCache(owner,
                    CacheSpec.ofWeight(true, 100L, 1000L, 128L * 1024L * 1024L),
                    ignored -> () -> { }, new IcebergCatalogResourceTracker());
            AtomicInteger loads = new AtomicInteger();
            for (int i = 0; i < 2; i++) {
                try (IcebergTableCache.TableLease lease = cache.borrow(id(), () -> {
                    loads.incrementAndGet();
                    return table;
                })) {
                    Assertions.assertSame(table, lease.table());
                    Table statement = lease.snapshotReadTable();
                    ManifestFile manifest = statement.currentSnapshot().dataManifests(io).get(0);
                    // Use FileIO's real ManifestFile overload: it resolves the lazy length as scan planning does.
                    Assertions.assertSame(input, io.newInputFile(manifest));
                    Assertions.assertEquals(512L, manifest.length());
                }
            }
            Assertions.assertEquals(2, loads.get());
            Assertions.assertEquals(0, cache.size());
        }
    }

    private static BaseTable tableWithV1History(int snapshotCount, int tailIndex, int tailManifests) throws Exception {
        BaseTable seed = (BaseTable) tableWithV1EmbeddedManifests(1);
        ObjectNode json = (ObjectNode) JsonUtil.mapper().readTree(
                TableMetadataParser.toJson(seed.operations().current()));
        ArrayNode snapshots = json.putArray("snapshots");
        for (int i = 0; i < snapshotCount; i++) {
            ObjectNode snapshot = snapshots.addObject();
            snapshot.put("snapshot-id", i + 1);
            snapshot.put("timestamp-ms", 1000 + i);
            snapshot.put("schema-id", 0);
            snapshot.putObject("summary").put("operation", "append");
            ArrayNode manifests = snapshot.putArray("manifests");
            for (int j = 0; j < (i == tailIndex ? tailManifests : 1); j++) {
                manifests.add("file:///tmp/manifest-" + i + "-" + j + ".avro");
            }
        }
        json.put("current-snapshot-id", snapshotCount);
        json.put("last-updated-ms", 1000 + snapshotCount);
        json.remove("refs");
        json.putArray("snapshot-log");
        return new BaseTable(new StaticTableOperations(TableMetadataParser.fromJson(
                "file:///tmp/v1-history.metadata.json", json.toString())), "weighted-v1-history");
    }

    @Test
    public void weightedBorrowSupportsMetadataWithoutAFileLocation() {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        TableMetadata metadata = TableMetadata.newTableMetadata(
                schema, PartitionSpec.unpartitioned(), "file:///tmp/no-metadata-location", Collections.emptyMap());
        Table table = new BaseTable(new StaticTableOperations(metadata), "no-metadata-location");

        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            IcebergTableCache cache = new IcebergTableCache(
                    owner, CacheSpec.ofWeight(true, 100L, 1000L, 10L * 1024L * 1024L),
                    ignored -> () -> { }, new IcebergCatalogResourceTracker());
            try (IcebergTableCache.TableLease lease = cache.borrow(id(), () -> table)) {
                BaseTable statement = (BaseTable) lease.snapshotReadTable();
                Assertions.assertNull(statement.operations().current().metadataFileLocation());
                Assertions.assertEquals(schema.asStruct(), statement.schema().asStruct());
            }
            Assertions.assertEquals(1, cache.size());
        }
    }

    @Test
    public void ttlZeroDisablesCachingAlwaysLive() {
        AtomicInteger loads = new AtomicInteger();
        IcebergTableCache c = new IcebergTableCache(0, 1000);
        c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return table("first");
        });
        Table second = c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return table("second");
        });
        // ttl-second=0 (the no-cache catalog) reads live every time. MUTATION: caching despite ttl<=0 ->
        // second=="first" / loads==1 -> red.
        Assertions.assertEquals("second", second.name(), "ttl-second=0 must always read the live table");
        Assertions.assertEquals(2, loads.get());
        Assertions.assertFalse(c.isEnabled());
        Assertions.assertEquals(0, c.size(), "ttl-second=0 must not store anything");
    }

    @Test
    public void negativeTtlDisablesCachingAlwaysLive() {
        // ttl-second=-1 (or any negative) is still the no-cache catalog. Guards the CacheSpec trap where
        // ttl == -1 means "no expiration (enabled)": the adapter must translate "<= 0" to disabled.
        AtomicInteger loads = new AtomicInteger();
        IcebergTableCache c = new IcebergTableCache(-1, 1000);
        c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return table("first");
        });
        Table second = c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return table("second");
        });
        Assertions.assertEquals("second", second.name(), "ttl-second=-1 must always read the live table");
        Assertions.assertEquals(2, loads.get());
        Assertions.assertFalse(c.isEnabled());
    }

    @Test
    public void invalidateWaitsForActiveBorrowerBeforeCleaning() {
        AtomicInteger cleanerCalls = new AtomicInteger();
        IcebergTableCache c = new IcebergTableCache(100, 1000, table -> cleanerCalls::incrementAndGet);
        IcebergTableCache.TableLease lease = c.borrow(id(), () -> table("first"));
        c.invalidate(id());
        Assertions.assertEquals(0, cleanerCalls.get(), "invalidation must not close an active statement table");
        Assertions.assertEquals("first", lease.table().name());
        lease.close();
        Assertions.assertEquals(1, cleanerCalls.get(), "removing a cached table must release its resources");
    }

    @Test
    public void disabledCacheCleansAfterBorrowerRelease() {
        AtomicInteger cleanerCalls = new AtomicInteger();
        IcebergTableCache c = new IcebergTableCache(0, 1000, table -> cleanerCalls::incrementAndGet);
        IcebergTableCache.TableLease lease = c.borrow(id(), () -> table("uncached"));
        Assertions.assertEquals(0, c.size());
        Assertions.assertEquals(0, cleanerCalls.get(), "the returned uncached table is still borrowed");
        lease.close();
        Assertions.assertEquals(1, cleanerCalls.get());
    }

    @Test
    public void capacityEvictionWaitsForActiveBorrower() {
        AtomicInteger cleanerCalls = new AtomicInteger();
        IcebergTableCache c = new IcebergTableCache(100, 1, table -> cleanerCalls::incrementAndGet);
        IcebergTableCache.TableLease first = c.borrow(
                TableIdentifier.of("db", "first"), () -> table("first"));
        IcebergTableCache.TableLease second = c.borrow(
                TableIdentifier.of("db", "second"), () -> table("second"));
        Assertions.assertEquals(0, cleanerCalls.get(), "an evicted active table must remain usable");
        Assertions.assertEquals("first", first.table().name());
        Assertions.assertEquals("second", second.table().name());
        first.close();
        second.close();
        c.invalidateAll();
        Assertions.assertEquals(2, cleanerCalls.get());
    }

    @Test
    public void connectorCloseWaitsForActiveBorrowerBeforeClosingCatalogResources() {
        AtomicInteger tableCleanerCalls = new AtomicInteger();
        AtomicInteger catalogCleanerCalls = new AtomicInteger();
        IcebergCatalogResourceTracker tracker = new IcebergCatalogResourceTracker();
        IcebergTableCache c = new IcebergTableCache(
                100, 1000, table -> tableCleanerCalls::incrementAndGet, tracker);

        IcebergTableCache.TableLease lease = c.borrow(id(), () -> table("first"));
        c.invalidateAll();
        tracker.close(catalogCleanerCalls::incrementAndGet);

        Assertions.assertEquals(0, tableCleanerCalls.get(),
                "cache invalidation must not clean a table still borrowed by a statement");
        Assertions.assertEquals(0, catalogCleanerCalls.get(),
                "connector close must not close catalog resources used by that table");
        Assertions.assertEquals("first", lease.table().name());

        lease.close();
        Assertions.assertEquals(1, tableCleanerCalls.get());
        Assertions.assertEquals(1, catalogCleanerCalls.get());
    }

    @Test
    public void closeRejectsAndCleansLoaderThatPublishesLate() throws Exception {
        AtomicInteger tableCleanerCalls = new AtomicInteger();
        AtomicInteger catalogCleanerCalls = new AtomicInteger();
        CountDownLatch loaderEntered = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        IcebergCatalogResourceTracker tracker = new IcebergCatalogResourceTracker();
        IcebergTableCache cache = new IcebergTableCache(
                100, 1000, table -> tableCleanerCalls::incrementAndGet, tracker);

        CompletableFuture<Void> load = CompletableFuture.runAsync(() -> Assertions.assertThrows(
                IllegalStateException.class, () -> cache.borrow(id(), () -> {
                    loaderEntered.countDown();
                    try {
                        Assertions.assertTrue(releaseLoader.await(10, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    }
                    return table("late");
                })));

        Assertions.assertTrue(loaderEntered.await(10, TimeUnit.SECONDS));
        cache.close();
        tracker.close(catalogCleanerCalls::incrementAndGet);
        releaseLoader.countDown();
        load.get(10, TimeUnit.SECONDS);

        Assertions.assertEquals(0, cache.size(), "a post-close publication must not remain cache-owned");
        Assertions.assertEquals(1, tableCleanerCalls.get());
        Assertions.assertEquals(1, catalogCleanerCalls.get());
    }

    @Test
    public void invalidateForcesReload() {
        AtomicInteger loads = new AtomicInteger();
        IcebergTableCache c = new IcebergTableCache(100, 1000);
        c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return table("first");
        });
        c.invalidate(id());
        // After REFRESH TABLE invalidation the next read goes live. MUTATION: invalidate not clearing ->
        // returns cached "first" / loads==1 -> red.
        Table after = c.getOrLoad(id(), () -> {
            loads.incrementAndGet();
            return table("second");
        });
        Assertions.assertEquals("second", after.name());
        Assertions.assertEquals(2, loads.get());
    }

    @Test
    public void invalidateAllClearsEverything() {
        IcebergTableCache c = new IcebergTableCache(100, 1000);
        c.getOrLoad(TableIdentifier.of("db", "t1"), () -> table("t1"));
        c.getOrLoad(TableIdentifier.of("db", "t2"), () -> table("t2"));
        Assertions.assertEquals(2, c.size());
        c.invalidateAll();
        Assertions.assertEquals(0, c.size());
    }

    @Test
    public void invalidateDbClearsOnlyThatDbsTables() {
        AtomicInteger loads = new AtomicInteger();
        IcebergTableCache c = new IcebergTableCache(100, 1000);
        c.getOrLoad(TableIdentifier.of("db1", "t1"), () -> table("db1.t1"));
        c.getOrLoad(TableIdentifier.of("db1", "t2"), () -> table("db1.t2"));
        c.getOrLoad(TableIdentifier.of("db2", "t1"), () -> table("db2.t1"));
        Assertions.assertEquals(3, c.size());

        // REFRESH DATABASE db1 (or a Doris DROP DATABASE db1) must drop BOTH db1 tables and leave db2 intact.
        // MUTATION: invalidateDb a no-op -> db1.t1 still cached -> loads stays 0 / after=="db1.t1" -> red.
        c.invalidateDb("db1");
        Assertions.assertEquals(1, c.size(), "only db2's single entry must survive");

        Table afterDb1 = c.getOrLoad(TableIdentifier.of("db1", "t1"), () -> {
            loads.incrementAndGet();
            return table("db1.t1.reloaded");
        });
        Assertions.assertEquals("db1.t1.reloaded", afterDb1.name(), "db1.t1 must reload live after invalidateDb");
        Assertions.assertEquals(1, loads.get());

        Table db2 = c.getOrLoad(TableIdentifier.of("db2", "t1"), () -> {
            loads.incrementAndGet();
            return table("db2.t1.reloaded");
        });
        Assertions.assertEquals("db2.t1", db2.name(), "db2 must keep its cached table (not dropped by invalidateDb(db1))");
        Assertions.assertEquals(1, loads.get(), "db2 read must be a hit (no extra load)");
    }

    @Test
    public void loaderExceptionPropagatesUnwrapped() {
        // The partition-view readers (listPartitions / listPartitionNames) catch NoSuchTableException to degrade
        // a concurrent-drop race to an empty list. Routing them through this cache must NOT wrap that exception,
        // or the degradation would break and they'd throw instead. The MetaCache manual-miss-load path
        // re-throws the loader's RuntimeException verbatim. MUTATION: wrapping the loader exception ->
        // assertThrows(NoSuchTableException) fails (a different type is thrown) -> red.
        IcebergTableCache c = new IcebergTableCache(100, 1000);
        Assertions.assertThrows(NoSuchTableException.class, () -> c.getOrLoad(id(), () -> {
            throw new NoSuchTableException("simulated concurrent drop");
        }));
    }

    private static Table tableWithSnapshot() {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        TableMetadata base = TableMetadata.newTableMetadata(
                schema, PartitionSpec.unpartitioned(), "file:///tmp/weighted-table", Collections.emptyMap());
        Snapshot snapshot = SnapshotParser.fromJson("{"
                + "\"sequence-number\":1,"
                + "\"snapshot-id\":101,"
                + "\"timestamp-ms\":1000,"
                + "\"summary\":{\"operation\":\"append\"},"
                + "\"manifest-list\":\"file:///tmp/snap-101.avro\","
                + "\"schema-id\":0}");
        TableMetadata metadata = TableMetadata.buildFrom(base)
                .upgradeFormatVersion(2)
                .withMetadataLocation("file:///tmp/v2.metadata.json")
                .setBranchSnapshot(snapshot, "main")
                .discardChanges()
                .build();
        return new BaseTable(new StaticTableOperations(metadata), "weighted");
    }

    private static Table tableWithV1EmbeddedManifests(int manifestCount) {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        TableMetadata base = TableMetadata.newTableMetadata(
                schema, PartitionSpec.unpartitioned(), "file:///tmp/weighted-v1-table",
                Collections.singletonMap(TableProperties.FORMAT_VERSION, "1"));
        StringBuilder snapshotJson = new StringBuilder()
                .append("{\"snapshot-id\":101,\"timestamp-ms\":1000,")
                .append("\"summary\":{\"operation\":\"append\"},\"schema-id\":0,\"manifests\":[");
        for (int i = 0; i < manifestCount; i++) {
            if (i > 0) {
                snapshotJson.append(',');
            }
            snapshotJson.append("\"file:///tmp/manifest-").append(i).append(".avro\"");
        }
        snapshotJson.append("]}");
        Snapshot snapshot = SnapshotParser.fromJson(snapshotJson.toString());
        TableMetadata metadata = TableMetadata.buildFrom(base)
                .withMetadataLocation("file:///tmp/v1.metadata.json")
                .setBranchSnapshot(snapshot, "main")
                .discardChanges()
                .build();
        return new BaseTable(new StaticTableOperations(metadata), "weighted-v1");
    }

    private static final class StaticTableOperations implements TableOperations {
        private final TableMetadata metadata;

        private StaticTableOperations(TableMetadata metadata) {
            this.metadata = metadata;
        }

        @Override
        public TableMetadata current() {
            return metadata;
        }

        @Override
        public TableMetadata refresh() {
            return metadata;
        }

        @Override
        public void commit(TableMetadata base, TableMetadata updated) {
            throw new UnsupportedOperationException("read only");
        }

        @Override
        public FileIO io() {
            return null;
        }

        @Override
        public EncryptionManager encryption() {
            return null;
        }

        @Override
        public String metadataFileLocation(String fileName) {
            return "file:///tmp/" + fileName;
        }

        @Override
        public LocationProvider locationProvider() {
            return null;
        }
    }
}
