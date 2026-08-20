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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.cache.ConnectorMetadataCache;
import org.apache.doris.connector.cache.ConnectorTableKey;
import org.apache.doris.connector.spi.ConnectorPartitionInfo;

import org.apache.paimon.catalog.CachingCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * Tests PaimonConnector's FIX-4 cache knobs (CI 973411): the {@code meta.cache.paimon.table.ttl-second}
 * mapping to the generic schema-cache TTL override (Axis B). The data-snapshot cache itself is covered by
 * {@link PaimonLatestSnapshotCacheTest}; the end-to-end behavior is gated by the docker e2e.
 */
public class PaimonConnectorCacheTest {

    private static PaimonConnector connector(Map<String, String> props) {
        return new PaimonConnector(props, new RecordingConnectorContext());
    }

    /** Like {@link #connector}, but also hands back the recording context so a test can assert on it. */
    private static PaimonConnector connectorWithContext(Map<String, String> props, RecordingConnectorContext ctx) {
        return new PaimonConnector(props, ctx);
    }

    private static Map<String, String> props(String ttl) {
        Map<String, String> m = new HashMap<>();
        if (ttl != null) {
            m.put(PaimonConnector.TABLE_CACHE_TTL_SECOND, ttl);
        }
        return m;
    }

    @Test
    public void schemaTtlOverrideAbsentWhenPropertyUnset() {
        // No meta.cache.paimon.table.ttl-second -> no override -> the catalog keeps the engine-default schema
        // cache TTL (the with-cache catalog: schema is cached). MUTATION: returning a value -> red.
        Assertions.assertEquals(OptionalLong.empty(),
                connector(Collections.emptyMap()).schemaCacheTtlSecondOverride());
    }

    @Test
    public void schemaTtlOverrideZeroDisablesSchemaCache() {
        // The no-cache catalog (meta.cache.paimon.table.ttl-second=0) must drive schema.cache.ttl-second=0 so
        // its schema is served FRESH (Test 2 / L112 of test_paimon_table_meta_cache). MUTATION: not mapping
        // ttl-second -> the no-cache catalog would serve stale schema -> red.
        Assertions.assertEquals(OptionalLong.of(0L), connector(props("0")).schemaCacheTtlSecondOverride());
    }

    @Test
    public void schemaTtlOverridePositiveIsPassedThrough() {
        Assertions.assertEquals(OptionalLong.of(3600L), connector(props("3600")).schemaCacheTtlSecondOverride());
    }

    @Test
    public void schemaTtlOverrideIgnoresUnparseableValue() {
        // A malformed value must not break catalog schema caching; fall back to no override (engine default).
        Assertions.assertEquals(OptionalLong.empty(), connector(props("not-a-number")).schemaCacheTtlSecondOverride());
    }

    @Test
    public void invalidateHooksAreNoThrowOnFreshConnector() {
        // Smoke: the REFRESH TABLE / REFRESH DATABASE / REFRESH CATALOG hooks must be safe on a fresh connector
        // (they only touch the connector-internal latest-snapshot cache + schema memo; the actual db-scoped
        // invalidate semantics are in PaimonLatestSnapshotCacheTest / PaimonSchemaAtMemoTest). invalidateDb
        // wires BOTH caches — a mutation dropping the schemaAtMemo half still passes this smoke but fails those.
        // MUTATION: an NPE on an empty cache -> red.
        PaimonConnector connector = connector(Collections.emptyMap());
        Assertions.assertDoesNotThrow(() -> connector.invalidateTable("db1", "t1"));
        Assertions.assertDoesNotThrow(() -> connector.invalidateDb("db1"));
        Assertions.assertDoesNotThrow(connector::invalidateAll);
    }

    // ============ PERF-06: derived partition-view cache A (no session=user gate) + invalidation ============

    @Test
    public void partitionViewCacheAlwaysBuilt() {
        // Unlike iceberg, paimon has NO session=user / per-user credential-isolation cache-disabling
        // convention (a paimon catalog authenticates at catalog-creation time, not per-query session
        // identity), so the connector must construct cache A unconditionally on every flavor. MUTATION: a
        // stray session-like gate leaving the field null on some property combination -> red.
        Assertions.assertNotNull(connector(Collections.emptyMap()).partitionViewCacheForTest(),
                "a fresh paimon connector must always build the partition-view cache");
        Map<String, String> withTtl = props("3600");
        Assertions.assertNotNull(connector(withTtl).partitionViewCacheForTest(),
                "the partition-view cache is independent of meta.cache.paimon.table.ttl-second");
    }

    @Test
    public void refreshHooksInvalidatePartitionViewCache() {
        // The REFRESH hooks must clear cache A too (else external DDL/writes stay invisible beyond the TTL):
        // REFRESH TABLE drops that table's snapshot entries, REFRESH DATABASE that db's, REFRESH CATALOG
        // everything. Asserted via a counting loader (the framework's size() is package-private): after
        // invalidation the loader must run again. MUTATION: an invalidate* hook not routed to the view cache
        // -> the entry survives -> loader not re-run -> a loads assert below red.
        PaimonConnector connector = connector(Collections.emptyMap());
        ConnectorMetadataCache<List<ConnectorPartitionInfo>> cache = connector.partitionViewCacheForTest();
        Assertions.assertNotNull(cache);
        int[] loads = {0};
        Supplier<List<ConnectorPartitionInfo>> loader = () -> {
            loads[0]++;
            return Collections.emptyList();
        };
        ConnectorTableKey db1t1 = new ConnectorTableKey("db1", "t1", 1L, -1L);
        ConnectorTableKey db1t2 = new ConnectorTableKey("db1", "t2", 1L, -1L);
        ConnectorTableKey db2t1 = new ConnectorTableKey("db2", "t1", 1L, -1L);

        // REFRESH TABLE db1.t1 -> only db1.t1 re-loads.
        cache.get(db1t1, loader);
        cache.get(db1t1, loader);
        Assertions.assertEquals(1, loads[0], "second get is a hit");
        connector.invalidateTable("db1", "t1");
        cache.get(db1t1, loader);
        Assertions.assertEquals(2, loads[0], "REFRESH TABLE forces a reload of db1.t1");

        // REFRESH DATABASE db1 -> db1.t2 re-loads; db2.t1 unaffected.
        cache.get(db1t2, loader);   // loads=3 (miss)
        cache.get(db2t1, loader);   // loads=4 (miss)
        cache.get(db1t2, loader);   // hit
        cache.get(db2t1, loader);   // hit
        Assertions.assertEquals(4, loads[0]);
        connector.invalidateDb("db1");
        cache.get(db2t1, loader);   // db2 untouched -> hit
        Assertions.assertEquals(4, loads[0], "REFRESH DATABASE db1 must NOT drop db2's entries");
        cache.get(db1t2, loader);   // db1.t2 dropped -> miss
        Assertions.assertEquals(5, loads[0], "REFRESH DATABASE db1 drops db1's entries");

        // REFRESH CATALOG -> everything re-loads.
        connector.invalidateAll();
        cache.get(db2t1, loader);
        Assertions.assertEquals(6, loads[0], "REFRESH CATALOG drops everything");
    }

    // ============ paimon CachingCatalog eviction: the frozen-Table-after-out-of-band-ALTER fix ============

    @Test
    public void invalidateHooksEvictPaimonCachingCatalogFrozenTable(@TempDir java.nio.file.Path warehouse)
            throws Exception {
        // A real FileSystemCatalog wrapped by paimon's CachingCatalog (the CatalogFactory default), plus a
        // SECOND bare catalog over the same warehouse modelling the other side of the production bug: an
        // ALTER COLUMN executed on ANOTHER FE (DDL forwards to master) or by an external engine. The
        // mutation bypasses THIS CachingCatalog instance, so its alterTable self-invalidation never fires
        // and getTable keeps serving the frozen pre-ALTER FileStoreTable — which the scan path serializes
        // to the BE, where PaimonJniScanner fails with "jni reader fields' size {N} is not matched with
        // paimon fields' size {M}" on every merged-read split until the (default 24h) access-TTL.
        try (Catalog inner = new FileSystemCatalog(LocalFileIO.create(),
                        new org.apache.paimon.fs.Path(warehouse.toUri()));
                Catalog outOfBand = new FileSystemCatalog(LocalFileIO.create(),
                        new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            Catalog caching = CachingCatalog.tryToCreate(inner, new Options());
            Assertions.assertTrue(caching instanceof CachingCatalog,
                    "precondition: the factory default wraps in CachingCatalog (cache.enabled)");
            caching.createDatabase("db1", false);
            Identifier id = Identifier.create("db1", "t1");
            caching.createTable(id, Schema.newBuilder().column("id", DataTypes.INT()).build(), false);
            Assertions.assertEquals(1, caching.getTable(id).rowType().getFieldCount());

            // Out-of-band ALTER: the caching instance still serves the frozen 1-column Table (the repro).
            outOfBand.alterTable(id, SchemaChange.addColumn("c2", DataTypes.INT()), false);
            Assertions.assertEquals(1, caching.getTable(id).rowType().getFieldCount(),
                    "precondition: an out-of-band ALTER leaves the CachingCatalog serving the frozen Table");

            PaimonConnector connector = connector(Collections.emptyMap());
            connector.setCatalogForTest(caching);

            // REFRESH TABLE / the PluginDrivenExternalCatalog DDL hook (both route through
            // connector.invalidateTable on every FE) must evict the frozen entry. MUTATION: dropping the
            // invalidatePaimonCatalogTable call -> getTable keeps returning 1 column -> red.
            connector.invalidateTable("db1", "t1");
            Assertions.assertEquals(2, caching.getTable(id).rowType().getFieldCount(),
                    "invalidateTable must evict the paimon CachingCatalog's frozen Table");

            // REFRESH DATABASE analogue.
            outOfBand.alterTable(id, SchemaChange.addColumn("c3", DataTypes.INT()), false);
            Assertions.assertEquals(2, caching.getTable(id).rowType().getFieldCount());
            connector.invalidateDb("db1");
            Assertions.assertEquals(3, caching.getTable(id).rowType().getFieldCount(),
                    "invalidateDb must evict the database's frozen Tables");

            // REFRESH CATALOG analogue.
            outOfBand.alterTable(id, SchemaChange.addColumn("c4", DataTypes.INT()), false);
            Assertions.assertEquals(3, caching.getTable(id).rowType().getFieldCount());
            connector.invalidateAll();
            Assertions.assertEquals(4, caching.getTable(id).rowType().getFieldCount(),
                    "invalidateAll must evict every frozen Table");
        }
    }

    // ============ EXTERNAL-CHANGE-POLL: background detection of an out-of-band (external-engine) ALTER ========

    @Test
    public void externalChangePollerDisabledByDefault() {
        // The poller is OFF unless meta.cache.paimon.external-change-poll-interval-second is positive, so the
        // default deployment keeps the prior event-only behavior (no background thread). A pollOnce() on a
        // never-built catalog is a no-op (nothing cached to probe). MUTATION: a stray default enabling it, or
        // pollOnce throwing on a null catalog -> red.
        PaimonConnector connector = connector(Collections.emptyMap());
        Assertions.assertFalse(connector.externalChangePollerForTest().isEnabled(),
                "the external-change poller must be disabled when the interval knob is unset");
        Assertions.assertDoesNotThrow(() -> connector.externalChangePollerForTest().pollOnce());

        // A positive interval enables it; a <= 0 / malformed value keeps it disabled (parity with the prior
        // behavior — a mistyped knob must not spin up a thread).
        Assertions.assertTrue(new PaimonConnector(pollProps("60"), new RecordingConnectorContext())
                .externalChangePollerForTest().isEnabled());
        Assertions.assertFalse(new PaimonConnector(pollProps("0"), new RecordingConnectorContext())
                .externalChangePollerForTest().isEnabled());
        Assertions.assertFalse(new PaimonConnector(pollProps("not-a-number"), new RecordingConnectorContext())
                .externalChangePollerForTest().isEnabled());
    }

    @Test
    public void externalChangePollerPollOnceEvictsFrozenTable(@TempDir java.nio.file.Path warehouse)
            throws Exception {
        // The core of the fix: a SINGLE synchronous poll (no invalidate call, no scheduler) must detect an
        // out-of-band ALTER and evict the CachingCatalog's frozen Table. Same setup as the invalidate-hooks
        // test (real FileSystemCatalog wrapped in CachingCatalog + a second bare catalog for the out-of-band
        // ALTER), but here NOTHING calls invalidateTable/Db/All — the poller is the only thing that heals.
        try (Catalog inner = new FileSystemCatalog(LocalFileIO.create(),
                        new org.apache.paimon.fs.Path(warehouse.toUri()));
                Catalog outOfBand = new FileSystemCatalog(LocalFileIO.create(),
                        new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            Catalog caching = CachingCatalog.tryToCreate(inner, new Options());
            caching.createDatabase("db1", false);
            Identifier id = Identifier.create("db1", "t1");
            caching.createTable(id, Schema.newBuilder().column("id", DataTypes.INT()).build(), false);
            // Prime the CachingCatalog so it holds a frozen 1-column Table (the poll target is the table cache's
            // key set; an unqueried table is not cached, so it would not be polled).
            Assertions.assertEquals(1, caching.getTable(id).rowType().getFieldCount());

            // Out-of-band ALTER via the second catalog: the caching instance's alterTable self-invalidation
            // never fires, so getTable keeps serving the frozen 1-column Table (the production repro).
            outOfBand.alterTable(id, SchemaChange.addColumn("c2", DataTypes.INT()), false);
            Assertions.assertEquals(1, caching.getTable(id).rowType().getFieldCount(),
                    "precondition: an out-of-band ALTER leaves the CachingCatalog serving the frozen Table");

            PaimonConnector connector = connector(pollProps("60"));
            connector.setCatalogForTest(caching);

            // ONE poll detects the schema-id drift (held=0 vs latest=1) and routes through invalidateTable to
            // evict. MUTATION: comparing rowType() instead of schema ids, or not evicting -> getTable stays at
            // 1 column -> red.
            connector.externalChangePollerForTest().pollOnce();
            Assertions.assertEquals(2, caching.getTable(id).rowType().getFieldCount(),
                    "a single background poll must detect the out-of-band ALTER and evict the frozen Table");

            // Idempotent: with no further external change, a second poll evicts nothing (the reloaded handle's
            // held id now equals the latest id). MUTATION: an unconditional evict every poll -> still 2, so this
            // only guards against a needless churn regression, asserted via a fresh reload staying stable.
            connector.externalChangePollerForTest().pollOnce();
            Assertions.assertEquals(2, caching.getTable(id).rowType().getFieldCount(),
                    "a poll with no external change must leave the freshly-loaded Table alone");
        }
    }

    @Test
    public void externalChangePollerAlsoNotifiesTheEngineNotJustTheConnector(
            @TempDir java.nio.file.Path warehouse) throws Exception {
        // The connector-side eviction (asserted above) only heals the BE-facing symptom (the JNI split
        // failure): the paimon CachingCatalog's frozen Table stops being served. But fe-core keeps its OWN
        // independent schema cache (ExtMetaCache), which nothing in the connector can reach — a query would
        // still bind against the stale column list even after a successful connector-side eviction. The
        // poller's eviction callback MUST therefore also call ConnectorContext#notifyExternalTableChanged (the
        // same fe-core-side refresh REFRESH TABLE triggers), or the user stays stuck reading a stale schema
        // despite the connector believing it already healed. MUTATION: wiring the poller to invalidateTable
        // alone (dropping the notifyExternalTableChanged half) -> notifications stays empty -> red.
        try (Catalog inner = new FileSystemCatalog(LocalFileIO.create(),
                        new org.apache.paimon.fs.Path(warehouse.toUri()));
                Catalog outOfBand = new FileSystemCatalog(LocalFileIO.create(),
                        new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            Catalog caching = CachingCatalog.tryToCreate(inner, new Options());
            caching.createDatabase("db1", false);
            Identifier id = Identifier.create("db1", "t1");
            caching.createTable(id, Schema.newBuilder().column("id", DataTypes.INT()).build(), false);
            caching.getTable(id); // prime the CachingCatalog's table cache (the poll target)

            outOfBand.alterTable(id, SchemaChange.addColumn("c2", DataTypes.INT()), false);

            RecordingConnectorContext ctx = new RecordingConnectorContext();
            PaimonConnector connector = connectorWithContext(pollProps("60"), ctx);
            connector.setCatalogForTest(caching);

            Assertions.assertTrue(ctx.externalTableChangeNotifications.isEmpty(),
                    "precondition: no notification before any poll ran");
            connector.externalChangePollerForTest().pollOnce();

            Assertions.assertEquals(1, ctx.externalTableChangeNotifications.size(),
                    "a detected out-of-band change must notify the engine (fe-core) exactly once, not just "
                            + "evict the connector's own CachingCatalog");
            Assertions.assertEquals("db1", ctx.externalTableChangeNotifications.get(0).getKey());
            Assertions.assertEquals("t1", ctx.externalTableChangeNotifications.get(0).getValue());
        }
    }

    @Test
    public void externalChangePollerBackgroundThreadEvicts(@TempDir java.nio.file.Path warehouse)
            throws Exception {
        // Complements the pollOnce() test: prove the SCHEDULED thread actually invokes the detection. Uses a
        // very short interval and a bounded await (never a fixed sleep) so the test is fast and stable.
        try (Catalog inner = new FileSystemCatalog(LocalFileIO.create(),
                        new org.apache.paimon.fs.Path(warehouse.toUri()));
                Catalog outOfBand = new FileSystemCatalog(LocalFileIO.create(),
                        new org.apache.paimon.fs.Path(warehouse.toUri()))) {
            Catalog caching = CachingCatalog.tryToCreate(inner, new Options());
            caching.createDatabase("db1", false);
            Identifier id = Identifier.create("db1", "t1");
            caching.createTable(id, Schema.newBuilder().column("id", DataTypes.INT()).build(), false);
            Assertions.assertEquals(1, caching.getTable(id).rowType().getFieldCount());

            // Poll every 1s (the smallest whole-second interval the knob accepts); start() schedules the thread.
            PaimonConnector connector = connector(pollProps("1"));
            connector.setCatalogForTest(caching);
            connector.externalChangePollerForTest().start();
            try {
                // Do the out-of-band ALTER AFTER start(): the thread must pick it up on a later tick.
                outOfBand.alterTable(id, SchemaChange.addColumn("c2", DataTypes.INT()), false);
                Assertions.assertTrue(
                        await(() -> fieldCount(caching, id) == 2, 10, TimeUnit.SECONDS),
                        "the scheduled poller thread must eventually detect the out-of-band ALTER and evict");
            } finally {
                connector.externalChangePollerForTest().close();
            }
        }
    }

    /** Reads {@code id}'s current cached-Table field count, adapting the checked getTable exception to
     * unchecked so it can be polled from a {@link BooleanSupplier}. */
    private static int fieldCount(Catalog catalog, Identifier id) {
        try {
            return catalog.getTable(id).rowType().getFieldCount();
        } catch (Catalog.TableNotExistException e) {
            throw new RuntimeException(e);
        }
    }

    /** Poll-interval-only property map (keeps the other cache knobs at their defaults). */
    private static Map<String, String> pollProps(String intervalSecond) {
        Map<String, String> m = new HashMap<>();
        m.put(PaimonConnector.EXTERNAL_CHANGE_POLL_INTERVAL_SECOND, intervalSecond);
        return m;
    }

    /** Busy-waits (10ms granularity) for {@code condition} up to {@code timeout}; true if it held in time. */
    private static boolean await(BooleanSupplier condition, long timeout, TimeUnit unit)
            throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(10);
        }
        return condition.getAsBoolean();
    }
}
