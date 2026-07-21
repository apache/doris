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

import org.apache.doris.connector.api.ConnectorPartitionInfo;
import org.apache.doris.connector.cache.ConnectorPartitionViewCache;
import org.apache.doris.connector.cache.PartitionViewCacheKey;

import org.apache.iceberg.DataFiles;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.function.Supplier;

/**
 * Tests IcebergConnector's T08 cache knobs: the latest-snapshot cache TTL resolution
 * ({@code meta.cache.iceberg.table.ttl-second}) and the REFRESH-TABLE invalidate hooks. The cache mechanics
 * themselves are covered by {@link IcebergLatestSnapshotCacheTest}; end-to-end behavior is gated by docker e2e.
 */
public class IcebergConnectorCacheTest {

    private static Map<String, String> props(String key, String value) {
        Map<String, String> m = new HashMap<>();
        if (value != null) {
            m.put(key, value);
        }
        return m;
    }

    @Test
    public void tableCacheTtlDefaultsTo24hWhenUnset() {
        // No meta.cache.iceberg.table.ttl-second -> the legacy with-cache catalog default (24h).
        // MUTATION: defaulting to 0 (no-cache) -> red.
        Assertions.assertEquals(IcebergConnector.DEFAULT_TABLE_CACHE_TTL_SECOND,
                IcebergConnector.resolveTableCacheTtlSecond(Collections.emptyMap()));
    }

    @Test
    public void tableCacheTtlZeroDisablesCaching() {
        // ttl-second=0 = the no-cache catalog (always read the latest snapshot live). MUTATION: not honoring 0
        // -> a write would not be seen until the default 24h TTL -> red.
        Assertions.assertEquals(0L,
                IcebergConnector.resolveTableCacheTtlSecond(props(IcebergConnector.TABLE_CACHE_TTL_SECOND, "0")));
    }

    @Test
    public void tableCacheTtlPositiveIsPassedThrough() {
        Assertions.assertEquals(3600L,
                IcebergConnector.resolveTableCacheTtlSecond(props(IcebergConnector.TABLE_CACHE_TTL_SECOND, "3600")));
    }

    @Test
    public void tableCacheTtlIgnoresUnparseableAndBlank() {
        // A malformed/blank value must not break catalog creation; fall back to the default.
        Assertions.assertEquals(IcebergConnector.DEFAULT_TABLE_CACHE_TTL_SECOND,
                IcebergConnector.resolveTableCacheTtlSecond(
                        props(IcebergConnector.TABLE_CACHE_TTL_SECOND, "not-a-number")));
        Assertions.assertEquals(IcebergConnector.DEFAULT_TABLE_CACHE_TTL_SECOND,
                IcebergConnector.resolveTableCacheTtlSecond(props(IcebergConnector.TABLE_CACHE_TTL_SECOND, "   ")));
    }

    @Test
    public void schemaTtlOverrideEmptyWhenUnset() {
        // No meta.cache.iceberg.table.ttl-second -> no override, so the engine-default schema-cache TTL applies
        // (mirrors PaimonConnector). MUTATION: returning a concrete value would wrongly override the engine
        // default for a plain (with-cache) catalog -> red.
        Assertions.assertEquals(OptionalLong.empty(),
                new IcebergConnector(Collections.emptyMap(), new RecordingConnectorContext())
                        .schemaCacheTtlSecondOverride());
    }

    @Test
    public void schemaTtlOverrideZeroDisablesSchemaCache() {
        // The no-cache catalog (meta.cache.iceberg.table.ttl-second=0) must drive schema.cache.ttl-second=0 so a
        // desc after external DDL reads FRESH schema (test_iceberg_table_cache line 251). MUTATION: not mapping
        // ttl-second here -> the no-cache catalog serves stale cached schema -> red.
        Assertions.assertEquals(OptionalLong.of(0L),
                new IcebergConnector(props(IcebergConnector.TABLE_CACHE_TTL_SECOND, "0"),
                        new RecordingConnectorContext()).schemaCacheTtlSecondOverride());
    }

    @Test
    public void schemaTtlOverridePositiveIsPassedThrough() {
        Assertions.assertEquals(OptionalLong.of(3600L),
                new IcebergConnector(props(IcebergConnector.TABLE_CACHE_TTL_SECOND, "3600"),
                        new RecordingConnectorContext()).schemaCacheTtlSecondOverride());
    }

    @Test
    public void schemaTtlOverrideIgnoresUnparseableValue() {
        // A malformed value must not break catalog schema caching; fall back to no override (engine default).
        Assertions.assertEquals(OptionalLong.empty(),
                new IcebergConnector(props(IcebergConnector.TABLE_CACHE_TTL_SECOND, "not-a-number"),
                        new RecordingConnectorContext()).schemaCacheTtlSecondOverride());
    }

    @Test
    public void invalidateHooksAreNoThrowOnFreshConnector() {
        // Smoke: the REFRESH TABLE / REFRESH CATALOG hooks must be safe to call (they only touch the
        // connector-internal latest-snapshot cache; the actual invalidate semantics are in
        // IcebergLatestSnapshotCacheTest). MUTATION: an NPE on an empty cache -> red.
        IcebergConnector connector =
                new IcebergConnector(Collections.emptyMap(), new RecordingConnectorContext());
        Assertions.assertDoesNotThrow(() -> connector.invalidateTable("db1", "t1"));
        Assertions.assertDoesNotThrow(() -> connector.invalidateDb("db1"));
        Assertions.assertDoesNotThrow(connector::invalidateAll);
    }

    @Test
    public void latestSnapshotCacheDisabledForSessionUser() {
        // The latest-snapshot cache is an AUTHORIZATION-sensitive projection (snapshotId/schemaId) that
        // beginQuerySnapshot reads WITHOUT a preceding per-user loadTable, so a shared hit would bypass the
        // per-user authorization. It is disabled (null) under iceberg.rest.session=user (kept otherwise, incl.
        // vended-credentials, since a snapshot id carries no token). MUTATION: dropping the session=user gate ->
        // non-null for session -> red.
        Assertions.assertNotNull(
                new IcebergConnector(Collections.emptyMap(), new RecordingConnectorContext())
                        .latestSnapshotCacheForTest(),
                "a plain catalog builds the latest-snapshot cache");
        Map<String, String> vended = new HashMap<>();
        vended.put(IcebergConnectorProperties.ICEBERG_CATALOG_TYPE, IcebergConnectorProperties.TYPE_REST);
        vended.put(IcebergConnectorProperties.REST_VENDED_CREDENTIALS_ENABLED, "true");
        Assertions.assertNotNull(
                new IcebergConnector(vended, new RecordingConnectorContext()).latestSnapshotCacheForTest(),
                "a vended-credentials catalog still builds the latest-snapshot cache (an id carries no token)");
        Map<String, String> session = new HashMap<>();
        session.put(IcebergConnectorProperties.ICEBERG_CATALOG_TYPE, IcebergConnectorProperties.TYPE_REST);
        session.put(IcebergConnectorProperties.REST_SESSION, IcebergConnectorProperties.SESSION_USER);
        Assertions.assertNull(
                new IcebergConnector(session, new RecordingConnectorContext()).latestSnapshotCacheForTest(),
                "a session=user catalog must NOT build the latest-snapshot cache (per-user authz bypass)");
    }

    @Test
    public void invalidateHooksAreNoThrowForSessionUserWithNulledCaches() {
        // Under session=user the latest-snapshot / partition / format caches are all null. The REFRESH hooks must
        // still be no-throw (the invalidate* methods null-guard each cache). MUTATION: an unguarded invalidate call
        // on a nulled cache -> NPE -> red.
        Map<String, String> session = new HashMap<>();
        session.put(IcebergConnectorProperties.ICEBERG_CATALOG_TYPE, IcebergConnectorProperties.TYPE_REST);
        session.put(IcebergConnectorProperties.REST_SESSION, IcebergConnectorProperties.SESSION_USER);
        IcebergConnector connector = new IcebergConnector(session, new RecordingConnectorContext());
        Assertions.assertNull(connector.latestSnapshotCacheForTest());
        Assertions.assertNull(connector.partitionCacheForTest());
        Assertions.assertNull(connector.formatCacheForTest());
        Assertions.assertDoesNotThrow(() -> connector.invalidateTable("db1", "t1"));
        Assertions.assertDoesNotThrow(() -> connector.invalidateDb("db1"));
        Assertions.assertDoesNotThrow(connector::invalidateAll);
    }

    @Test
    public void refreshCatalogInvalidateAllDropsManifestCache() {
        // H-5: REFRESH CATALOG -> Connector.invalidateAll() must drop the connector's OWN manifest cache too
        // (legacy catalog-wide group.invalidateAll parity), not just the latest-snapshot cache. REFRESH TABLE
        // (invalidateTable) intentionally keeps manifest entries, so this is the catalog-level-only behavior.
        IcebergConnector connector =
                new IcebergConnector(Collections.emptyMap(), new RecordingConnectorContext());
        Table table = tableWithOneManifest();
        ManifestFile manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
        IcebergManifestCache manifestCache = connector.manifestCacheForTest();
        manifestCache.getManifestCacheValue(manifest, table);
        Assertions.assertEquals(1, manifestCache.size(), "the manifest is cached after a load");

        // REFRESH TABLE must NOT drop the manifest cache (path-keyed immutable content; legacy parity).
        // MUTATION: invalidateTable clearing the manifest cache -> size 0 here -> red.
        connector.invalidateTable("db1", "t1");
        Assertions.assertEquals(1, manifestCache.size(), "REFRESH TABLE keeps manifest entries");

        // REFRESH CATALOG drops it. MUTATION: removing manifestCache.invalidateAll() from invalidateAll ->
        // size stays 1 -> red.
        connector.invalidateAll();
        Assertions.assertEquals(0, manifestCache.size(), "REFRESH CATALOG flushes the manifest cache");
    }

    private static Table tableWithOneManifest() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        Table table = catalog.createTable(TableIdentifier.of("db1", "t1"),
                new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())),
                PartitionSpec.unpartitioned());
        table.newAppend()
                .appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
                        .withPath("/data/f1.parquet").withFileSizeInBytes(100).withRecordCount(1).build())
                .commit();
        return table;
    }

    // ==================== PERF-01: cross-query table cache gate + invalidation ====================

    private static Table fakeTable(String name) {
        return new FakeIcebergTable(name,
                new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())),
                PartitionSpec.unpartitioned(), "s3://b/" + name, Collections.emptyMap());
    }

    @Test
    public void crossQueryTableCacheEnabledForPlainCatalog() {
        // A plain catalog (no per-user session, no REST vended credentials) has query-independent credentials,
        // so the cross-query RAW-table cache is built and enabled at the default 24h TTL — restoring the legacy
        // IcebergExternalMetaCache table cache. MUTATION: leaving it null/disabled for a plain catalog -> the
        // 3~7x remote loadTable amplification is not collapsed across queries -> assert below red.
        IcebergTableCache cache =
                new IcebergConnector(Collections.emptyMap(), new RecordingConnectorContext()).tableCacheForTest();
        Assertions.assertNotNull(cache, "a plain catalog must build the cross-query table cache");
        Assertions.assertTrue(cache.isEnabled(), "the default 24h TTL enables the cache");
    }

    @Test
    public void crossQueryTableCacheDisabledForVendedCredentials() {
        // REST vended-credentials: the cached raw table's FileIO carries a server-vended token that expires
        // within the query (iceberg keeps it fresh by reloading the table each query). A 24h-TTL cross-query hit
        // would hand BE an expired token (403 mid-scan), so this layer MUST be off (null); the query-scoped fat
        // handle still dedups within one query. MUTATION: building the cache for a vended catalog -> non-null -> red.
        Map<String, String> vended = new HashMap<>();
        vended.put(IcebergConnectorProperties.ICEBERG_CATALOG_TYPE, IcebergConnectorProperties.TYPE_REST);
        vended.put(IcebergConnectorProperties.REST_VENDED_CREDENTIALS_ENABLED, "true");
        Assertions.assertNull(
                new IcebergConnector(vended, new RecordingConnectorContext()).tableCacheForTest(),
                "a REST vended-credentials catalog must NOT build the cross-query table cache");
    }

    @Test
    public void crossQueryTableCacheDisabledForPerUserSession() {
        // iceberg.rest.session=user: the cached raw table carries per-user delegated FileIO, so sharing it
        // across users would leak credentials. This layer MUST be off (null) — the fat handle keeps within-query
        // dedup. MUTATION: building the cache for a session=user catalog -> tableCacheForTest non-null -> red.
        Map<String, String> session = new HashMap<>();
        session.put(IcebergConnectorProperties.ICEBERG_CATALOG_TYPE, IcebergConnectorProperties.TYPE_REST);
        session.put(IcebergConnectorProperties.REST_SESSION, IcebergConnectorProperties.SESSION_USER);
        Assertions.assertNull(
                new IcebergConnector(session, new RecordingConnectorContext()).tableCacheForTest(),
                "a per-user session catalog must NOT build the cross-query table cache");
    }

    @Test
    public void refreshHooksInvalidateCrossQueryTableCache() {
        // The REFRESH hooks must clear the cross-query table cache (else external DDL/writes would stay invisible
        // beyond the pin): REFRESH TABLE drops one table, REFRESH DATABASE drops that db's tables, REFRESH
        // CATALOG drops everything — mirroring the latest-snapshot cache. MUTATION: an invalidate* hook not
        // touching tableCache -> a stale entry survives -> a size assert below red.
        IcebergConnector connector =
                new IcebergConnector(Collections.emptyMap(), new RecordingConnectorContext());
        IcebergTableCache cache = connector.tableCacheForTest();
        Assertions.assertNotNull(cache);

        cache.getOrLoad(TableIdentifier.of("db1", "t1"), () -> fakeTable("db1.t1"));
        cache.getOrLoad(TableIdentifier.of("db1", "t2"), () -> fakeTable("db1.t2"));
        cache.getOrLoad(TableIdentifier.of("db2", "t1"), () -> fakeTable("db2.t1"));
        Assertions.assertEquals(3, cache.size());

        connector.invalidateTable("db1", "t1");
        Assertions.assertEquals(2, cache.size(), "REFRESH TABLE drops only that table");

        connector.invalidateDb("db1");
        Assertions.assertEquals(1, cache.size(), "REFRESH DATABASE drops that db's remaining tables");

        connector.invalidateAll();
        Assertions.assertEquals(0, cache.size(), "REFRESH CATALOG drops everything");
    }

    // ============ PERF-02: partition-view cache (session=user gated) + invalidation ============

    private static IcebergPartitionCache.Key partKey(String db, String tbl, long snapshotId) {
        return new IcebergPartitionCache.Key(TableIdentifier.of(db, tbl), snapshotId);
    }

    @Test
    public void partitionCacheBuiltUnlessSessionUser() {
        // The partition-view cache stores pure metadata (no FileIO/credential), so unlike the table cache it stays
        // built for a REST vended-credentials catalog (a partition list carries no token). But under
        // iceberg.rest.session=user it is an AUTHORIZATION-sensitive projection -- a shared (no user dimension) hit
        // would disclose one user's partitions to a "can-list-cannot-load" principal -- so it is disabled (null)
        // there, holding the "session=user => no live cross-query metadata cache" invariant.
        // MUTATION: dropping the session=user gate -> non-null for session -> red; gating on the vended flag ->
        // null for vended -> red.
        Assertions.assertNotNull(
                new IcebergConnector(Collections.emptyMap(), new RecordingConnectorContext()).partitionCacheForTest(),
                "a plain catalog builds the partition cache");
        Map<String, String> vended = new HashMap<>();
        vended.put(IcebergConnectorProperties.ICEBERG_CATALOG_TYPE, IcebergConnectorProperties.TYPE_REST);
        vended.put(IcebergConnectorProperties.REST_VENDED_CREDENTIALS_ENABLED, "true");
        Assertions.assertNotNull(
                new IcebergConnector(vended, new RecordingConnectorContext()).partitionCacheForTest(),
                "a vended-credentials catalog still builds the partition cache (metadata carries no credentials)");
        Map<String, String> session = new HashMap<>();
        session.put(IcebergConnectorProperties.ICEBERG_CATALOG_TYPE, IcebergConnectorProperties.TYPE_REST);
        session.put(IcebergConnectorProperties.REST_SESSION, IcebergConnectorProperties.SESSION_USER);
        Assertions.assertNull(
                new IcebergConnector(session, new RecordingConnectorContext()).partitionCacheForTest(),
                "a session=user catalog must NOT build the partition cache (per-user authz must not be bypassed)");
    }

    @Test
    public void refreshHooksInvalidatePartitionCache() {
        // The REFRESH hooks must clear the partition-view cache too (else external DDL/writes would stay invisible
        // beyond the pin): REFRESH TABLE drops that table's snapshot entries, REFRESH DATABASE that db's, REFRESH
        // CATALOG everything. MUTATION: an invalidate* hook not touching partitionCache -> a stale entry survives
        // -> a size assert below red.
        IcebergConnector connector =
                new IcebergConnector(Collections.emptyMap(), new RecordingConnectorContext());
        IcebergPartitionCache cache = connector.partitionCacheForTest();
        Assertions.assertNotNull(cache);
        cache.getOrLoad(partKey("db1", "t1", 1L), Collections::emptyList);
        cache.getOrLoad(partKey("db1", "t1", 2L), Collections::emptyList);
        cache.getOrLoad(partKey("db1", "t2", 1L), Collections::emptyList);
        cache.getOrLoad(partKey("db2", "t1", 1L), Collections::emptyList);
        Assertions.assertEquals(4, cache.size());

        connector.invalidateTable("db1", "t1");
        Assertions.assertEquals(2, cache.size(), "REFRESH TABLE drops both snapshot entries of db1.t1");

        connector.invalidateDb("db1");
        Assertions.assertEquals(1, cache.size(), "REFRESH DATABASE drops db1's remaining table");

        connector.invalidateAll();
        Assertions.assertEquals(0, cache.size(), "REFRESH CATALOG drops everything");
    }

    private static IcebergFormatCache.Key fmtKey(String db, String tbl, long snapshotId) {
        return new IcebergFormatCache.Key(TableIdentifier.of(db, tbl), snapshotId);
    }

    @Test
    public void formatCacheBuiltUnlessSessionUser() {
        // The inferred-format cache stores a pure metadata format-name string (no FileIO/credential), so like the
        // partition cache it stays built for a REST vended-credentials catalog. But under iceberg.rest.session=user
        // it is an AUTHORIZATION-sensitive projection, so it is disabled (null) there (same treatment as the
        // partition cache). MUTATION: dropping the session=user gate -> non-null for session -> red; gating on the
        // vended flag -> null for vended -> red.
        Assertions.assertNotNull(
                new IcebergConnector(Collections.emptyMap(), new RecordingConnectorContext()).formatCacheForTest(),
                "a plain catalog builds the format cache");
        Map<String, String> vended = new HashMap<>();
        vended.put(IcebergConnectorProperties.ICEBERG_CATALOG_TYPE, IcebergConnectorProperties.TYPE_REST);
        vended.put(IcebergConnectorProperties.REST_VENDED_CREDENTIALS_ENABLED, "true");
        Assertions.assertNotNull(
                new IcebergConnector(vended, new RecordingConnectorContext()).formatCacheForTest(),
                "a vended-credentials catalog still builds the format cache (a format name carries no credentials)");
        Map<String, String> session = new HashMap<>();
        session.put(IcebergConnectorProperties.ICEBERG_CATALOG_TYPE, IcebergConnectorProperties.TYPE_REST);
        session.put(IcebergConnectorProperties.REST_SESSION, IcebergConnectorProperties.SESSION_USER);
        Assertions.assertNull(
                new IcebergConnector(session, new RecordingConnectorContext()).formatCacheForTest(),
                "a session=user catalog must NOT build the format cache (per-user authz must not be bypassed)");
    }

    @Test
    public void refreshHooksInvalidateFormatCache() {
        // The REFRESH hooks must clear the inferred-format cache too (else a rewrite that changed the write format
        // would stay invisible beyond the pin): REFRESH TABLE drops that table's snapshot entries, REFRESH DATABASE
        // that db's, REFRESH CATALOG everything. MUTATION: an invalidate* hook not touching formatCache -> a stale
        // entry survives -> a size assert below red.
        IcebergConnector connector =
                new IcebergConnector(Collections.emptyMap(), new RecordingConnectorContext());
        IcebergFormatCache cache = connector.formatCacheForTest();
        Assertions.assertNotNull(cache);
        cache.getOrLoad(fmtKey("db1", "t1", 1L), () -> "parquet");
        cache.getOrLoad(fmtKey("db1", "t1", 2L), () -> "orc");
        cache.getOrLoad(fmtKey("db1", "t2", 1L), () -> "parquet");
        cache.getOrLoad(fmtKey("db2", "t1", 1L), () -> "parquet");
        Assertions.assertEquals(4, cache.size());

        connector.invalidateTable("db1", "t1");
        Assertions.assertEquals(2, cache.size(), "REFRESH TABLE drops both snapshot entries of db1.t1");

        connector.invalidateDb("db1");
        Assertions.assertEquals(1, cache.size(), "REFRESH DATABASE drops db1's remaining table");

        connector.invalidateAll();
        Assertions.assertEquals(0, cache.size(), "REFRESH CATALOG drops everything");
    }

    private static Map<String, String> restProps(boolean vended, boolean sessionUser) {
        Map<String, String> m = new HashMap<>();
        m.put(IcebergConnectorProperties.ICEBERG_CATALOG_TYPE, IcebergConnectorProperties.TYPE_REST);
        if (vended) {
            m.put(IcebergConnectorProperties.REST_VENDED_CREDENTIALS_ENABLED, "true");
        }
        if (sessionUser) {
            m.put(IcebergConnectorProperties.REST_SESSION, IcebergConnectorProperties.SESSION_USER);
        }
        return m;
    }

    private static IcebergCommentCache commentCacheOf(Map<String, String> props) {
        return new IcebergConnector(props, new RecordingConnectorContext()).commentCacheForTest();
    }

    @Test
    public void commentCacheBuiltOnlyForVendedNonSessionCatalog() {
        // PERF-05: the comment cache fills the gap PERF-01's tableCache leaves for vended-credentials catalogs, but
        // ONLY when NOT session=user -- a session=user comment cache would serve one user's comment to another
        // whose per-user loadTable authorization was never checked (a metadata disclosure). Plain catalogs already
        // reuse tableCache for the comment path, so no comment cache there either.
        // Plain catalog -> null (tableCache covers the comment path; no redundant cache).
        Assertions.assertNull(commentCacheOf(Collections.emptyMap()),
                "a plain catalog must NOT build the comment cache (tableCache already serves it)");
        // Vended, non-session -> built (the one flavor it is safe + useful for).
        Assertions.assertNotNull(commentCacheOf(restProps(true, false)),
                "a vended-credentials (non-session) catalog must build the comment cache");
        // session=user -> null (per-user authorization must not be bypassed by a shared cache).
        Assertions.assertNull(commentCacheOf(restProps(false, true)),
                "a session=user catalog must NOT build the comment cache (per-user authz)");
        // vended AND session=user -> null (session=user wins; !isUserSessionEnabled() gates it off).
        Assertions.assertNull(commentCacheOf(restProps(true, true)),
                "vended + session=user must NOT build the comment cache (session=user takes precedence)");
    }

    @Test
    public void refreshHooksInvalidateCommentCache() {
        // The REFRESH hooks must clear the comment cache too (else an external ALTER comment stays invisible beyond
        // the pin): REFRESH TABLE drops that table, REFRESH DATABASE that db, REFRESH CATALOG everything. MUTATION:
        // an invalidate* hook not touching commentCache -> a stale comment survives -> a size assert below red.
        IcebergConnector connector = new IcebergConnector(restProps(true, false), new RecordingConnectorContext());
        IcebergCommentCache cache = connector.commentCacheForTest();
        Assertions.assertNotNull(cache);
        cache.getOrLoad(TableIdentifier.of("db1", "t1"), () -> "c1");
        cache.getOrLoad(TableIdentifier.of("db1", "t2"), () -> "c2");
        cache.getOrLoad(TableIdentifier.of("db2", "t1"), () -> "c3");
        Assertions.assertEquals(3, cache.size());

        connector.invalidateTable("db1", "t1");
        Assertions.assertEquals(2, cache.size(), "REFRESH TABLE drops db1.t1");

        connector.invalidateDb("db1");
        Assertions.assertEquals(1, cache.size(), "REFRESH DATABASE drops db1's remaining table");

        connector.invalidateAll();
        Assertions.assertEquals(0, cache.size(), "REFRESH CATALOG drops everything");
    }

    // ============ PERF-06: derived partition-view cache A (session=user gated) + invalidation ============

    @Test
    public void partitionViewCacheBuiltUnlessSessionUser() {
        // Cache A (the derived partition-view cache) stores pure metadata (a partition view carries no
        // FileIO/credential), so like the partition cache it stays built for a REST vended-credentials catalog. But
        // under iceberg.rest.session=user it is an AUTHORIZATION-sensitive projection -- a shared (no user
        // dimension) hit would disclose one user's partition view -- so BOTH typed instances are disabled (null)
        // there. MUTATION: dropping the session=user gate -> non-null for session -> red; gating on the vended flag
        // -> null for vended -> red.
        IcebergConnector plain = new IcebergConnector(Collections.emptyMap(), new RecordingConnectorContext());
        Assertions.assertNotNull(plain.mvccPartitionViewCacheForTest(), "a plain catalog builds the MVCC view cache");
        Assertions.assertNotNull(plain.listPartitionsViewCacheForTest(),
                "a plain catalog builds the listPartitions view cache");
        Map<String, String> vended = new HashMap<>();
        vended.put(IcebergConnectorProperties.ICEBERG_CATALOG_TYPE, IcebergConnectorProperties.TYPE_REST);
        vended.put(IcebergConnectorProperties.REST_VENDED_CREDENTIALS_ENABLED, "true");
        IcebergConnector vendedConn = new IcebergConnector(vended, new RecordingConnectorContext());
        Assertions.assertNotNull(vendedConn.mvccPartitionViewCacheForTest(),
                "a vended-credentials catalog still builds the MVCC view cache (metadata carries no credentials)");
        Assertions.assertNotNull(vendedConn.listPartitionsViewCacheForTest(),
                "a vended-credentials catalog still builds the listPartitions view cache");
        Map<String, String> session = new HashMap<>();
        session.put(IcebergConnectorProperties.ICEBERG_CATALOG_TYPE, IcebergConnectorProperties.TYPE_REST);
        session.put(IcebergConnectorProperties.REST_SESSION, IcebergConnectorProperties.SESSION_USER);
        IcebergConnector sessionConn = new IcebergConnector(session, new RecordingConnectorContext());
        Assertions.assertNull(sessionConn.mvccPartitionViewCacheForTest(),
                "a session=user catalog must NOT build the MVCC view cache (per-user authz must not be bypassed)");
        Assertions.assertNull(sessionConn.listPartitionsViewCacheForTest(),
                "a session=user catalog must NOT build the listPartitions view cache");
    }

    @Test
    public void refreshHooksInvalidatePartitionViewCache() {
        // The REFRESH hooks must clear cache A too (else external DDL/writes stay invisible beyond the pin): REFRESH
        // TABLE drops that table's snapshot entries, REFRESH DATABASE that db's, REFRESH CATALOG everything. Asserted
        // via a counting loader (the framework's size() is package-private): after invalidation the loader must run
        // again. MUTATION: an invalidate* hook not routed to the view cache -> the entry survives -> loader not
        // re-run -> a loads assert below red.
        IcebergConnector connector =
                new IcebergConnector(Collections.emptyMap(), new RecordingConnectorContext());
        ConnectorPartitionViewCache<List<ConnectorPartitionInfo>> cache = connector.listPartitionsViewCacheForTest();
        Assertions.assertNotNull(cache);
        int[] loads = {0};
        Supplier<List<ConnectorPartitionInfo>> loader = () -> {
            loads[0]++;
            return Collections.emptyList();
        };
        PartitionViewCacheKey db1t1 = new PartitionViewCacheKey("db1", "t1", 1L, 1L);
        PartitionViewCacheKey db1t2 = new PartitionViewCacheKey("db1", "t2", 1L, 1L);
        PartitionViewCacheKey db2t1 = new PartitionViewCacheKey("db2", "t1", 1L, 1L);

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

    @Test
    public void invalidateHooksNoThrowForSessionUserPartitionViewCaches() {
        // Under session=user cache A's two instances are null; the invalidate hooks must null-guard them (no NPE).
        // MUTATION: an unguarded view-cache invalidate on a nulled field -> NPE -> red.
        Map<String, String> session = new HashMap<>();
        session.put(IcebergConnectorProperties.ICEBERG_CATALOG_TYPE, IcebergConnectorProperties.TYPE_REST);
        session.put(IcebergConnectorProperties.REST_SESSION, IcebergConnectorProperties.SESSION_USER);
        IcebergConnector connector = new IcebergConnector(session, new RecordingConnectorContext());
        Assertions.assertNull(connector.mvccPartitionViewCacheForTest());
        Assertions.assertNull(connector.listPartitionsViewCacheForTest());
        Assertions.assertDoesNotThrow(() -> connector.invalidateTable("db1", "t1"));
        Assertions.assertDoesNotThrow(() -> connector.invalidateDb("db1"));
        Assertions.assertDoesNotThrow(connector::invalidateAll);
    }
}
