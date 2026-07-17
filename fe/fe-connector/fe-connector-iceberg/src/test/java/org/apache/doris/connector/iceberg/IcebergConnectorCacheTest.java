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
import java.util.Map;
import java.util.OptionalLong;

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
}
