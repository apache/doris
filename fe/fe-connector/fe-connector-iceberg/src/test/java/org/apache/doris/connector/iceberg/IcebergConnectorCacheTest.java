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
    public void invalidateHooksAreNoThrowOnFreshConnector() {
        // Smoke: the REFRESH TABLE / REFRESH CATALOG hooks must be safe to call (they only touch the
        // connector-internal latest-snapshot cache; the actual invalidate semantics are in
        // IcebergLatestSnapshotCacheTest). MUTATION: an NPE on an empty cache -> red.
        IcebergConnector connector =
                new IcebergConnector(Collections.emptyMap(), new RecordingConnectorContext());
        Assertions.assertDoesNotThrow(() -> connector.invalidateTable("db1", "t1"));
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
}
