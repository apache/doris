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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.hms.CachingHmsClient;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsDatabaseInfo;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsTableInfo;
import org.apache.doris.filesystem.FileSystem;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests {@link HiveConnector#invalidateTable}/{@link HiveConnector#invalidateAll} — the REFRESH TABLE / REFRESH
 * CATALOG hooks that arm the connector-owned D2 caches.
 *
 * <p>WHY (Rule 9): a flipped hive catalog's caches never expire on external change (event sync is off until the
 * event Model B step), so {@code REFRESH TABLE} / {@code REFRESH CATALOG} are the user's explicit way to see new
 * data. fe-core already routes them to {@code connector.invalidateTable} / {@code invalidateAll}; these tests pin
 * that the hive connector then drops BOTH cache layers — the metastore-metadata cache ({@link CachingHmsClient})
 * AND the directory-listing cache ({@link HiveFileListingCache}) — because a hive table's schema, partitions and
 * files are all mutable (unlike iceberg's immutable manifests). {@code invalidateTable} is scoped to one table;
 * {@code invalidateAll} clears everything; and the public hooks never force-build a metastore client just to
 * flush (a REFRESH on a never-scanned catalog must be a cheap no-op on the metastore side).</p>
 *
 * <p>Live since the hms flip; this test drives the hooks directly.</p>
 */
public class HiveConnectorInvalidateTest {

    // The file-listing cache above the FileSystem seam: the injected FileSystem lists successfully (empty is fine —
    // a successful load still leaves a cache entry), so these size()-based invalidation assertions don't need real
    // files. Mirrors the role the old Configuration CONF constant played.
    private static final FileSystem FS = new FakeFileSystem();

    private static Map<String, String> props() {
        Map<String, String> m = new HashMap<>();
        m.put("hive.metastore.uris", "thrift://host:9083");
        return m;
    }

    @Test
    public void invalidateTableFlushesBothCachesForThatTableOnly() {
        HiveConnector connector = new HiveConnector(props(), new FakeConnectorContext());
        RecordingHmsClient raw = new RecordingHmsClient();
        CachingHmsClient cachingClient = (CachingHmsClient) connector.wrapWithCache(raw);

        // Metastore-metadata cache: populate t1 and t2 (each one RPC, then cached).
        cachingClient.getTable("db", "t1");
        cachingClient.getTable("db", "t2");
        Assertions.assertEquals(2, raw.getTableCalls);

        // Directory-listing cache: populate t1 and t2 (each one listing, then cached).
        HiveFileListingCache fileCache = connector.fileListingCacheForTest();
        fileCache.listDataFiles("db", "t1", "file:///wh/db/t1", FS);
        fileCache.listDataFiles("db", "t2", "file:///wh/db/t2", FS);
        Assertions.assertEquals(2, fileCache.size());

        connector.invalidateTable(cachingClient, "db", "t1");

        // Metastore cache: t1 re-fetches; t2 (a different table) is still served from the cache.
        cachingClient.getTable("db", "t1");
        Assertions.assertEquals(3, raw.getTableCalls, "REFRESH TABLE must drop t1's metastore entry");
        cachingClient.getTable("db", "t2");
        Assertions.assertEquals(3, raw.getTableCalls, "REFRESH TABLE must NOT drop another table's metastore entry");

        // File cache: t1's listing dropped, t2's survives (invalidateTable is scoped by (db, table)).
        Assertions.assertEquals(1, fileCache.size(), "REFRESH TABLE must drop only that table's directory listings");
    }

    @Test
    public void invalidateDbFlushesBothCachesForThatDbOnly() {
        HiveConnector connector = new HiveConnector(props(), new FakeConnectorContext());
        RecordingHmsClient raw = new RecordingHmsClient();
        CachingHmsClient cachingClient = (CachingHmsClient) connector.wrapWithCache(raw);

        // Metastore cache: TWO tables in db1 (t1, t2) and one in db2. REFRESH DATABASE db1 must drop every db1
        // table, not just one, while db2 survives.
        cachingClient.getTable("db1", "t1");
        cachingClient.getTable("db1", "t2");
        cachingClient.getTable("db2", "t1");
        Assertions.assertEquals(3, raw.getTableCalls);

        // Directory-listing cache: db1.t1 and db2.t1 (each one listing, then cached).
        HiveFileListingCache fileCache = connector.fileListingCacheForTest();
        fileCache.listDataFiles("db1", "t1", "file:///wh/db1/t1", FS);
        fileCache.listDataFiles("db2", "t1", "file:///wh/db2/t1", FS);
        Assertions.assertEquals(2, fileCache.size());

        connector.invalidateDb(cachingClient, "db1");

        // Metastore cache: every db1 table re-fetches (t1 AND t2); db2 (another database) is still cached.
        cachingClient.getTable("db1", "t1");
        cachingClient.getTable("db1", "t2");
        Assertions.assertEquals(5, raw.getTableCalls, "REFRESH DATABASE must drop every db1 table's metastore entry");
        cachingClient.getTable("db2", "t1");
        Assertions.assertEquals(5, raw.getTableCalls, "REFRESH DATABASE must NOT drop another database's entries");

        // File cache: db1's listing dropped, db2's survives (invalidateDb is scoped by db).
        Assertions.assertEquals(1, fileCache.size(), "REFRESH DATABASE must drop only that db's directory listings");
    }

    @Test
    public void invalidateAllFlushesBothCachesEntirely() {
        HiveConnector connector = new HiveConnector(props(), new FakeConnectorContext());
        RecordingHmsClient raw = new RecordingHmsClient();
        CachingHmsClient cachingClient = (CachingHmsClient) connector.wrapWithCache(raw);

        cachingClient.getTable("db", "t1");
        Assertions.assertEquals(1, raw.getTableCalls);
        HiveFileListingCache fileCache = connector.fileListingCacheForTest();
        fileCache.listDataFiles("db", "t1", "file:///wh/db/t1", FS);
        Assertions.assertEquals(1, fileCache.size());

        connector.invalidateAll(cachingClient);

        // Both caches fully cleared: the metastore entry re-fetches and the file cache is empty.
        cachingClient.getTable("db", "t1");
        Assertions.assertEquals(2, raw.getTableCalls, "REFRESH CATALOG must drop the metastore cache");
        Assertions.assertEquals(0, fileCache.size(), "REFRESH CATALOG must drop the directory-listing cache");
    }

    @Test
    public void publicHooksAreNoThrowAndClearFileCacheWithoutBuildingAClient() {
        // A fresh connector never built its metastore client (hmsClient == null). The public hooks must not
        // force-build one (a REFRESH on a never-scanned catalog is a cheap no-op on the metastore side), must not
        // throw on the null client, and must still clear the file cache.
        HiveConnector connector = new HiveConnector(props(), new FakeConnectorContext());
        HiveFileListingCache fileCache = connector.fileListingCacheForTest();

        fileCache.listDataFiles("db", "t", "file:///wh/db/t", FS);
        Assertions.assertEquals(1, fileCache.size());
        Assertions.assertDoesNotThrow(() -> connector.invalidateTable("db", "t"));
        Assertions.assertEquals(0, fileCache.size(), "REFRESH TABLE clears the file cache even with no client built");

        fileCache.listDataFiles("db", "t", "file:///wh/db/t", FS);
        Assertions.assertEquals(1, fileCache.size());
        Assertions.assertDoesNotThrow(() -> connector.invalidateDb("db"));
        Assertions.assertEquals(0, fileCache.size(), "REFRESH DATABASE clears the file cache with no client built");

        fileCache.listDataFiles("db", "t", "file:///wh/db/t", FS);
        Assertions.assertEquals(1, fileCache.size());
        Assertions.assertDoesNotThrow(() -> connector.invalidateAll());
        Assertions.assertEquals(0, fileCache.size(), "REFRESH CATALOG clears the file cache even with no client built");
    }

    @Test
    public void invalidatePartitionDropsOnlyThatPartitionsFileListing() {
        // WHY (Rule 9): a partition add/drop/alter must drop ONLY that partition's cached listing — legacy
        // HiveExternalMetaCache scoped its file-cache invalidation to (tableId + partitionValues), NOT the whole
        // table. The values are derived purely from the partition NAME (no metastore lookup), which is what stops
        // an evicted partition-metadata entry from leaving a stale listing (the #65334 failure mode).
        HiveConnector connector = new HiveConnector(props(), new FakeConnectorContext());
        CachingHmsClient cachingClient = (CachingHmsClient) connector.wrapWithCache(new RecordingHmsClient());
        HiveFileListingCache fileCache = connector.fileListingCacheForTest();

        // Two partitions of table t, plus a same-named partition of a DIFFERENT table t2 (must survive: scoped
        // by db+table, mirroring legacy's tableId in the predicate).
        fileCache.listDataFiles("db", "t", "file:///wh/db/t/dt=2024-01-01",
                Collections.singletonList("2024-01-01"), FS);
        fileCache.listDataFiles("db", "t", "file:///wh/db/t/dt=2024-01-02",
                Collections.singletonList("2024-01-02"), FS);
        fileCache.listDataFiles("db", "t2", "file:///wh/db/t2/dt=2024-01-01",
                Collections.singletonList("2024-01-01"), FS);
        Assertions.assertEquals(3, fileCache.size());

        connector.invalidatePartition(cachingClient, "db", "t", Collections.singletonList("dt=2024-01-01"));

        // Exactly one entry dropped (t's dt=2024-01-01); t's other partition and t2's same-named partition survive.
        Assertions.assertEquals(2, fileCache.size(),
                "invalidatePartition must drop ONLY the refreshed partition's listing, not the whole table");
        // Prove WHICH one was dropped: re-listing dt=2024-01-01 is a miss that re-adds it (size 3 again); had a
        // survivor been dropped instead, its earlier re-list would already have shown the miss.
        fileCache.listDataFiles("db", "t", "file:///wh/db/t/dt=2024-01-01",
                Collections.singletonList("2024-01-01"), FS);
        Assertions.assertEquals(3, fileCache.size(), "the dropped partition re-lists on the next scan");
    }

    @Test
    public void invalidatePartitionDropsOnlyThatPartitionsMetadata() {
        // The metastore-metadata half mirrors legacy's per-partition partitionEntry.invalidateKey: only the named
        // partition's cached HmsPartitionInfo is dropped; another partition of the same table stays cached.
        HiveConnector connector = new HiveConnector(props(), new FakeConnectorContext());
        RecordingHmsClient raw = new RecordingHmsClient();
        CachingHmsClient cachingClient = (CachingHmsClient) connector.wrapWithCache(raw);

        // Populate the partition-metadata cache for two partitions (misses fetched in ONE delegate round-trip).
        cachingClient.getPartitions("db", "t", Arrays.asList("dt=2024-01-01", "dt=2024-01-02"));
        Assertions.assertEquals(1, raw.getPartitionsCalls);

        connector.invalidatePartition(cachingClient, "db", "t", Collections.singletonList("dt=2024-01-01"));

        // dt=2024-01-01 re-fetches (a new delegate round-trip); dt=2024-01-02 is still served from the cache.
        cachingClient.getPartitions("db", "t", Collections.singletonList("dt=2024-01-01"));
        Assertions.assertEquals(2, raw.getPartitionsCalls, "the refreshed partition's metadata must be dropped");
        cachingClient.getPartitions("db", "t", Collections.singletonList("dt=2024-01-02"));
        Assertions.assertEquals(2, raw.getPartitionsCalls, "another partition's metadata must NOT be dropped");
    }

    /**
     * Minimal {@link HmsClient} that counts {@code getTable} calls and returns a fresh table info per call (so a
     * cache hit — same instance — is distinguishable from a reload). Only the abstract read methods are stubbed;
     * the write/txn methods are interface defaults.
     */
    private static final class RecordingHmsClient implements HmsClient {
        int getTableCalls;
        int getPartitionsCalls;

        @Override
        public HmsTableInfo getTable(String dbName, String tableName) {
            getTableCalls++;
            return HmsTableInfo.builder().dbName(dbName).tableName(tableName).build();
        }

        @Override
        public List<String> listDatabases() {
            return Collections.emptyList();
        }

        @Override
        public HmsDatabaseInfo getDatabase(String dbName) {
            return null;
        }

        @Override
        public List<String> listTables(String dbName) {
            return Collections.emptyList();
        }

        @Override
        public boolean tableExists(String dbName, String tableName) {
            return false;
        }

        @Override
        public Map<String, String> getDefaultColumnValues(String dbName, String tableName) {
            return Collections.emptyMap();
        }

        @Override
        public List<String> listPartitionNames(String dbName, String tableName, int maxParts) {
            return Collections.emptyList();
        }

        @Override
        public List<HmsPartitionInfo> getPartitions(String dbName, String tableName, List<String> partNames) {
            getPartitionsCalls++;
            List<HmsPartitionInfo> result = new ArrayList<>(partNames.size());
            for (String name : partNames) {
                result.add(new HmsPartitionInfo(HiveWriteUtils.toPartitionValues(name),
                        "file:///wh/" + dbName + "/" + tableName + "/" + name, null, null, null,
                        Collections.emptyMap()));
            }
            return result;
        }

        @Override
        public HmsPartitionInfo getPartition(String dbName, String tableName, List<String> values) {
            return null;
        }

        @Override
        public void close() {
        }
    }
}
